"""
This module is intended to compare sql files in two tags in a git repository.

TAG1 should be an earlier git_tag.
TAG2 should be a later git_tag.

The results will return two dataframes with a special DDL column that shows
all DDL changes in the file.

DF_FILES_NEW_ALL
DF_FILES_CHANGED_ALL

Examples:
    compare_two_tags("v2017.2.0", "v2018.1.3")

@author: jong
"""
import urllib
import re
import pandas as pd
import sqlalchemy
import yaml
import db_ops

def compare_all_tags(git_repo):
    """Read all tags, attempts ordering by date, runs compare_two_tags.

    Previous tag is determined by the order in the query.
    Excludes tags in git_tag_exclusions table.
    """
    queryx = ("SELECT gtd.git_repo \
              , gtd.git_tag \
              , gtd.git_tag_date \
              FROM git_tag_dates gtd \
              LEFT OUTER JOIN git_tag_exclusions gte \
              ON gtd.git_repo = gte.git_repo and gtd.git_tag = gte.git_tag \
              WHERE gte.git_tag is null \
              AND gtd.git_repo = '{git_repo}' \
              ORDER BY gtd.git_tag_date")
    queryx = queryx.format(git_repo=git_repo)
    df_all_tags = pd.read_sql(queryx, ENGINE)
    previous_tag = None
    for rowi in df_all_tags.itertuples():
        current_tag = rowi.git_tag
        print(previous_tag)
        print(current_tag)
        # print("Comparing tag %s to %s.") % (previous_tag, current_tag)
        compare_two_tags(git_repo, previous_tag, current_tag)
        previous_tag = current_tag
    return df_all_tags


def compare_two_tags(git_repo, git_tag1, git_tag2):
    """Compare all sql in two git tags to find new or changed files."""
    df1 = read_tag_to_dataframe(git_repo, git_tag1)
    df2 = read_tag_to_dataframe(git_repo, git_tag2)
    # Get 2 columns from each df.
    df1part = df1.loc[:, ['full_path',
                          'dir_path',
                          'file_name',
                          'file_content_hash']]
    df2part = df2.loc[:, ['full_path',
                          'dir_path',
                          'file_name',
                          'file_content_hash']]
    # Get list of unchanged files based on exact hash match.
    df2_unchanged = pd.merge(df2part,
                             df1part,
                             how='inner',
                             left_on=['file_content_hash'],
                             right_on=['file_content_hash'])
    df2_unchanged.columns = ['full_path',
                             'dir_path',
                             'file_name',
                             'file_content_hash',
                             'full_path_y',
                             'dir_path_y',
                             'file_name_y']
    df2_unchanged = df2_unchanged.drop(columns=['full_path_y'])
    df2_unchanged = df2_unchanged.drop(columns=['dir_path_y'])
    df2_unchanged = df2_unchanged.drop(columns=['file_name_y'])
    # Get list of new files only.
    # Left join on file_name.
    df2_new = pd.merge(df2part, df1part, how='left', on='file_name')
    # Only return rows where they did not exist in git_tag1.
    df2_new = df2_new.loc[df2_new.notna()['file_content_hash_y'] == 0]
    # Rename the columns after join and drop file_content_hash_y column.
    df2_new = df2_new.drop(columns=['full_path_y'])
    df2_new = df2_new.drop(columns=['dir_path_y'])
    df2_new = df2_new.drop(columns=['file_content_hash_y'])
    df2_new.columns = ['full_path',
                       'dir_path',
                       'file_name',
                       'file_content_hash']
    # Finally make sure does not exist in the files unchanged list
    df2_new = pd.merge(df2_new,
                       df2_unchanged,
                       how='left',
                       left_on=['file_content_hash'],
                       right_on=['file_content_hash'])
    df2_new = df2_new.loc[df2_new.notna()['file_name_y'] == 0]
    df2_new = df2_new.drop(columns=['file_name_y'])
    df2_new = df2_new.drop(columns=['full_path_y'])
    df2_new = df2_new.drop(columns=['dir_path_y'])
    df2_new.columns = ['full_path',
                       'dir_path',
                       'file_name',
                       'file_content_hash']
    # Get list of files changed only by removing unchanged and new files.
    # Exclude unchanged files first.
    df2_changed = pd.merge(df2part,
                           df2_unchanged,
                           how='left',
                           left_on=['file_content_hash'],
                           right_on=['file_content_hash'])
    df2_changed = df2_changed.loc[df2_changed.notna()['file_name_y'] == 0]
    df2_changed = df2_changed.drop(columns=['full_path_y'])
    df2_changed = df2_changed.drop(columns=['dir_path_y'])
    df2_changed = df2_changed.drop(columns=['file_name_y'])
    df2_changed.columns = ['full_path',
                           'dir_path',
                           'file_name',
                           'file_content_hash']
    # Exclude new files.
    df2_changed = pd.merge(df2_changed,
                           df2_new,
                           how='left',
                           left_on=['file_content_hash'],
                           right_on=['file_content_hash'])
    df2_changed = df2_changed.loc[df2_changed.notna()['file_name_y'] == 0]
    df2_changed = df2_changed.drop(columns=['full_path_y'])
    df2_changed = df2_changed.drop(columns=['dir_path_y'])
    df2_changed = df2_changed.drop(columns=['file_name_y'])
    df2_changed.columns = ['full_path',
                           'dir_path',
                           'file_name',
                           'file_content_hash']
    # Join back to the original df2.
    df2_changed['change_type'] = "modified"
    df2_new['change_type'] = "new"
    # Combine dataframe of new and modified files
    df2_diff = df2_new.append(df2_changed, ignore_index=True)
    df2_diff_all = pd.merge(df2_diff, df2, how='inner')
    df2_diff_all['ddl'] = ""
    for index2, row2 in df2_diff_all.iterrows():
        # Read file contents, set ddl column to list of all ddl statements.
        df2_diff_all.at[index2, 'ddl'] = find_ddls(row2['file_content'])
        print(row2['full_path'])
        print(df2_diff_all.loc[index2, 'ddl'])
    df2_diff_all = df2_diff_all.drop(columns=['file_content'])
    df2_diff_all = df2_diff_all.drop(columns=['file_content_hash'])
    df2_diff_all = df2_diff_all.drop(columns=['file_size'])
    # Normalize the ddl column from and save the results to parse_sql_ddl.
    save_diff_normalize_ddl(df2_diff_all)
    return df2_diff_all


if __name__ == "__main__":
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql_ddl")
    # DF_DIFF = compare_two_tags(GIT_REPO, "v2017.2.0", "v2018.1.3")
    DF_ALL_TAGS = compare_all_tags(GIT_REPO)
