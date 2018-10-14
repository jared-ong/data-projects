from dateutil import parser
import urllib
import re
import git
import sqlalchemy
import yaml
import os
import pandas as pd
import pytz
import db_ops
import parse_ddl


# Load the config yaml file
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.yaml')
with open(CONFIG_FILE) as yaml_file:
    CFG = yaml.safe_load(yaml_file)

# pyodbc connection string
DB_CONNECT_STRING = "DRIVER={%s};\
                     SERVER=%s;\
                     DATABASE=%s;\
                     UID=%s;\
                     PWD=%s" % (CFG['SQL_DRIVER'],
                                CFG['SQL_SERVER'],
                                CFG['SQL_DATABASE'],
                                CFG['SQL_LOGIN'],
                                CFG['SQL_PASSWORD'])
PARAMS = urllib.parse.quote_plus(DB_CONNECT_STRING)
ENGINE = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)

# Grab the current git git_tag from repo
GIT_REPO_MAIN_DIR = CFG['GIT_REPO_MAIN_DIR']
GIT_REPO = CFG['GIT_REPO_NAME']
REPO = git.Repo(GIT_REPO_MAIN_DIR)
G = git.Git(GIT_REPO_MAIN_DIR)

def populate_git_tag_dates():
    """Read from git repository all tags and last commit date.  Put in DB."""
    tagslist = []
    loginfo = G.log('--tags',
                    '--simplify-by-decoration',
                    '--pretty="format:%ci %d"')
    loginfo = re.findall(r'".+"', loginfo)
    for logentry in loginfo:
        if "tag:" in logentry:
            datepart = re.search(r"format:[0-9\-]+[ ][0-9\:]+[ ][0-9\-+]+",
                                 logentry)
            if datepart is None:
                datepart = None
            else:
                datepart = datepart.group(0)
            datepart = datepart.replace("format:", "")
            # Parse date time with UTC offset using dateutil.parser
            datepart = parser.parse(datepart)
            utc = pytz.UTC
            datepart = datepart.replace(tzinfo=utc) - datepart.utcoffset()
            tags = re.search(r'\(.+\)', logentry)
            if tags is None:
                tags = None
            else:
                tags = tags.group(0)
            tags = tags.replace("(", "")
            tags = tags.replace(")", "")
            tags = tags.split(',')
            for tag in tags:
                if "tag:" in tag:
                    tag = tag.replace("tag:", "")
                    tag = tag.strip()
                    tagslist.append({'git_repo': GIT_REPO,
                                     'git_tag': tag,
                                     'git_tag_date': datepart})
                    print(datepart)
                    print(tag)
    tagsdf = pd.DataFrame(tagslist)
    tagsdf.to_sql('git_tag_dates',
                  ENGINE,
                  if_exists='append',
                  index=False,
                  chunksize=1000,
                  dtype={'git_repo': sqlalchemy.types.NVARCHAR(length=255),
                         'git_tag': sqlalchemy.types.NVARCHAR(length=255),
                         'git_tag_date': sqlalchemy.types.DateTime()})
    return tagsdf


def git_relevant_tags():
    """Returns dataframe of git tags with exclusions."""
    the_sql = ("SELECT gtd.git_repo, gtd.git_tag, gtd.git_tag_date \
               FROM git_tag_dates gtd \
               LEFT OUTER JOIN git_tag_exclusions gte ON gtd.git_repo = gte.git_repo and gtd.git_tag = gte.git_tag \
               WHERE gte.git_tag IS NULL \
               AND gtd.git_repo = '%s' \
               ORDER BY gtd.git_tag_date") % (GIT_REPO)
    df1 = pd.read_sql(the_sql,
                      ENGINE)
    return df1


def git_parse_ddl_all_tags():
    """Loop through all tags in the repo, check them out, and pull the SQL."""
    df1 = git_relevant_tags()
    for index, row in df1.iterrows():
        git_parse_ddl_single_tag(row.git_tag, GIT_REPO_MAIN_DIR)


def git_parse_ddl_single_tag(tagname, directory):
    """Pull single git_tag.
    
    Pull sql files into data frame, parse the ddls, save to database."""

    G.clean('-xdf')
    G.checkout(tagname)

    df1 = parse_ddl.parse_ddl_to_dataframe(directory)
    df2 = parse_ddl.parse_ddl_normalize_ddl(df1)
    df1["git_repo"] = str(GIT_REPO)
    df1["git_tag"] = str(tagname)
    df2["git_repo"] = str(GIT_REPO)
    df2["git_tag"] = str(tagname)
    
    df1.to_sql('git_parse_ddl',
               ENGINE,
               if_exists='append',
               index=False,
               chunksize=1000,
               dtype={'full_path': sqlalchemy.types.NVARCHAR(),
                      'dir_path':  sqlalchemy.types.NVARCHAR(),
                      'file_name': sqlalchemy.types.NVARCHAR(length=255),
                      'file_content': sqlalchemy.types.NVARCHAR(),
                      'file_content_hash': sqlalchemy.types.NVARCHAR(length=255),  # noqa
                      'file_size': sqlalchemy.types.BigInteger(),
                      'ddl': sqlalchemy.types.NVARCHAR(),
                      'git_repo': sqlalchemy.types.NVARCHAR(length=255),
                      'git_tag': sqlalchemy.types.NVARCHAR(length=255)})
    df2.to_sql('git_parse_ddl_objects',
               ENGINE,
               if_exists='append',
               index=False,
               chunksize=1000,
               dtype={'full_path': sqlalchemy.types.NVARCHAR(),
                      'dir_path': sqlalchemy.types.NVARCHAR(),
                      'file_name': sqlalchemy.types.NVARCHAR(length=255),
                      'file_content': sqlalchemy.types.NVARCHAR(),
                      'file_content_hash': sqlalchemy.types.NVARCHAR(length=255),  # noqa
                      'file_size': sqlalchemy.types.BigInteger(),
                      'ddl': sqlalchemy.types.NVARCHAR(),
                      'object_action': sqlalchemy.types.NVARCHAR(length=50),
                      'object_name': sqlalchemy.types.NVARCHAR(length=255),
                      'object_schema': sqlalchemy.types.NVARCHAR(length=255),
                      'object_type': sqlalchemy.types.NVARCHAR(length=255),
                      'git_repo': sqlalchemy.types.NVARCHAR(length=255),
                      'git_tag': sqlalchemy.types.NVARCHAR(length=255)})    
    

def read_tag_to_dataframe(git_repo, git_tag):
    """Pull git tag from parse_ddl table into a dataframe."""
    queryx = "select full_path,\
              dir_path,\
              file_name,\
              file_content,\
              file_content_hash,\
              file_size,\
              git_repo,\
              git_tag\
              from git_parse_ddl where git_tag = '{git_tag}'\
              and git_repo = '{git_repo}'"
    queryx = queryx.format(git_tag=git_tag, git_repo=git_repo)
    dfx = pd.read_sql(queryx, ENGINE)
    # Delete the second duplicate file_content_hash values from each dataframe.
    dfx = dfx.drop_duplicates(subset='file_content_hash', keep='first')
    
    # Remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
    #string_contains = 'Rave_Viper_Lucy_Merged_DB_Scripts'
    #dfx = dfx[dfx['dir_path'].str.contains(string_contains)]
    
    # Remove certain directory paths the ~ is the opposite result set
    dfx = dfx[~dfx['dir_path'].str.contains('tSQLt_UnitTests')]
    dfx = dfx[~dfx['dir_path'].str.contains('Samples')]
    dfx = dfx[~dfx['dir_path'].str.contains('SolarWinds')]
    #dfx = dfx[~dfx['dir_path'].str.contains('Registry1')]
    #dfx = dfx[~dfx['dir_path'].str.contains('TSDV DB Install Scripts')]
    return dfx


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

    df2_diff_all = df2_diff_all.drop(columns=['file_content'])
    df2_diff_all = df2_diff_all.drop(columns=['file_content_hash'])
    df2_diff_all = df2_diff_all.drop(columns=['file_size'])
    df2_diff_all = df2_diff_all.drop(columns=['git_tag'])
    df2_diff_all['git_tag1'] = git_tag1
    df2_diff_all['git_tag2'] = git_tag2
    return df2_diff_all


def compare_two_tags_to_db(git_repo, git_tag1, git_tag2):
    """Compare two git tags sql files and save difference to database."""
    df_diff = compare_two_tags(git_repo, git_tag1, git_tag2)
    df_diff.to_sql('git_compare_two_tags',
                   ENGINE,
                   if_exists='append',
                   index=False,
                   chunksize=1000,
                   dtype={'full_path': sqlalchemy.types.NVARCHAR(),
                          'dir_path': sqlalchemy.types.NVARCHAR(),
                          'file_name': sqlalchemy.types.NVARCHAR(255),
                          'change_type': sqlalchemy.types.NVARCHAR(255),
                          'git_repo': sqlalchemy.types.NVARCHAR(255),
                          'git_tag1': sqlalchemy.types.NVARCHAR(255),
                          'git_tag2': sqlalchemy.types.NVARCHAR(255)})


def git_compare_all_tags():
    """Iterate over relevant git tags in order and compare before/after."""
    df_tags = git_relevant_tags()
    git_tag1 = None
    git_tag2 = None
    for index, row in df_tags.iterrows():
        git_tag2 = row.git_tag
        df_diff = compare_two_tags_to_db(GIT_REPO, git_tag1, git_tag2)
        git_tag1 = row.git_tag


if __name__ == "__main__":
    # Pull all tags and save to database
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "git_tag_dates")
    populate_git_tag_dates()

    # Loop through all tags in order and pull object name, schema, types
    # db_ops.truncate_sql_table(DB_CONNECT_STRING, "git_parse_ddl")
    # db_ops.truncate_sql_table(DB_CONNECT_STRING, "git_parse_ddl_objects")
    # git_parse_ddl_all_tags()

    # Loop through all tags in order and compare before and after tag
    # db_ops.truncate_sql_table(DB_CONNECT_STRING, "git_compare_two_tags")
    # git_compare_all_tags()
    