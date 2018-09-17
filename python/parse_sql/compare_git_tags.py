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

"""
import urllib
import re
import pandas as pd
from sqlalchemy import create_engine
import yaml
import dataframe_conversions as dc

# Load the config yaml file
with open('config.yaml') as fp:
    my_configuration = yaml.load(fp)

# pyodbc connection PARAMS and ENGINE creation for later df to sql
DB_CONNECT_STRING = "DRIVER={%s};\
                     SERVER=%s;\
                     DATABASE=%s;\
                     UID=%s;\
                     PWD=%s" % (my_configuration['SQL_DRIVER'],
                     my_configuration['SQL_SERVER'],
                     my_configuration['SQL_DATABASE'],
                     my_configuration['SQL_LOGIN'],
                     my_configuration['SQL_PASSWORD'])
PARAMS = urllib.parse.quote_plus(DB_CONNECT_STRING)
ENGINE = create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
GIT_REPO = my_configuration['GIT_REPO_NAME']


def remove_empty_lists(the_list):
    """Remove empty lists removes any [] empty lists from a list of lists."""
    newlist = []
    # Loop over elements in list
    for i in the_list:
        # Is element a non-empty list? then call self on it.
        if isinstance(i, list) and i:
            newlist.append(remove_empty_lists(i))
        # If not a list.
        if not isinstance(i, list):
            newlist.append(i)
    return newlist


# Find all ddl statements in a sql string.
def process_ddl(file_content):
    """Search a file string and outputs all found DDL as list of lists."""
    # Procedures
    find = []
    find = find + re.findall(r"CREATE\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"CREATE\s+PROC\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+PROC\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+PROCEDURE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Views
    find = find + re.findall(r"CREATE\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+VIEW\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Tables
    find = find + re.findall(r"CREATE\s+TABLE\s+[a-zA-Z0-9_\[\].#]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TABLE\s+[a-zA-Z0-9_\[\].#]+",
                             file_content, re.I)
    regex = r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].#]+\s+ALTER\s+COLUMN.*"
    find = find + re.findall(regex, file_content, re.I)
    find = find + re.findall(r"ALTER\s+TABLE\s+[a-zA-Z0-9_\[\].#]+\s+ADD.*",
                             file_content, re.I)
    # Functions
    find = find + re.findall(r"CREATE\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+FUNCTION\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Types
    find = find + re.findall(r"CREATE\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+TYPE\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Triggers
    find = find + re.findall(r"CREATE\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"DROP\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+TRIGGER\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Indexes
    regex = r"CREATE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].#]+"
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+CLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].#]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+NONCLUSTERED\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].#]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    regex = r"CREATE\s+UNIQUE\s+INDEX\s+[a-zA-Z0-9_\[\].]+\s+ON\s+[a-zA-Z0-9_\[\].#]+"  # noqa
    find = find + re.findall(regex, file_content, re.I)
    find = find + re.findall(r"DROP\s+INDEX\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    find = find + re.findall(r"ALTER\s+INDEX\s+[a-zA-Z0-9_\[\].]+",
                             file_content, re.I)
    # Rename objects
    find = find + re.findall(r"sp_rename\s+[@A-Za-z0-9'._= ]+,\s+[@A-Za-z0-9'._= ]+",  # noqa
                             file_content, re.I)
    # Remove any temp tables from list
    find = [item for item in find if "#" not in item]
    find = remove_empty_lists(find)
    return find


def git_sql_to_dataframe(git_repo, git_tag):
    """Pull git tag from git_sql table into a dataframe."""
    queryx = "select full_path,\
              dir_path,\
              file_name,\
              file_content,\
              file_content_hash,\
              file_size,\
              git_repo,\
              git_tag\
              from parse_sql where git_tag = '{git_tag}' and git_repo = '{git_repo}'"
    queryx = queryx.format(git_tag=git_tag, git_repo=git_repo)
    dfx = pd.read_sql(queryx, ENGINE)
    # Delete the second duplicate file_content_hash values from each dataframe.
    dfx = dfx.drop_duplicates(subset='file_content_hash', keep='first')
    # Remove directory paths not like Rave_Viper_Lucy_Merged_DB_Scripts
    string_contains = 'Rave_Viper_Lucy_Merged_DB_Scripts'
    dfx = dfx[dfx['dir_path'].str.contains(string_contains)]
    # Remove certain directory paths the ~ is the opposite result set
    dfx = dfx[~dfx['dir_path'].str.contains('tSQLt_UnitTests')]
    dfx = dfx[~dfx['dir_path'].str.contains('Samples')]
    dfx = dfx[~dfx['dir_path'].str.contains('SolarWinds')]
    dfx = dfx[~dfx['dir_path'].str.contains('Registry1')]
    dfx = dfx[~dfx['dir_path'].str.contains('TSDV DB Install Scripts')]
    return dfx


def compare_two_tags(git_repo, git_tag1, git_tag2):
    """Compare all sql in two git tags to find new or changed files."""
    #global df1
    #global df2
    #global df2_unchanged
    #global df2_changed
    #global df2_new
    df1 = git_sql_to_dataframe(git_repo, git_tag1)
    df2 = git_sql_to_dataframe(git_repo, git_tag2)
    # Get 2 columns from each df.
    df1part = df1.loc[:, ['full_path', 'dir_path', 'file_name', 'file_content_hash']]
    df2part = df2.loc[:, ['full_path', 'dir_path', 'file_name', 'file_content_hash']]
    # Get list of unchanged files based on exact hash match.
    df2_unchanged = pd.merge(df2part,
                             df1part,
                             how='inner',
                             left_on=['file_content_hash'],
                             right_on=['file_content_hash'])
    df2_unchanged.columns = ['full_path','dir_path','file_name', 'file_content_hash', 'full_path_y', 'dir_path_y', 'file_name_y']
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
    df2_new.columns = ['full_path','dir_path','file_name', 'file_content_hash']
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
    df2_new.columns = ['full_path','dir_path','file_name', 'file_content_hash']
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
    df2_changed.columns = ['full_path','dir_path','file_name', 'file_content_hash']
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
    df2_changed.columns = ['full_path','dir_path','file_name', 'file_content_hash']
    # Join back to the original df2.
    df2_changed['change_type'] = "modified"
    df2_new['change_type'] = "new"
    # Combine dataframe of new and modified files
    df2_diff = df2_new.append(df2_changed, ignore_index=True)
    df2_diff_all = pd.merge(df2_diff, df2, how='inner')
    df2_diff_all['ddl'] = ""
    for index, row in df2_diff_all.iterrows():
        # Read file contents, set ddl column to list of all ddl statements.
        df2_diff_all.at[index, 'ddl'] = process_ddl(row['file_content'])
        print(row['full_path'])
        print(df2_diff_all.loc[index, 'ddl'])
    df2_diff_all = df2_diff_all.drop(columns=['file_content'])
    df2_diff_all = df2_diff_all.drop(columns=['file_content_hash'])    
    df2_diff_all = df2_diff_all.drop(columns=['file_size'])
    return df2_diff_all


if __name__ == "__main__":
    df_diff = compare_two_tags(GIT_REPO, "v2017.2.0", "v2018.1.3")
    datalist = []
    for index, row in df_diff.iterrows():
        for single_ddl in row['ddl']:
            datalist.append({'full_path': row['full_path'], 'dir_path': row['dir_path'], 'file_name': row['file_name'], 'change_type': row['change_type'], 'git_repo': row['git_repo'], 'git_tag': row['git_tag'], 'ddl': single_ddl})
    df_diff_split_ddl = pd.DataFrame(datalist)
    dc.truncate_sql_table("parse_sql_ddl")
    dc.dataframe_to_mssql(DB_CONNECT_STRING, '[dbo]', '[parse_sql_ddl]', '[change_type], [ddl], [dir_path], [file_name], [full_path], [git_repo], [git_tag]', df_diff_split_ddl)

