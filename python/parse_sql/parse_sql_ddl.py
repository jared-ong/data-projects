"""
This module is intended to parse the ddl from a set of sql files.

"""
import urllib
import re
import pandas as pd
import sqlalchemy
import yaml
import db_ops

# Load the config yaml file
with open('config.yaml') as fp:
    MY_CONFIGURATION = yaml.load(fp)

# pyodbc connection PARAMS and ENGINE creation for later df to sql
DB_CONNECT_STRING = "DRIVER={%s};\
                     SERVER=%s;\
                     DATABASE=%s;\
                     UID=%s;\
                     PWD=%s" % (MY_CONFIGURATION['SQL_DRIVER'],
                                MY_CONFIGURATION['SQL_SERVER'],
                                MY_CONFIGURATION['SQL_DATABASE'],
                                MY_CONFIGURATION['SQL_LOGIN'],
                                MY_CONFIGURATION['SQL_PASSWORD'])
PARAMS = urllib.parse.quote_plus(DB_CONNECT_STRING)
ENGINE = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)
GIT_REPO = MY_CONFIGURATION['GIT_REPO_NAME']








def read_tag_to_dataframe(git_repo, git_tag):
    """Pull git tag from parse_sql table into a dataframe."""
    queryx = "select full_path,\
              dir_path,\
              file_name,\
              file_content,\
              file_content_hash,\
              file_size,\
              git_repo,\
              git_tag\
              from parse_sql where git_tag = '{git_tag}'\
              and git_repo = '{git_repo}'"
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


def save_diff_normalize_ddl(df_diff):
    """Take the difference dataframe after comparing two git tags and the ddl.

    Uses this dataframe to split the ddl list into separate rows.
    Inserts into the parse_sql_ddl table.
    """
    datalist = []
    for row in df_diff.itertuples():
        for single_ddl in row.ddl:
            object_info = ddl_object_action_name_type(single_ddl)
            datalist.append({'change_type': row.change_type,
                             'ddl': single_ddl,
                             'dir_path': row.dir_path,
                             'file_name': row.file_name,
                             'full_path': row.full_path,
                             'git_repo': row.git_repo,
                             'git_tag': row.git_tag,
                             'object_action': object_info[0],
                             'object_name': object_info[1],
                             'object_schema': object_info[2],
                             'object_type': object_info[3]})
    df_ddl = pd.DataFrame(datalist)
    df_ddl.to_sql('parse_sql_ddl',
                  ENGINE,
                  if_exists='append',
                  index=False,
                  chunksize=1000,
                  dtype={'change_type': sqlalchemy.types.NVARCHAR(length=50),
                         'ddl':  sqlalchemy.types.NVARCHAR(),
                         'dir_path': sqlalchemy.types.NVARCHAR(),
                         'file_name': sqlalchemy.types.NVARCHAR(length=255),
                         'full_path': sqlalchemy.types.NVARCHAR(),
                         'git_repo': sqlalchemy.types.NVARCHAR(length=255),
                         'git_tag': sqlalchemy.types.NVARCHAR(length=255),
                         'object_action': sqlalchemy.types.NVARCHAR(length=50),
                         'object_name': sqlalchemy.types.NVARCHAR(length=255),
                         'object_schema': sqlalchemy.types.NVARCHAR(length=255),
                         'object_type': sqlalchemy.types.NVARCHAR(length=255)})


def ddl_object_action_name_type(ddl_string):
    """Return object_action, object name, object type as a tuple."""
    ddl_string = re.sub(r'\[dbo\]\.', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'dbo\.', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'\bNONCLUSTERED\b', '',
                        ddl_string,
                        flags=re.I)
    ddl_string = re.sub(r'\bCLUSTERED\b', '', ddl_string, flags=re.I)
    ddl_string = re.sub(r'\bUNIQUE\b', '', ddl_string, flags=re.I)
    select_into = False  # used to determine if this is a select into statement
    # First, identify the object ddl action.
    object_action = re.search(r"(\bcreate\b|\balter\b|\bdrop\b|\bsp_rename\b)",
                              ddl_string,
                              re.I)
    # If no create, alter, drop, or sp_rename found in DDL statement
    if object_action is None:  # select into case
        object_action = re.search(r"INTO\s+[a-zA-Z0-9_\[\].#]+\s+FROM",
                              ddl_string,
                              re.I)
        if object_action is not None:
            select_into = True
            object_action = "CREATE"
    else:  # create, alter, drop, sp_rename case
        object_action = object_action.group(0).upper()
        ddl_string = re.sub(r'\b%s\b' % object_action,
                            '',
                            ddl_string,
                            flags=re.I)
    if object_action is not None:
        object_action = object_action.upper()
    # Next, identify the object type.
    # sp_rename could be any object type so we can't be sure.
    if object_action == 'SP_RENAME':
        object_type = None
    elif select_into:
        object_type = "TABLE"
    else:
        object_type = re.search(r"[A-Za-z0-9]+", ddl_string, re.I).group(0)
        ddl_string = re.sub(r'\b%s\b' % object_type,
                            '',
                            ddl_string,
                            flags=re.I)
        object_type = re.sub(r'\bproc\b',
                             'PROCEDURE',
                             object_type,
                             flags=re.I)
    if object_type is not None:
            object_type = object_type.upper()
    # Lastly, identify the object name if possible.
    # SP_RENAME condition.
    if object_action == "SP_RENAME":
        print("In SP_RENAME loop")
        object_name = re.search(r"@newname[\s]*=[\s]*[N]*'[a-zA-Z0-9_\.']+",
                                ddl_string, re.I)
        if object_name is None:
            object_name = re.search(r",\s*N*'*[a-zA-Z0-9_]+",
                                    ddl_string,
                                    re.I)
    # CREATE, ALTER, DROP condition.
    else:
        # Replace word "into" if this is a select into x from ddl create table.
        ddl_string = re.sub(r'\binto\b',
                             '',
                             ddl_string,
                             flags=re.I)
        object_name = re.search(r"[\S]+", ddl_string, re.I)
    if object_name is not None:
        object_name = object_name.group(0)
        # Replace =N'
        object_name = re.sub("[=\s]+N'", "", object_name, flags=re.I)
        # Replace ='
        object_name = re.sub("[=\s]+'", "", object_name, flags=re.I)
        # Replace @newname
        object_name = re.sub(r'@newname[\s]*', '',
                             object_name, flags=re.I)
        # Replace other single characters ,' [ ]
        object_name = re.sub(r",", "", object_name, flags=re.I)
        object_name = re.sub(r"'", "", object_name, flags=re.I)
        object_name = re.sub(r"\[", "", object_name, flags=re.I)
        object_name = re.sub(r"\]", "", object_name, flags=re.I)
        object_name = object_name.strip()

    obj_info = (object_action,
                object_name,
                object_type)
    return obj_info


if __name__ == "__main__":
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql_ddl")
    # DF_DIFF = compare_two_tags(GIT_REPO, "v2017.2.0", "v2018.1.3")
    DF_ALL_TAGS = compare_all_tags(GIT_REPO)
