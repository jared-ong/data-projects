r"""Main file that reads from a Github directory and reads all .sql files.

Custom module read_sql_files need to add to PYTHONPATH in
Spyder IDE > Tools > "PYTHONPATH Manager"
to GitHub\\data-projects\\python\\lib\\site-packages

Example to truncate the table before reload:
    truncate_sql_table()

Example to reload data into table:
    populate_parse_sql_all_tags()

Example to reload single tags:
    populate_parse_sql_single_tag('v2017.2.0')
    populate_parse_sql_single_tag('v2018.1.3')
"""

import urllib
from pathlib import Path
import os
import hashlib
import re
from dateutil.parser import parse
import pandas as pd
import git
import sqlalchemy
import yaml
import db_ops

# Load the config yaml file
with open('config.yaml') as fp:
    MY_CONFIGURATION = yaml.load(fp)

# pyodbc connection string
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

# Directory where the .sql files are
SQL_DIRECTORY = MY_CONFIGURATION['SQL_DIR']
# Grab the current git git_tag from repo
GIT_REPO_MAIN_DIRECTORY = MY_CONFIGURATION['GIT_REPO_MAIN_DIR']
GIT_REPO = MY_CONFIGURATION['GIT_REPO_NAME']
REPO = git.Repo(GIT_REPO_MAIN_DIRECTORY)
G = git.Git(GIT_REPO_MAIN_DIRECTORY)


def populate_get_tag_dates():
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
            datepart = parse(datepart)
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


def get_file_content(full_path):
    """Get file content function from read_sql_files_to_db.py."""
    # print(full_path)
    some_bytes = min(32, os.path.getsize(full_path))
    raw = open(full_path, 'rb').read(some_bytes)
    if '\\xff\\xfe' in str(raw):
        # print("utf-16")
        the_file = open(full_path, encoding="utf-16",
                        errors="backslashreplace")
        data = the_file.read()
    else:
        # print("latin-1")
        the_file = open(full_path, encoding="latin-1",
                        errors="backslashreplace")
        data = the_file.read()
    return data


def read_sql_files_to_dataframe(directory_path):
    """Recursive function to read .sql files into a dataframe."""
    file_ends_with = '.sql'
    glob_pattern = '**/*' + file_ends_with
    # Define the list of lists to use to create the dataframe
    datalist = []
    # First list is the column headers for later use in the dataframe
    headers = ('full_path',
               'dir_path',
               'file_name',
               'file_content',
               'file_content_hash',
               'file_size')
    # Recursive listing of all files matching glob_pattern
    pathlist = Path(directory_path).glob(glob_pattern)
    for path in pathlist:
        # Because path is object not string
        path = str(path)
        split_path = os.path.split(os.path.abspath(path))
        dir_path = split_path[0]
        file_name = split_path[1]
        file_content = get_file_content(path)
        # Replace sql comments with nothing
        file_content = re.sub(r"(--.*)|(((/\*)+?[\w\W]+?(\*/)+))",
                              "",
                              file_content)
        the_hash = hashlib.md5(file_content.encode('utf-8'))
        file_content_hash = the_hash.hexdigest()
        file_size = os.path.getsize(path)
        # Append tuple
        datalist.append((path,
                         dir_path,
                         file_name,
                         file_content,
                         file_content_hash,
                         file_size))
        print(path)
    df1 = pd.DataFrame(datalist, columns=headers)
    return df1


def populate_parse_sql_all_tags():
    """Loop through all tags in the repo, check them out, and pull the SQL."""
    for git_tag in REPO.tags:
        populate_parse_sql_single_tag(git_tag)


def populate_parse_sql_single_tag(tagname):
    """Pull single git_tag and append to existing table."""
    G.clean('-xdf')
    G.checkout(tagname)
    df1 = read_sql_files_to_dataframe(SQL_DIRECTORY)
    df1["git_repo"] = str(GIT_REPO)
    df1["git_tag"] = str(tagname)
    df1.to_sql('parse_sql',
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
                      'git_repo': sqlalchemy.types.NVARCHAR(length=255),
                      'git_tag': sqlalchemy.types.NVARCHAR(length=255)})


if __name__ == "__main__":
    db_ops.truncate_sql_table(DB_CONNECT_STRING, "git_tag_dates")
    populate_get_tag_dates()
    # db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql")
    # populate_parse_sql_all_tags()
    # populate_parse_sql_single_tag('v2017.2.0')
    # populate_parse_sql_single_tag('v2018.1.3')
