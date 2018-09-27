import git
import sqlalchemy
import yaml
import db_ops
import parse_sql

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

# Grab the current git git_tag from repo
GIT_REPO_MAIN_DIRECTORY = MY_CONFIGURATION['GIT_REPO_MAIN_DIR']
GIT_REPO = MY_CONFIGURATION['GIT_REPO_NAME']
REPO = git.Repo(GIT_REPO_MAIN_DIRECTORY)
G = git.Git(GIT_REPO_MAIN_DIRECTORY)

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


def populate_parse_sql_all_tags():
    """Loop through all tags in the repo, check them out, and pull the SQL."""
    for git_tag in REPO.tags:
        populate_parse_sql_single_tag(git_tag)


def populate_parse_sql_single_tag(tagname):
    """Pull single git_tag and append to existing table."""
    G.clean('-xdf')
    G.checkout(tagname)
    df1 = parse_sql.read_sql_files_to_dataframe(SQL_DIRECTORY)
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
    populate_git_tag_dates()
    # db_ops.truncate_sql_table(DB_CONNECT_STRING, "parse_sql")
    # populate_parse_sql_all_tags()
    # populate_parse_sql_single_tag('v2017.2.0')
    # populate_parse_sql_single_tag('v2018.1.3')

