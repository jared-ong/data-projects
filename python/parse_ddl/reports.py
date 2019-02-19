import sqlalchemy
import yaml
import os
import urllib
import pandas as pd
import matplotlib.pyplot as plt


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

def query_object_repos_by_url(urls_list):
    """Returns dataframe of git tags with exclusions."""
    the_sql = ("SELECT u.url, COALESCE(ogrr.git_repo_main,'Unknown') as git_repo_main, count(do.object_name) as db_object_count "
               "FROM StatusRollupsHelper.dbo.urls u "
               "LEFT OUTER JOIN rave_db_objects.dbo.database_objects do on u.url = do.url "
               "LEFT OUTER JOIN parse_ddl.dbo.object_git_repo_refs ogrr ON do.object_name = ogrr.object_name "
               "WHERE u.url IN (%s) "
               "GROUP BY u.url, u.RaveVersionSortable, u.IsRaveX, ogrr.git_repo_main "
               "ORDER BY url desc") % (urls_list)
    df1 = pd.read_sql(the_sql,
                      ENGINE)
    return df1


def report_object_repos_by_url(my_df):
    ax = my_df.plot(kind='bar', title ="Rave URL Database Objects To Git Repo", figsize=(15, 10), legend=True, fontsize=12)
    ax.set_xlabel("Rave URL", fontsize=12)
    ax.set_ylabel("Count Of Database Objects", fontsize=12)
    plt.xticks(rotation=20)
    plt.show()


if __name__ == "__main__":
    DF_REPORT = query_object_repos_by_url("'jnj.mdsol.com','abbvie.mdsol.com','beigeneclinical.mdsol.com','nxt.mdsol.com','onyx.mdsol.com'")
    DF_PIVOT = DF_REPORT.pivot(index='url', columns='git_repo_main', values='db_object_count')
    report_object_repos_by_url(DF_PIVOT)