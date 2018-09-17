"""Converts pandas dataframe to various other data formats."""
import pyodbc
import yaml

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

def dataframe_to_mssql(pyodbc_connect_string, schemaname, tablename, columnlist, dataframe):
    """Takes a dataframe and inserts into a pre-defined MSSQL table."""
    # Count the number of columns to parameterize the sql command
    col_params = ""
    col_count = columnlist.count(",") + 1
    for x in range(col_count):
        if x == (col_count - 1):
            col_params = col_params + "?"
        else:
            col_params = col_params + "?, "
    print (col_params)
    datalist = dataframe.values.tolist()
    # load the table from datalist
    connection = pyodbc.connect(pyodbc_connect_string, autocommit=True)
    cursor = connection.cursor()
    cursor.fast_executemany = True
    cursor = connection.cursor()
    sqlcommand = ("INSERT INTO %s.%s (%s) VALUES (%s)") % (schemaname, tablename, columnlist, col_params) 
    print (sqlcommand)
    cursor.executemany(sqlcommand, datalist)


def truncate_sql_table(table_name):
    """Pull single git_tag and append to existing table."""
    conn = pyodbc.connect(DB_CONNECT_STRING)
    cursor = conn.cursor()
    sqltruncate = ("truncate table %s") % (table_name)
    cursor.execute(sqltruncate)
    conn.commit()



def test_dataframe_to_mssql():
    """Test function"""
    mylist = [[1,2],[3,4],[5,6]]
    mydf = pd.DataFrame(mylist)
    dataframe_to_mssql(DB_CONNECT_STRING, '[dbo]', '[test]', '[cola], [colb]', mydf)