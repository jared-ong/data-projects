"""Custom database operations."""
import pyodbc


def truncate_sql_table(pyodbc_connect_string, table_name):
    """Pull single git_tag and append to existing table."""
    conn = pyodbc.connect(pyodbc_connect_string)
    cursor = conn.cursor()
    sqltruncate = ("truncate table %s") % (table_name)
    cursor.execute(sqltruncate)
    conn.commit()
