import pyodbc
import os
from multiprocessing import Process

def get_file_content(full_path):
    """Get file content function from read_sql_files_to_db.py"""
    print(full_path)
    bytes = min(32, os.path.getsize(full_path))
    raw = open(full_path, 'rb').read(bytes)
    if '\\xff\\xfe' in str(raw):
        print("file is utf-16")
        the_file = open(full_path, encoding="utf-16",
                        errors="backslashreplace")
        data = the_file.read()
    else:
        print("file is latin-1")
        the_file = open(full_path, encoding="latin-1",
                        errors="backslashreplace")
        data = the_file.read()
    return data

def update_database_data():
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=ravexdemo6;'
                          'Trusted_Connection=yes;queryTimeout=60', autocommit=True)
    thesql = get_file_content("C:\\Users\\jong\\Documents\\GitHub\\data-projects\\python\\load_generator\\generate_load.sql")
    cursor = cnxn.cursor()
    cursor.execute(thesql)
    cursor.close
    cnxn.close

if __name__ == '__main__':
    update_database_data()
    # p1 = Process(target=update_database_data)
    # p1.start()
    # p2 = Process(target=update_database_data)
    # p2.start()
    # p3 = Process(target=update_database_data)
    # p3.start()
    # p4 = Process(target=update_database_data)
    # p4.start()
    # p5 = Process(target=update_database_data)
    # p5.start()
    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()
    # p5.join()