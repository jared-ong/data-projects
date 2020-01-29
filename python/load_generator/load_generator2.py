import pyodbc
from multiprocessing import Process
import random
import time
import datetime

def update_datapoints_audits(how_many, sleep_between_updates):
    """Updates datapoint table only Updated column to produce some data changes"""
    x = 1
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=ravexdemo6;'
                          'Trusted_Connection=yes;queryTimeout=60', autocommit=True)
    cursor = cnxn.cursor()
    row = cursor.execute("select max(DataPointID) as MaxDataPointID from DataPoints").fetchone()
    max_datapoint = row.MaxDataPointID
    row = cursor.execute("select max(AuditID) as MaxAuditID from Audits").fetchone()
    max_audit = row.MaxAuditID
    while (x <= how_many):
        time.sleep(sleep_between_updates)
        random_datapoint = random.randint(1,max_datapoint)
        random_audit = random.randint(1,max_audit)
        update_sql = "update Datapoints set Updated = getutcdate() where DatapointID = " + str(random_datapoint)
        cursor.execute(update_sql)
        # print(update_sql)
        update_sql = "update Audits set DatabaseTime = getutcdate() where AuditID = " + str(random_audit)
        cursor.execute(update_sql)
        # print(update_sql)
        x = x + 1
    cursor.close
    cnxn.close

def update_datapoints_audits2(how_many, sleep_between_updates,_commit_size):
    """Updates datapoint table only Updated column to produce some data changes"""
    x = 1
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=ravexdemo6;'
                          'Trusted_Connection=yes;queryTimeout=60')
    cursor = cnxn.cursor()
    row = cursor.execute("select max(DataPointID) as MaxDataPointID from DataPoints").fetchone()
    max_datapoint = row.MaxDataPointID
    row = cursor.execute("select max(AuditID) as MaxAuditID from Audits").fetchone()
    max_audit = row.MaxAuditID
    while (x <= how_many):
        time.sleep(sleep_between_updates)
        random_datapoint = random.randint(1,max_datapoint)
        random_audit = random.randint(1,max_audit)
        update_sql = "update Datapoints set Updated = getutcdate() where DatapointID = " + str(random_datapoint)
        cursor.execute(update_sql)
        # print(update_sql)
        update_sql = "update Audits set DatabaseTime = getutcdate() where AuditID = " + str(random_audit)
        cursor.execute(update_sql)
        # print(update_sql)
        if ((x == how_many) or ((x % commit_size) == 0)):
            var_print = "Committing " + str(commit_size) + " rows."
            print(var_print)
            cursor.commit()
        x = x + 1
    cursor.close
    cnxn.close

def update_datapoints(how_many, sleep_between_updates,_commit_size):
    """Updates datapoint table only Updated column to produce some data changes"""
    x = 1
    cnxn = pyodbc.connect('Driver={SQL Server};'
                          'Server=localhost;'
                          'Database=ravexdemo6;'
                          'Trusted_Connection=yes;queryTimeout=60')
    cursor = cnxn.cursor()
    row = cursor.execute("select max(DataPointID) as MaxDataPointID from DataPoints").fetchone()
    max_datapoint = row.MaxDataPointID
    row = cursor.execute("select max(AuditID) as MaxAuditID from Audits").fetchone()
    max_audit = row.MaxAuditID
    while (x <= how_many):
        time.sleep(sleep_between_updates)
        random_datapoint = random.randint(1,max_datapoint)
        random_audit = random.randint(1,max_audit)
        update_sql = "update Datapoints set Updated = getutcdate() where DatapointID = " + str(random_datapoint)
        cursor.execute(update_sql)
        # print(update_sql)
        if ((x == how_many) or ((x % commit_size) == 0)):
            var_print = "Committing " + str(commit_size) + " rows."
            print(var_print)
            cursor.commit()
        x = x + 1
    cursor.close
    cnxn.close


if __name__ == '__main__':
    print("Starting Updates.")
    print(datetime.datetime.now())
    how_many_updates = 1000000
    # Sleep for 0 seconds
    sleep_between_updates = 0.000
    # Batch size
    commit_size = 100000
    # Run 5 threads of updates against the DB
    p1 = Process(target=update_datapoints(how_many_updates, sleep_between_updates, commit_size))
    p1.start()
    p2 = Process(target=update_datapoints(how_many_updates, sleep_between_updates, commit_size))
    p2.start()
    p3 = Process(target=update_datapoints(how_many_updates, sleep_between_updates, commit_size))
    p3.start()
    p4 = Process(target=update_datapoints(how_many_updates, sleep_between_updates, commit_size))
    p4.start()
    p5 = Process(target=update_datapoints(how_many_updates, sleep_between_updates, commit_size))
    p5.start()
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    p5.join()
    print("Completed Updates.")
    print(datetime.datetime.now())