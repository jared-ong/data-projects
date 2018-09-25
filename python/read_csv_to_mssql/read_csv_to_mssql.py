# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:24:35 2018

@author: jong
"""

import pyodbc
import pandas as pd
import yaml
import urllib

# Load the config yaml file
with open('config.yaml') as fp:
    MY_CONFIGURATION = yaml.load(fp)

# pyodbc connection string  
DB_CONNECT_STRING = "DRIVER={%s};\
                     SERVER={%s};\
                     DATABASE={%s};\
                     UID={%s};\
                     PWD={%s}" % (MY_CONFIGURATION['SQL_DRIVER'],
                                  MY_CONFIGURATION['SQL_SERVER'],
                                  MY_CONFIGURATION['SQL_DATABASE'],
                                  MY_CONFIGURATION['SQL_LOGIN'],
                                  MY_CONFIGURATION['SQL_PASSWORD'])
PARAMS = urllib.parse.quote_plus(DB_CONNECT_STRING)
ENGINE = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % PARAMS)                     

db_objects = pd.read_csv("..\\..\\..\\AllDatabaseObjects20180924.csv",
                         low_memory=False)
db_indexes = pd.read_csv("..\\..\\..\\AllDatabaseIndexes20180924.csv",
                         low_memory=False)

db_objects.to_sql("database_objects",
                  ENGINE,
                  if_exists= "replace",
                  index=True,
                  index_label="id",
                  chunksize=5000)

db_indexes.to_sql("database_indexes",
                  ENGINE,
                  if_exists= "replace",
                  index=True,
                  index_label="id",
                  chunksize=5000)
s