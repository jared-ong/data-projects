# -*- coding: utf-8 -*-
"""
Created on Mon Sep 24 20:24:35 2018

@author: jong
"""

import pyodbc
import pandas as pd
import yaml
import urllib
import sqlalchemy

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

# The dtype is optional and can be removed if column types are not known.
db_objects.to_sql("database_objects",
                  ENGINE,
                  if_exists= "replace",
                  index=True,
                  index_label="id",
                  chunksize=5000,
                  dtype={'object_name':  sqlalchemy.types.NVARCHAR(length=255),
                      'type_desc': sqlalchemy.types.NVARCHAR(length=255),
                      'parent_object_name': sqlalchemy.types.NVARCHAR(length=255),
                      'create_date': sqlalchemy.types.DATETIME(),
                      'modify_date': sqlalchemy.types.DATETIME(),
                      'url': sqlalchemy.types.NVARCHAR(length=500),
                      'urltype': sqlalchemy.types.NVARCHAR(length=50),
                      'sqlserver': sqlalchemy.types.NVARCHAR(length=255),
                      'dbname': sqlalchemy.types.NVARCHAR(length=255)})

# The dtype is optional and can be removed if column types are not known.
db_indexes.to_sql("database_indexes",
                  ENGINE,
                  if_exists= "replace",
                  index=True,
                  index_label="id",
                  chunksize=5000,
                  dtype={'index_name': sqlalchemy.types.NVARCHAR(length=255),
                         'table_name': sqlalchemy.types.NVARCHAR(length=255),
                         'index_id': sqlalchemy.types.INT(),
                         'type_desc': sqlalchemy.types.NVARCHAR(length=255),
                         'is_unique': sqlalchemy.types.SMALLINT(),
                         'is_primary_key': sqlalchemy.types.SMALLINT(),
                         'is_unique_constraint': sqlalchemy.types.SMALLINT(),
                         'url': sqlalchemy.types.NVARCHAR(length=500),
                         'urltype': sqlalchemy.types.NVARCHAR(length=50),
                         'sqlserver': sqlalchemy.types.NVARCHAR(length=255),
                         'dbname': sqlalchemy.types.NVARCHAR(length=255)})
