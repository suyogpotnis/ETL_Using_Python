"""
Date: 07/29/2018
Description: This script will allow you to move tables from source schema to target schema

"""

import petl as etl, sys
from sqlalchemy  import *
from importlib import reload
import pyodbc

reload(sys) # reloading the system

# Step[1]. Define Connections
sourceConn = pyodbc.connect('Trusted_Connection=yes',
                     driver = '{SQL Server}',
                     server = '(local)\SQLEXPRESS',
                     database = 'My_Source'
                            )

targetConn = pyodbc.connect('Trusted_Connection=yes',
                            driver = '{SQL Server}',
                            server = '(local)\SQLEXPRESS',
                            database = 'My_Target'
                            )
# =================================================================
# Step[2]. Define Cursor
sourceCursor = sourceConn.cursor()
targetCursor = targetConn.cursor()

# =================================================================
# Step[3]. Selecting all Tables you want to copy over to Target Schema
sourceCursor.execute(""" select table_name 
                         From INFORMATION_SCHEMA.COLUMNS 
                         Group BY table_name"""
                     )
# =================================================================
# Step[4]. Fetching All data from Source Schema
sourceTables = sourceCursor.fetchall()

# =================================================================
# Step[5]. Move data from Source Schema to Target Schema
for t in sourceTables:
    targetCursor.execute("drop table if exists %s" %(t[0]))
    sourceDs = etl.fromdb(sourceConn, "select * from %s" %(t[0]))
    etl.todb(sourceDs, targetConn, t[0], create=True, sample=1000)
