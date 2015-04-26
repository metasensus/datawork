#!/usr/bin/env python273

import cx_Oracle
import sys

# Function to open a connection to the oracle database
# parameter is connection string
def openconnect(connstr):
    #connstr='demo/demo@statsdb.3pardata.com:1521/statscentral.3pardata.com'
    conn = cx_Oracle.connect(connstr);
    return conn;

# Function to open a connection to the oracle database
#  parameter is connection object and sqlstmt
def execSql( conn, sqlstmt):
    curs = conn.cursor();
    #print sqlstmt;
    try:
        curs.execute(sqlstmt);
    except:
        print 'Error Sql:'+sqlstmt;
        print "Unexpected error:", sys.exc_info()[0];
        raise;
    return curs;
    

