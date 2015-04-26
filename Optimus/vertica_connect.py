#!/usr/bin/env python
from vertica_python import connect
import sys;

def vertica_connect(hostname,port,username,password,dbname):
    try:
        connection = connect({
            'host': hostname,
            'port': port,
            'user': username,
            'password': password,
            'database': dbname
    
            });
        return connection;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def vertica_sql_execute(connection,sql):
    try:
        cur = connection.cursor();
        cur.execute(sql);
        return cur;
    except:
        print(sql+"\n Error reported: "+str(sys.exc_info()[1]));

