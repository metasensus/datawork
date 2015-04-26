#!/usr/bin/env python
import mysql.connector
#import MySQLdb
def connectMysql(dbname,username,passwd,host):
    cnx = mysql.connector.connect(user=username, password=passwd,
                                  host=host,database=dbname,buffered=True);
    return cnx;

def checkDb(dbname,cnx):
    mysqlcur=cnx.cursor();
    crtdbcur=cnx.cursor();
    crtdbcur.execute ('create database if not exists ods');
    return 0;

def checkUser(username,cnx):
    mysqlcur=cnx.cursor();

    mysqlcur.execute("select user from mysql.user where user='procuser'");
    usrList=mysqlcur.fetchall();
    if not usrList:
        print "Creating procuser....."
        mysqlcur.execute("create user procuser@localhost identified by 'c@llhome'");
        print "Opening DB as root....."
        newcnx = connectMysql('ods','root','','localhost');
        mysqlcurnew=newcnx.cursor();
        print "Granting permission to procuser....."
        mysqlcurnew.execute('grant all on * to procuser')
        newcnx.close();

#def connect(d,username,passwd,host):
    #cnx = mysql.connector.connect(user='',password='',host='',database)
   
def CheckCreateDb(dbname,cnx):
    mysqlcur=cnx.cursor();
    crtdbcur=cnx.cursor();
    
    #sqlstmt= "create database if not exists %s", (dbname)
    #print sqlstmt;
    sqlstmt='create database if not exists '+dbname;
    print sqlstmt;
    crtdbcur.execute (sqlstmt);



def CheckCreateUser(dbname,username,passwd,cnx):
    mysqlcur=cnx.cursor();
    #try:
    mysqlcur.execute('select user from mysql.user where user=\''+username+'\'');
    usrList=mysqlcur.fetchall();
    
    if not usrList:
            #print "deleting user....."
            #mysqlcur.execute('drop user \''+username+'\'@\'localhost\'');
            #print "flush....."
            #mysqlcur.execute('flush privileges');
            print "Creating user....."
            mysqlcur.execute('create user \''+username+'\'@\'localhost\' identified by \''+passwd+'\'');
            print "Opening DB as root....."
            
            newcnx = connectMysql(dbname,'root','','localhost');
            mysqlcurnew=newcnx.cursor();
            
            mysqlcurnew.execute('grant all on * to \''+username+'\'');
           
            
            print "Granting permission to user....."
           
            newcnx.close();
    #except MySQLdb.Error,e:
        #print repr(e)


        
