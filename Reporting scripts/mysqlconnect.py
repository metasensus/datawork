#!/usr/bin/env python
import mysql.connector

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
    
    usrList=mysqlcur.execute("select user from mysql.user where user='procuser'");
    if not usrList:
        print "Creating procuser....."
        mysqlcur.execute("create user procuser@localhost identified by 'c@llhome'");
        print "Opening DB as root....."
        newcnx = connectMysql('ods','root','','localhost');
        mysqlcurnew=newcnx.cursor();
        print "Granting permission to procuser....."
        mysqlcurnew.execute('grant all on * to procuser')
        newcnx.close();    
        
