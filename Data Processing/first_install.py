#!/usr/bin/env python
import os;
import shutil as sh;
import string;
import sys;


import mysqlconnect as mysqlcon;

def checkmakedir(dname):
    try:
        print(dname);
        d=os.path.dirname(dname);
        if not os.path.exists(d):
            print('making '+dname);
            os.makedirs(d);
    except:
        print sys.exc_info()[1];
    return;



def firstInstall():
    machine_type=raw_input('Type of machine:')
    if machine_type == 'datamartextract':
        checkmakedir('/mysql/');
        sh.copyfile('/etc/my.cnf','/etc/my.cnf.bak');
        orgfl=open('/etc/my.cnf','r');
        readLines=orgfl.readlines();
        newfl=open('/etc/my.cnf.new','w');
        newlines=[];
        for ln in readLines:
            if string.find(ln,'datadir')>=0: 
                newlines.append('datadir=/mysql/');
            else:
                newlines.append(string.replace(ln,'\n',''));
        newfl.write(string.join(newlines,'\n'));        
        newfl.close();
        sh.copyfile('/etc/my.cnf.new','/etc/my.cnf');
        os.system('service mysqld stop');
        os.system('service mysqld start');
        dbname='ods';
        username='datamart';
        passwd='datamart';
        host='localhost';

        cnx=mysqlcon.connectMysql('','root','','localhost');
        mysqlcon.CheckCreateDb(dbname,cnx);
        mysqlcon.CheckCreateUser(dbname,username,passwd,cnx);


def main():
    firstInstall();
    
if __name__ == '__main__':
    main();   


