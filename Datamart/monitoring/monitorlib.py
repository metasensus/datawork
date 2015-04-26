#!/usr/bin/env python
#-*- coding: utf-8 -*-
import oracleconnect as oracon;
import mysqlconnect as mysql;
import string;
import time;
import sys;
from multiprocessing import Process;
import multiprocessing
import os;
import hostlib as hst;
import commands;
import codecs;
import smtplib;
import email.utils;
from datetime import datetime;
from email.mime.text import MIMEText;

#datamart monitoring process

#this process will get the host name of the machine
def get_hostname():
    global machine_id,machine_name;
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);
    hstip=hst.retHostIP();
    sqlstmt= 'select purpose,machineid,name from STATSPROCESSINGMACHINE where IPADDRESS=\''+hstip+'\'';
    purrec=oracon.execSql(oraconn,sqlstmt);
    purpose='';
    for prec in purrec:
        purpose=prec[0];
        machine_id=prec[1];
        machine_name=prec[2];
    oraconn.close();
    return machine_name;

#this process will check if mysql process is running or not
def check_mysql():
    
   
    mysqlstatus= "service mysqld status"
    log=commands.getoutput(mysqlstatus);
    if string.find(log,'mysqld (pid) is running...'):
        mysqlflag='1';
        
    else:
        mysqlflag='0';
        
    return mysqlflag;
    
#this process will check if datamart is running or not
def datamart_process(machine_id):
    
    connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521'
    oraconn = oracon.openconnect(connstr);
    sqlstmt='select NAME, STATUS_POST_TIME, PURPOSE, DISKSPACE_UTIL,DATAMART_STATUS FROM omi_machine_status_v  where MACHINEID=\''+str(machine_id)+'\'';
    OracleResultset = oracon.execSql(oraconn,sqlstmt);
    
    for data in OracleResultset:
        name=data[0];
        status_time=data[1];
        purpose=data[2];
        diskspace=data[3];
        datamart=data[4];
    oraconn.close();
    return datamart;
    
   
#this process will check dates 
def extract_check(machineid):
    ex_tablearray=[];
    connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521'
    oraconn = oracon.openconnect(connstr);
    
    sqlstmt="select count(1) from omi_extract_v where table_id in (select table_id from datamart_source where machineid="+machineid+")";
    CountResultset = oracon.execSql(oraconn,sqlstmt);
    for rec in CountResultset:
        count=rec[0];
    
    if(count==0):
        tablename_e= 1;
        agg_e=1;
        ext_e=1;
        ext_flag=1;
        ex_tablearray.append([ext_flag,tablename_e,agg_e,ext_e]);
        
        
    if(count>0):
        sqlstmt="select AGG_UPDATE_DATE, EXTRACTED from omi_extract_v where table_id in (select table_id from datamart_source where machineid="+machineid+")";
        ExtractResultset=oracon.execSql(oraconn,sqlstmt);
         
        for rec in ExtractResultset:
            ext_flag=0;
            tablename_e= rec[0];
            agg_e=rec[1];
            ext_e=rec[2];
            ex_tablearray.append([ext_flag,tablename_e,agg_e,ext_e]);
            
           
           
    oraconn.close();
    
    return ex_tablearray;

def sync_check():
    
    sync_tablearray=[];
    connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521'
    oraconn = oracon.openconnect(connstr);
    
    sqlstmt="select count(1) from omi_sync_v";
    CountResultset = oracon.execSql(oraconn,sqlstmt);
    for rec in CountResultset:
        synccount=rec[0];
    
    if(synccount == 0):
        tablename_s = 1;
        agg_s = 1;
        ext_s = 1;
        rep_s = 1;
        s_flag = 1;
        sync_tablearray.append([s_flag,tablename_s,agg_s,ext_s,rep_s]);
        
    if(synccount>0):
        sqlstmt="select TABLE_NAME,AGG_UPDATE_DATE,EXTRACTED,REPLICATION_DATE from omi_sync_v"
        SyncResultset=oracon.execSql(oraconn,sqlstmt);
         
        for rec in SyncResultset:
            s_flag = 0;
            tablename_s = rec[0];
            agg_s = rec[1];
            ext_s = rec[2];
            rep_s = rec[3];
            sync_tablearray.append([s_flag,tablename_s,agg_s,ext_s,rep_s]);
           
           
    oraconn.close();
    
    return sync_tablearray;

def montioring():
    emailList=[
        'vrushabh.shah@hp.com','rangadhama.krishnappa@hp.com','dnair@hp.com'
    ]
    hostname=get_hostname();
    sqlstmt='select machineid from statsprocessingmachine where name=\''+hostname+'\'';
    MachineResultset=oracon.execSql(oraconn,sqlstmt);
    for rec in MachineResultset:
        Machine=rec[1];
    
    mysql_chk=check_mysql();
    datamart_chk=datamart_process(Machine);
    
    extract_chk=extract_check(Machine);
    sync_chk=sync_check();
    
    for data in extract_chk:
        ex_flg=data[0];
    
    for rec in sync_chk:
        s_flg=rec[0];
    
    if(mysql_chk == 0 or datamart_chk == 0 or ex_flg == 0 or s_flg == 0 ):
        
        if(mysql_chk==0):
            mysqlmsg= "Failure";
        else:
            mysqlmsg= "OK";
        
        if(datamart_chk==0):
            datamartmsg="Failure";
        else:
            datamartmsg="OK";
            
        if(ex_flg==0):
            ex_flg_msg="Delayed";
        else:
            ex_flg_msg= "OK";
        
        if(s_flg==0):
            s_flg_msg="Delayed";
        else:
            s_flg_msg= "OK";
            
        for emailID in emailList:
            FROM="datamart@hp.com"
            TO = emailID
            EMAIL_SUB="Datamart Error Occured in "+str(hostname);

            message="\nMYSQL STATUS   "+mysqlmsg;
            message+="\nDatamart STATUS   "+datamartmsg;
            message+="\nExtraction   "+str(ex_flg_msg);
            message+="\nSync Process   "+str(s_flg_msg);
            message+="\n";
            message+="\n";
            message+="\n";
        
            message+="==============================Extraction Process================================="
            for dat in extract_chk:
            
                message+="\nTable Name "+str(dat[1])+"\t Aggregation Date "+str(dat[2])+"\t Extraction Date "+str(dat[3]);
        
                message+="\n";
            message+="\n";
            message+="\n";
            message+="==============================Sync Process===================================="
            
            for ret in sync_chk:    
                message+="\nTable Name "+str(dat[1])+"\t Aggregation Date "+str(dat[2])+"\t Extraction Date "+str(dat[3]);
        
                message+="\n"    
            message+="\n";       
            msg=MIMEText(message)
            msg["Subject"]=EMAIL_SUB
            msg["Message-id"]=email.Utils.make_msgid()
            msg["To"]=TO
            msg["From"]=FROM
            host="smtp.hp.com"
            server=smtplib.SMTP(host)
            server.sendmail(FROM,TO,msg.as_string())
            server.quit()
    
    else:    
        for emailID in emailList:
            FROM="datamart@hp.com"
            TO = emailID
            EMAIL_SUB="Datamart No Error for this hour"
            
            message="\n Success";
            
            message+="\n";       
            msg=MIMEText(message)
            msg["Subject"]=EMAIL_SUB
            msg["Message-id"]=email.Utils.make_msgid()
            msg["To"]=TO
            msg["From"]=FROM
            host="smtp.hp.com"
            server=smtplib.SMTP(host)
            server.sendmail(FROM,TO,msg.as_string())
            server.quit()
          
        


        
    
    
    
