#!/usr/bin/env python
import oracleconnect as oracon;
import mysqlconnect as mconnect;
import string;
import time;
import hostlib as hst;
import os;
import statslog;
import sys;

constr='ods/ods@callhomeods:1521/callhomeods';

lconn = mconnect.connectMysql('ods','procuser','c@llhome','localhost');
lcurr = lconn.cursor();
hstname=hst.retHostName();
ipadd=hst.retHostIP();

SplitConStr='ods/ods@callhomedb03:1521/callhomedb03';
ProcConStr='ods/ods@callhomedb04:1521/callhomedb04';

sqlArr=[];
MysqlArr=[];
oraconn=oracon.openconnect(constr);



sql='select purpose from statsprocessingmachine where ipaddress=\''+ipadd+'\''
recon=oracon.execSql(oraconn,sql);
for rec in recon:
    purpose=rec[0];
oraconn.close();

try:
    while True:
        if purpose == 'splitter':
            splitConn=oracon.openconnect(SplitConStr);
            lcurr.execute('select count(1) from STATS_SPLIT_FILES where ifnull(STATS_FILEID,0)>0');
            recDat=lcurr.fetchall();
            for rec in recDat:
                recCount=rec[0];
            numrec=0;
            recount=0;
            totrec=0;
            if recCount > 0:
                print "Checking for column...."
                lcurr.execute('show columns from STATS_SPLIT_FILES');
                col=lcurr.fetchall();
                colexist=0;
                for cl in col:
                    if cl[0]=='processed':
                       colexist=1;
                       break;
                if colexist==0:    
                    lcurr.execute('Alter table STATS_SPLIT_FILES add processed int');
                while numrec <= recCount:
                    lcurr.execute('update STATS_SPLIT_FILES set processed=1 where ifnull(STATS_FILEID,0)>0 limit 100000');
                    lcurr.execute('select distinct STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, STATS_SPLIT_FILE_TYPE from STATS_SPLIT_FILES where processed=1');
                    MysqlData=lcurr.fetchall();
                        
                    if MysqlData:
                        for myRec in MysqlData:
                            sqlstmt=' insert /*+ append +*/ into splitfiletmpstore (STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, FILE_CREATE_DATE, STATS_SPLIT_FILE_TYPE) values ';
                            sqlstmt+='('+str(myRec[0])+',\''+myRec[1]+'\',\''+myRec[2]+'\','+str(myRec[3])+',sysdate,\''+myRec[4]+'\');';
                            sqlArr.append(sqlstmt);
                            recount+=1;
                            if len(sqlArr) >= 1000:
                                sql='begin\n'+string.join(sqlArr,'\n')+'\n'+'commit; end;'
                                oracon.execSql(splitConn,sql);
                                totrec+=recount;
                                recount=0;    
                                sqlArr=[];
                    
                    if len(sqlArr) >= 0:
                        sql='begin\n'+string.join(sqlArr,'\n')+'\n'+'commit; end;'
                        totrec+=recount;
                        recount=0;
                        oracon.execSql(splitConn,sql);
                    lcurr.execute('delete from STATS_SPLIT_FILES where processed=1');
                    numrec+=100000;
            lcurr.execute('delete from STATSPROCESSTRANSACT where FILE_PROCESS_STATUS=1');
            splitConn.close();
            
        if purpose == 'process':
            procConn=oracon.openconnect(ProcConStr);
            lcurr.execute('select count(1) from STATSOUTPUT where ifnull(STATS_SPLITFILE_ID,0)>0');
            recDat=lcurr.fetchall();
            for rec in recDat:
                recCount=rec[0];
            numrec=0;
            totrec=0;
            
            if recCount > 0:
                while numrec <= recCount:
                    lcurr.execute('select  STATS_SPLITFILE_ID,STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS,STATS_STRUCTURE_TYPE_ID from STATSOUTPUT limit 100000');
                    MysqlData=lcurr.fetchall();
            
                    recount=0;
                    for myRec in MysqlData:
                        sqlstmt=' insert into TMPSTATSOUTPUT (STATS_SPLITFILE_ID,STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS,STATS_STRUCTURE_TYPE_ID) values ';
                        sqlstmt+='('+str(myRec[0])+',\''+myRec[1]+'\',\''+myRec[2]+'\',sysdate,0,'+str(myRec[5])+');';
                        sqlArr.append(sqlstmt);
                        MysqlArr.append('delete from STATSOUTPUT where STATS_SPLITFILE_ID='+str(myRec[0]));
                        sql+=sqlstmt;
                        recount+=1;
                        if recount>=1000:
                            recount=0;
                            sql='begin\n'+string.join(sqlArr,'\n')+'\n'+'commit; end;'
                            oracon.execSql(procConn,sql);
                            totrec+=recount;
                            recount=0;    
                            sqlArr=[];
                        
                    if len(sqlArr) >= 0:
                            sql='begin\n'+string.join(sqlArr,'\n')+'\n'+'commit; end;'
                            totrec+=recount;
                            oracon.execSql(procConn,sql);
                            recount=0;
                            
                    for Mrec in MysqlArr:
                        lcurr.execute(Mrec);
                    numrec+=100000;        
                
            lcurr.execute('delete from STATS_SPLIT_FILES where FILE_PROCESS_STATUS=1');
            procConn.close();
        time.sleep(900);
except Exception:
    fl=statslog.logcreate("log/dataCopy.log");
    statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));    
