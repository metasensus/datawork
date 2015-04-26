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

oraconn=oracon.openconnect(constr);
sql='select purpose from statsprocessingmachine where ipaddress=\''+ipadd+'\''
recon=oracon.execSql(oraconn,sql);

for rec in recon:
    purpose=rec[0];

try:
    while True:
        if purpose == 'splitter':
            print 'select min(STATS_FILEID) minid,max(STATS_FILEID) maxid from STATS_SPLIT_FILES where ifnull(STATS_FILEID,0)>0';
            lcurr.execute('select min(STATS_FILEID) minid,max(STATS_FILEID) maxid from STATS_SPLIT_FILES where ifnull(STATS_FILEID,0)>0');
            minMax=lcurr.fetchall();
            for mrec in minMax:
                minVal=mrec[0];
                maxVal=mrec[1];
            if maxVal:
                newmax=minVal + 1000;
                totrec=0;
                recount=0;
                sql='begin'
                
                while newmax < maxVal:
                    #print 'Selecting data between'+str(minVal)+' and STATS_FILEID <='+str(newmax)+'.......';
                    lcurr.execute('select count(1) from STATS_SPLIT_FILES where STATS_FILEID >='+str(minVal)+' and STATS_FILEID <='+str(newmax));
                    crecord=lcurr.fetchall();
                    for crec in crecord:
                        reccount=crec[0];
                    
                    print 'Number of records available:'+str(reccount);
                    
                    lcurr.execute('select STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, STATS_SPLIT_FILE_TYPE from STATS_SPLIT_FILES where STATS_FILEID >='+str(minVal)+' and STATS_FILEID <='+str(newmax));
                    MysqlData=lcurr.fetchall();
                    #print 'Deleting data between '+str(minVal)+'  and '+str(newmax)+' from STAT_SPLIT_FILES';
                    lcurr.execute('delete from STATS_SPLIT_FILES where STATS_FILEID >='+str(minVal)+' and STATS_FILEID <='+str(newmax));
                    if MysqlData:
                        for myRec in MysqlData:
                            sqlstmt=' insert into splitfiletmpstore (STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, FILE_CREATE_DATE, STATS_SPLIT_FILE_TYPE) values ';
                            sqlstmt+='('+str(myRec[0])+',\''+myRec[1]+'\',\''+myRec[2]+'\','+str(myRec[3])+',sysdate,\''+myRec[4]+'\');';
                            sql+=sqlstmt;
                            recount+=1;
                            #print 'Number of records in sql:'+str(recount);
                            if recount >= 1000:
                                sql+='commit; end;'
                                totrec+=recount;
                                recount=0;
                                oracon.execSql(oraconn,sql);
                                print 'Copied :'+str(totrec)+'.........';
                                sql='begin'
                            
                    
                    minVal=newmax;
                    newmax=newmax+1000;
                    if minVal > maxVal:
                        break;
                
                if recount >0:
                    sql+='commit; end;'
                    totrec+=recount;
                    recount=0;
                    oracon.execSql(oraconn,sql);
                    print 'Copied :'+str(totrec)+'.........';
                       
            print 'Cleaning STATS_SPLIT_FILES..........';            
            lcurr.execute('delete from STATS_SPLIT_FILES where STATS_FILEID >='+str(minVal)+' and STATS_FILEID <='+str(maxVal));        
            print 'Cleaning Statsprocesstransact.....';    
            lcurr.execute('delete from STATSPROCESSTRANSACT where FILE_PROCESS_STATUS=1');
            print 'All work done......'
        if purpose == 'process':
            lcurr.execute('select min(STATS_SPLITFILE_ID) minid,max(STATS_SPLITFILE_ID) maxid from STATSOUTPUT where ifnull(STATS_SPLITFILE_ID,0)>0');
            minMax=lcurr.fetchall();
            
        
            for mrec in minMax:
                minVal=mrec[0];
                maxVal=mrec[1];
        
            if maxVal:   
                lcurr.execute('select  STATS_SPLITFILE_ID,STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS,STATS_STRUCTURE_TYPE_ID from STATSOUTPUT where STATS_SPLITFILE_ID >='+str(minVal)+' and STATS_SPLITFILE_ID <='+str(maxVal));
                MysqlData=lcurr.fetchall();
            
                sql='begin'
                recount=0;
                for myRec in MysqlData:
                    sqlstmt=' insert into TMPSTATSOUTPUT (STATS_SPLITFILE_ID,STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS,STATS_STRUCTURE_TYPE_ID) values ';
                    sqlstmt+='('+str(myRec[0])+',\''+myRec[1]+'\',\''+myRec[2]+'\',sysdate,0,'+str(myRec[5])+');';
                    sql+=sqlstmt;
                    recount+=1;
                    if recount>=1000:
                        recount=0;
                        sql+='commit; end;'
                        oracon.execSql(oraconn,sql);
                        sql='begin'
                    
                sql+=' commit; end;'
                oracon.execSql(oraconn,sql);
            
                lcurr.execute('delete from STATSOUTPUT where STATS_SPLITFILE_ID >='+str(minVal)+' and STATS_SPLITFILE_ID <='+str(maxVal));
            lcurr.execute('delete from STATS_SPLIT_FILES where FILE_PROCESS_STATUS=1');
        time.sleep(1800);
except Exception:
    fl=statslog.logcreate("log/datacopy.log");
    statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));    

