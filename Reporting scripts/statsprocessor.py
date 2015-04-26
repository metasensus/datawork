#!/usr/bin/env python
import sys;
import string;
import oracleconnect as oracon;
import mysqlconnect as mysqlconn;
import hostlib;
import statslog;
import processinglib as prclib;
from statsfilelib import checkmakedir;
from multiprocessing import Process;
import os;
import commands;
import time;

constr='ods/ods@callhomeods:1521/callhomeods';

def versionExist(STATS_SPLITFILE_TYPE):
    noVersion={'alertdnew','alertdact','alertdfixed','hwnode','hwnodepci','hwnodecpu','hwnodeint','hwnodemem','hwcageint','hwcagemid','hwport','hwbattery','hwnodepower','hwcagemag','hwcagepower'};
    
    for st in noVersion:
        if string.strip(STATS_SPLITFILE_TYPE) == st:
            return 0;
    return 1;
        
def processFile(STATS_SPLITFILE_ID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATS_SPLITFILE_TYPE ,INSERVSERIAL ,DATAFOLDER ,MACHINEID ):
    try:
        fl=statslog.logcreate("log/"+STATS_SPLITFILE_NAME);
        lc = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
        curr=lc.cursor();
        
        errlog='log/'+STATS_SPLITFILE_NAME+'.err'
        if versionExist(STATS_SPLITFILE_TYPE) == 1:
            version=prclib.getVersion(STATS_SPLITFILE_NAME,STATS_SPLITFILE_PATH);
        else:
            version='2.3.1';
            
        sqlstmt='SELECT COUNT(1) FROM STATSSTRUCTURE WHERE STATS_VERSION=\''+version+'\' and STATSNAME=\''+STATS_SPLITFILE_TYPE+'\''
        curr.execute(sqlstmt);
        dat=curr.fetchall();
        for datrec in dat:
            count=datrec[0];
        
        if count > 0:
            sqlstmt='SELECT STATS_STRUCTURE_ID FROM STATSSTRUCTURE WHERE STATS_VERSION=\''+version+'\' and STATSNAME=\''+STATS_SPLITFILE_TYPE+'\'';
            curr.execute(sqlstmt);
            verdat=curr.fetchall();
            STATS_STRUCTURE_ID=0
            for ver in verdat:
                STATS_STRUCTURE_ID=ver[0];
                
            if STATS_STRUCTURE_ID >0:
                sqlstmt='SELECT PROCESS_SEQUENCE,FUNCTIONID,PARAMETER_VALUES FROM PROCESSLOGIC WHERE STATS_STRUCTURE_ID='+str(STATS_STRUCTURE_ID)+' ORDER BY PROCESS_SEQUENCE';
                curr.execute(sqlstmt);
                prcdat=curr.fetchall();
            
                linearray=prclib.readFile(STATS_SPLITFILE_NAME,STATS_SPLITFILE_PATH);
                for prc in prcdat:
                    sqlstmt='SELECT FUNCTIONNAME FROM PROCESSFUNCTION WHERE FUNCTIONID='+str(prc[1])    
                    curr.execute(sqlstmt);
                    funcNamerec=curr.fetchall();
                    for fu in funcNamerec:
                        functionName=fu[0];
                    func=getattr(prclib,functionName);
                    linearray=func(linearray);
            
                filepath=DATAFOLDER+'/'+str(INSERVSERIAL)+'/';
                checkmakedir(filepath,errlog);
                outputFl=open(filepath+STATS_SPLITFILE_NAME,'w');
                outputFl.write(linearray);
                sqlstmt='insert into STATSOUTPUT(STATS_SPLITFILE_ID,STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS,STATS_STRUCTURE_TYPE_ID) values '
                sqlstmt+='('+str(STATS_SPLITFILE_ID)+',\''+STATS_SPLITFILE_NAME+'\',\''+filepath+'\',sysdate(),0,'+str(STATS_STRUCTURE_ID)+')';
                curr.execute(sqlstmt);
                sqlstmt='Update STATS_SPLIT_FILES set FILE_PROCESS_STATUS=1 where STATS_SPLITFILE_ID='+str(STATS_SPLITFILE_ID);
                curr.execute(sqlstmt); 
                curr.close();
                lc.close();
            else:
                sqlstmt='DELETE FROM STATS_SPLIT_FILES WHERE STATS_SPLITFILE_ID='+str(STATS_SPLITFILE_ID);
                curr.execute(sqlstmt);
                curr.close();
                lc.close();
        else:
            fl=statslog.logcreate("log/"+STATS_SPLITFILE_NAME);
            statslog.logwrite(fl,"Error reported: Version "+version+" not yet supported");
            sqlstmt='Update STATS_SPLIT_FILES set FILE_PROCESS_STATUS=3 where STATS_SPLITFILE_ID='+str(STATS_SPLITFILE_ID);
            lc = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
            curr=lc.cursor();
            curr.execute(sqlstmt);
            curr.close();
            lc.close();
    except:
        fl=statslog.logcreate("log/"+STATS_SPLITFILE_NAME);
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
        sqlstmt='Update STATS_SPLIT_FILES set FILE_PROCESS_STATUS=3 where STATS_SPLITFILE_ID='+str(STATS_SPLITFILE_ID);
        lc = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
        curr=lc.cursor();
        curr.execute(sqlstmt);
        curr.close();
        lc.close();
    
def procExec():
    try:
        oraconn=oracon.openconnect(constr);
        lconn = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
    
        lcurr=lconn.cursor();
        ipadd=hostlib.retHostIP();

        sqlstmt='select machineid,numberofthreads,processloc,number_of_files_per_run,delay_seconds from statsprocessingmachine where ipaddress=\''+ipadd+'\' and enable =1';
        numthreadrec=oracon.execSql(oraconn,sqlstmt);
        for numthread in numthreadrec:
            numthreads = numthread[1];
            machineid=numthread[0];
            datafolder=numthread[2];
            files_per_run=numthread[3];
            delay_seconds=numthread[4];

        #statslog.logwrite(fl,str(numthreads)+':'+str(files_per_run)+':'+str(delay_seconds));
        numthreads=int(numthreads);
        
        sqlstmt='select distinct statsname from STATSSTRUCTURE a,PROCESSLOGIC b where a.STATS_STRUCTURE_ID = b.STATS_STRUCTURE_ID'
        
        lcurr.execute(sqlstmt);
        recc=lcurr.fetchall();
        statList=[];
        for rec in recc:
            statList.append('\''+rec[0]+'\'');
        statsStr=string.join(statList,',');
        sqlstmt='update  STATS_SPLIT_FILES set FILE_PROCESS_STATUS=0 WHERE FILE_PROCESS_STATUS=2';
        lcurr.execute(sqlstmt);
                
        sqlstmt='select count(1) from STATS_SPLIT_FILES where ifnull(FILE_PROCESS_STATUS,0)=0 and STATS_SPLIT_FILE_TYPE in ('+statsStr+')';
        
        lcurr.execute(sqlstmt);
        recc=lcurr.fetchall();
    
        numRec=0;
        for rec in recc:
            numRec=rec[0];
    
        numthreadstocopy = numthreads - numRec;
        #statslog.logwrite(fl,str(numthreadstocopy));
        
        if numthreadstocopy > 0:
            sqlstmt='';
            rows=1000;
            totrows=rows;
            while totrows <= numthreadstocopy:
                sqlstmt='select STATS_FILEID,STATS_SPLITFILE_ID,STATS_SPLITFILE_NAME,STATS_SPLITFILE_PATH,STATSID,STATS_SPLIT_FILE_TYPE from STATS_SPLIT_FILES ';
                sqlstmt+=' where FILE_PROCESS_STATUS=0 and MACHINEID='+str(machineid)+' and ROWNUM <='+str(rows)+' and STATS_SPLIT_FILE_TYPE in ('+statsStr+') order by STATS_SPLITFILE_ID desc';
                #statslog.logwrite(fl,sqlstmt);    
                FileRec=oracon.execSql(oraconn,sqlstmt);
                
                sqlstmt='INSERT INTO STATS_SPLIT_FILES (STATS_FILEID,STATS_SPLITFILE_ID,STATS_SPLITFILE_NAME,STATS_SPLITFILE_PATH,STATSID,STATS_SPLIT_FILE_TYPE) VALUES ';
                sql=[];
                    
                for flrec in FileRec:
                    sql.append('('+str(flrec[0])+','+str(flrec[1])+',\''+flrec[2]+'\',\''+flrec[3]+'\','+str(flrec[4])+',\''+flrec[5]+'\')');
                    sqlst='begin Update STATS_SPLIT_FILES set FILE_PROCESS_STATUS=2 where STATS_SPLITFILE_ID='+str(flrec[1])+'; commit; end;';
                    oracon.execSql(oraconn,sqlst);
    
                if len(sql) > 0:
                    sqlstmt+=string.join(sql,',');
                    lcurr.execute(sqlstmt);
                totrows=totrows+rows;
        pythonRuns=commands.getoutput("ps -ef | grep parsetabber | grep -v grep | wc -l");
        while int(pythonRuns) -3 <= files_per_run:
            sqlstmt='SELECT a.STATS_SPLITFILE_ID,a.STATS_SPLITFILE_NAME,a.STATS_SPLITFILE_PATH,STATS_SPLIT_FILE_TYPE from  STATS_SPLIT_FILES a where ifnull(FILE_PROCESS_STATUS,0)=0  and  STATS_SPLIT_FILE_TYPE in ('+statsStr+') LIMIT '+str(files_per_run - int(pythonRuns));   
            #statslog.logwrite(fl,sqlstmt);
            if (files_per_run - int(pythonRuns)) >0:
                ctr=0;
                lcurr=lconn.cursor();
                lcurr.execute(sqlstmt);
                filelist=lcurr.fetchall();
            
                for flrec in filelist:
                    flpthlist=string.split(flrec[2],'/');
                    inservserial=flpthlist[len(flpthlist)-2];
                    p=Process(target=processFile,args=(flrec[0],flrec[1],flrec[2],flrec[3],inservserial,datafolder,machineid,));
                    p.daemon = True;
                    p.start();
                    ctr+=1;    
                    sqlstmt='Update STATS_SPLIT_FILES set FILE_PROCESS_STATUS=2 where STATS_SPLITFILE_ID='+str(flrec[0]);
            
                    lcurr.execute(sqlstmt);
                lcurr.close();
                if ctr==0:
                    break;
                pythonRuns=commands.getoutput("ps -ef | grep parsetabber | grep -v grep | wc -l");
                while int(pythonRuns) > files_per_run:
                    time.sleep(delay_seconds);
                    pythonRuns=commands.getoutput("ps -ef | grep parsetabber | grep -v grep | wc -l");
            pythonRuns=commands.getoutput("ps -ef | grep parsetabber | grep -v grep | wc -l");
            
        lconn.close();       
    except:
        fl=statslog.logcreate("log/parser.log");
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
        