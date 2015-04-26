#!/usr/bin/env python
import string;
import vertica_connect as vconn;
from multiprocessing import Process;
import os;
import sys;
import time;
import commands;

#Read filename from db
# This process will read data from the ods statsouput table
import oracleconnect as oracon;


def GetAllStructure(STATSID):
    sql='SELECT DISTINCT STATSID,STATSNAME,VERTICA_COLUMN_POSITION,STATS_COLUMN_NAME FROM STATSSTRUCTURE A, STATSSTRUCTUREDETAIL B WHERE A.STATSID='+str(STATSID)+' AND A.STATS_STRUCTURE_ID = B.STATS_STRUCTURE_ID AND VERTICA_ENABLED = 1 ORDER BY STATSID, VERTICA_COLUMN_POSITION';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    structureRec=oracon.execSql(oraconn,sql);
    structList=[];
    for strec in structureRec:
        structList.append(strec[3]+':'+str(strec[2]));
    structureRec.close();
    oraconn.close(); 
    return structList;

def GetCurrentStructure(STATSSTRUCTUREID):
    sql='SELECT STATS_COLUMN_ID,STATS_COLUMN_NAME FROM STATSSTRUCTUREDETAIL WHERE STATS_STRUCTURE_ID='+str(STATSSTRUCTUREID)+' ORDER BY STATS_COLUMN_ID';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    structureRec=oracon.execSql(oraconn,sql);
    structList=[];
    for strec in structureRec:
        structList.append(strec[1]);
    structureRec.close();
    oraconn.close(); 
    return structList;

def GetEnabledData():
    sql='SELECT STATS_STRUCTURE_ID,STATSID,STATSNAME  FROM STATSSTRUCTURE WHERE VERTICA_ENABLED=1';
    
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    enableFileListrec=oracon.execSql(oraconn,sql);
    
    dataEnabledList=[];
    
    for enrec in enableFileListrec:
        dataEnabledList.append(str(enrec[0])+':'+str(enrec[1])+':'+enrec[2]);
    
    enableFileListrec.close();
    oraconn.close();
    
    return string.join(dataEnabledList,',');

def GetVerticaRowCount():
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sql='SELECT COUNT(1) FROM DATASTORE.STATSOUTPUT';
        vdat=vconn.vertica_sql_execute(conn,sql);
        datres=vdat.fetchall();
        recount=0;
        for datrec in datres:
            recount=datrec[0];
        return recount;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def CheckIdinVertica(statsoutputid):
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sql='SELECT COUNT(1) FROM DATASTORE.STATSOUTPUT WHERE STATS_OUTPUTFILE_ID='+str(statsoutputid);
        vdat=vconn.vertica_sql_execute(conn,sql);
        datres=vdat.fetchall();
        recount=0;
        for datrec in datres:
            recount=datrec[0];
        return recount;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def GetMaxFileIDfromVertica():
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sql='SELECT COUNT(1) FROM DATASTORE.STATSOUTPUT';
        vdat=vconn.vertica_sql_execute(conn,sql);
        datres=vdat.fetchall();
        for datrec in datres:
            recount=datrec[0];
        if recount >0:
            sql='select max(STATS_OUTPUTFILE_ID) from datastore.STATSOUTPUT';
        
            vdat=vconn.vertica_sql_execute(conn,sql);
            valres=vdat.fetchall();
            for valrec in valres:
                maxVal=valrec[0];
        else:
            maxVal=0;
        vdat.close();
        conn.close();
        return maxVal;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def GetMaxCreateDate():
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sql='SELECT COUNT(1) FROM DATASTORE.STATSOUTPUT';
        vdat=vconn.vertica_sql_execute(conn,sql);
        datres=vdat.fetchall();
        for datrec in datres:
            recount=datrec[0];
        if recount >0:
            sql='SELECT TO_CHAR(MAX(STATS_FILE_CREATE_DATE),\'YYYYMMDDHH24MISS\') from datastore.STATSOUTPUT';
        
            vdat=vconn.vertica_sql_execute(conn,sql);
            valres=vdat.fetchall();
            for valrec in valres:
                maxdate=valrec[0];
        else:
            maxdate=0;
        vdat.close();
        conn.close();
        return maxdate;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def countProcess(processName):
    processcount=int(commands.getoutput('ps -ef | grep '+processName+' | wc -l'));
    return processcount;

def loaddata(dataList):
    copyFile='/root/proc/data/fileList'+time.strftime('%Y%m%d%H%M%S');
    recordCopyFile=open(copyFile,'w');
    recordCopyFile.write(string.join(dataList,'\n'));
    recordCopyFile.close()
    
    sql='/opt/vertica/bin/vsql -c "COPY DATASTORE.STATSOUTPUT (STATS_SPLITFILE_ID, STATS_OUTPUTFILE_ID, STATS_OUTPUTFILE_NAME, STATS_OUTPUTFILE_PATH,STATS_FILE_CREATE_DATE,STATS_FILE_LOAD_STATUS, STATS_STRUCTURE_TYPE_ID) FROM LOCAL \''+copyFile+'\' record terminator E\'\\n\' delimiter \'|\'" -U dbadmin -w c@llhome -d callhomedb -h callhomelab-vertica01';
    #print sql;
    executeSql=commands.getoutput(sql);
    #print executeSql;
    os.system('rm -rf '+copyFile);
    


#Load data to Vertica
def GetAddFileList():
    try:
        recordCount=0;
        data='';
        dataList=[]
        totalCount=0;
        minOutputId=0;
        
        while (1):
            print 'Getting max record in Vertica.....'
            maxCreateDate=GetMaxCreateDate();
            print 'Max date in Vertica '+str(maxCreateDate);
            constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
            oraconn=oracon.openconnect(constr);
            if maxCreateDate == 0 and totalCount == 0 :
                print 'Getting min....'
                sql='SELECT TO_CHAR(MIN(STATS_FILE_CREATE_DATE),\'YYYYMMDDHH24MISS\') FROM STATSOUTPUT'
                #WHERE STATS_STRUCTURE_TYPE_ID IN ('+str(STATS_STRUCTURE_ID)+')';
                recCount=0;
                minRec=oracon.execSql(oraconn,sql);
                for mrec in minRec:
                    minOutputId=mrec[0];
            else:
                minOutputId=maxCreateDate;
                
            
            #print 'Reading data....'
            #print minOutputId; 
            #STATS_STRUCTURE_TYPE_ID IN ('+str(STATS_STRUCTURE_ID)+') AND
            sql='SELECT COUNT(1) FROM STATSOUTPUT WHERE  STATS_FILE_CREATE_DATE BETWEEN TO_DATE('+str(minOutputId)+',\'YYYYMMDDHH24MISS\') AND TO_DATE('+str(minOutputId)+',\'YYYYMMDDHH24MISS\')+60';
            CountRec=oracon.execSql(oraconn,sql);
            for ct in CountRec:
                records=ct[0];
                
            sql='SELECT STATS_SPLITFILE_ID, STATS_OUTPUTFILE_ID, STATS_OUTPUTFILE_NAME, STATS_OUTPUTFILE_PATH, TO_CHAR(STATS_FILE_CREATE_DATE,\'MM/DD/YYYY HH24:MI:SS\') STATS_FILE_CREATE_DATE, STATS_STRUCTURE_TYPE_ID FROM STATSOUTPUT WHERE  STATS_FILE_CREATE_DATE BETWEEN TO_DATE('+str(minOutputId)+',\'YYYYMMDDHH24MISS\') AND TO_DATE('+str(minOutputId)+',\'YYYYMMDDHH24MISS\')+60';
            #print sql;
            CopyList=oracon.execSql(oraconn,sql);
            print 'Data retrieved....'+str(records);
            
            for datrec in CopyList:
                STATS_SPLITFILE_ID=datrec[0]; 
                STATS_OUTPUTFILE_ID=datrec[1];
                STATS_OUTPUTFILE_NAME=datrec[2];
                STATS_OUTPUTFILE_PATH=datrec[3];
                STATS_FILE_CREATE_DATE=datrec[4];
                STATS_FILE_LOAD_STATUS=0;
                STATS_STRUCTURE_TYPE_ID=datrec[5];
                #ALTERNATEPATH=datrec[6];
                if CheckIdinVertica(STATS_OUTPUTFILE_ID) == 0:
                    data=str(STATS_SPLITFILE_ID)+'|'+str(STATS_OUTPUTFILE_ID)+'|'+STATS_OUTPUTFILE_NAME+'|'+STATS_OUTPUTFILE_PATH+'|'+STATS_FILE_CREATE_DATE+'\'|'+str(STATS_FILE_LOAD_STATUS)+'|'+str(STATS_STRUCTURE_TYPE_ID);
                    dataList.append(data);
                    recordCount+=1;
                    totalCount+=1;
                
                if recordCount > 1000000:
                    loaddata(dataList);
                    recordCount=0;
                    dataList=[];
            
            if len(dataList)>0:
                loaddata(dataList);
                recordCount=0;
                dataList=[];
                
            RowCount=GetVerticaRowCount();
            print 'Total rows in Vertica '+str(RowCount)+' total inserted :'+str(totalCount);   
            CopyList.close();
            oraconn.close();
            
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def optimusTagger():
    GetAddFileList();


def GetFiles(StatsStructureId,maxFilesToprocess):
    dbhost='callhomelab-vertica01';
    dbuser='dbadmin';
    passwd='c@llhome';
    dbsource='callhomedb';
    sql='SELECT STATS_OUTPUTFILE_NAME,STATS_OUTPUTFILE_PATH,SUBSTR(STATS_OUTPUTFILE_NAME,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,1) + 1,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,2) -  INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,1) -1) INSERVSERIAL,SUBSTR(STATS_OUTPUTFILE_NAME,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,2) + 1,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) -  INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,2) -1) ||\' \'||SUBSTR(STATS_OUTPUTFILE_NAME,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) + 1,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,4) -  INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) -1) DATDATE,STATS_OUTPUTFILE_ID,TO_CHAR(TO_TIMESTAMP(SUBSTR(STATS_OUTPUTFILE_NAME,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,2) + 1,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) -  INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,2) -1) ||\' \'||SUBSTR(STATS_OUTPUTFILE_NAME,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) + 1,INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,4) -  INSTR(STATS_OUTPUTFILE_NAME,\'.\',1,3) -1),\'YYmmdd HH24MISS\'),\'MM/DD/YYYY HH24:MI:SS\') datadatetype FROM datastore.STATSOUTPUT WHERE STATS_STRUCTURE_TYPE_ID='+str(StatsStructureId)+' AND NVL(PROCESSED_STATUS,0)=0 LIMIT '+str(maxFilesToprocess);
    conn=vconn.vertica_connect(dbhost,5433,dbuser,passwd,dbsource);
    vdat=vconn.vertica_sql_execute(conn,sql);
    dbresulSet=vdat.fetchall();
    
    fileList=[];
    
    for rec in dbresulSet:
        flrec=[];
        flrec.append(str(rec[0]));
        flrec.append(str(rec[1]));
        flrec.append(str(rec[2]));
        flrec.append(str(rec[3]));
        flrec.append(str(rec[4]));
        flrec.append(str(rec[5]));
        
        fileList.append(flrec);
    
    vdat.close();
    conn.close();
    return fileList;

def logger(error,filename):
    errFl=open('log/'+filename,'w');
    timestr=time.strftime('%m/%d/%Y %H:%M:%S');
    errFl.write(timestr+error);
    errFl.close();
    
def checkcreatehdfsfolder(datatype,yymmdd,inserv):
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=MKDIRS&user.name=hadoop"');
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'?op=MKDIRS&user.name=hadoop" ');
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'/'+str(inserv)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'/'+str(inserv)+'?op=MKDIRS&user.name=hadoop" &');

def InsertCopyToVertica(fileId,copyStmt,copyfilename,logfilename):
    try:
        fl=open('data/'+copyfilename,'a');
        fl.write(str(fileId)+'|'+copyStmt+'|'+time.strftime('%m/%d/%Y %H%M%S')+'\n');
        fl.flush();
        fl.close()
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);

def UpdateProcessDoneInVertica(fileId,STATUS,logfilename):
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sql='UPDATE DATASTORE.STATSOUTPUT SET PROCESSED_STATUS='+str(STATUS)+',PROCESSED_DATE=now() WHERE STATS_OUTPUTFILE_ID='+str(fileId);
        vdat=vconn.vertica_sql_execute(conn,sql);
        conn.commit();
        vdat.close();
        conn.close();
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);              
    
def ReadWriteDatatoHdfsVertica(path,flName,inservserial,datadate,currentStructure,fileId,StatsName,datadateType,copyFileName):
    try:
        inputFl=open(path+flName,'r');
        lines=inputFl.readlines();
        logFileName='errorlog_'+str(fileId);
        #newDat=string.replace(lines,'\t','|');
        inputFl.close()
        procArr=[];
        for ln in lines:
            newLn=inservserial+'|'+datadateType+'|'+string.replace(string.replace(ln,'\t','|'),'\n','')+'|'+fileId;
            procArr.append(newLn);
        daypart=string.split(datadate,' ')[0];
        
        
        checkcreatehdfsfolder(StatsName,daypart,inservserial);
        outFl=open('data/'+flName,'w');
        outFl.write(string.join(procArr,'\n'));
        outFl.close();            
        
        flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+StatsName+'/'+string.strip(str(daypart))+'/'+string.strip(str(inservserial))+'/'+flName+'.'+str(inservserial)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
        if string.find(flstatus,'FileNotFoundException')>0:
            flstatus=commands.getoutput('curl -v -X PUT -T /root/proc/data/'+flName+' "http://callhomelab-17:50075/webhdfs/v1/'+StatsName+'/'+string.strip(str(daypart))+'/'+string.strip(str(inservserial))+'/'+flName+'?op=CREATE&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004&overwrite=true"')
            if string.find(flstatus,'Exception')<0:
                os.system('rm -rf /root/proc/data/'+flName);
                copystmt='copy datastore.'+StatsName+'(INSERVSERIAL,DATADATE,'+string.join(currentStructure,',')+',FILEID) SOURCE Hdfs(url=\'http://callhomelab-01:50075/webhdfs/v1/'+StatsName+'/'+string.strip(str(daypart))+'/'+string.strip(str(inservserial))+'/'+flName+'\',username=\'hadoop\') ABORT ON ERROR;';
                InsertCopyToVertica(fileId,copystmt,copyFileName,logFileName);        
                UpdateProcessDoneInVertica(fileId,1,logFileName);        
            else:
                UpdateProcessDoneInVertica(fileId,-1,logFileName);
                logger('Processing file failed.....',logFileName);
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);
        
def uploadLoadableData(copyfileName,ServerName,logfilename):
    try:
        errExec=commands.getoutput('/opt/vertica/bin/vsql -c "COPY DATASTORE.VERTICA_COPY (STATS_OUTPUTFILE_ID, COPYSTMT,ADDED_DATE) FROM LOCAL \'/root/proc/data/'+copyfileName+'\' record terminator E\'\\n\' delimiter \'|\'" -U dbadmin -w c@llhome -d callhomedb -h '+ServerName);
        if string.find(string.lower(errExec),'error')>=0:
            logger("Error reported: Could not load data for "+copyfileName+" Error "+errExec,logfilename);
        else:
            errExec=commands.getoutput('rm -rf /root/proc/data/'+copyfileName);
            if string.find(string.lower(errExec),'cannot')>=0:
                logger("Error reported: Could not remove file "+copyfileName+" Error "+errExec,logfilename);
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);
        
def GetFileSize(filename):
    fl=open(filename);
    lines=fl.readlines();
    fl.close();  
    return len(lines);
    
def ProcessFiles(StatsStructureId,StatsId,StatsName,ServerName):
    logfilename=StatsName+'.log';
    try:
        maxFilesToprocess=100000;
        copyFileName=StatsName+'_'+str(StatsStructureId);
        completeStructure=GetAllStructure(StatsId);
        currentStructure=GetCurrentStructure(StatsStructureId);
        errExec=commands.getoutput('rm -rf /root/proc/data/'+copyFileName);
        if string.find(string.lower(errExec),'cannot')>=0:
            logger("Error reported: Could not remove file "+copyfileName+" Error "+errExec,logfilename);    
        else:
            fileList=GetFiles(StatsStructureId,maxFilesToprocess);
            
            for fl in fileList:
                path=fl[1];
                flName=fl[0];
                inservserial=fl[2];
                datadate=fl[3];
                fileId=fl[4];
                datadateType=fl[5];
            
                p=Process(target=ReadWriteDatatoHdfsVertica,args=(path,flName,inservserial,datadate,currentStructure,fileId,StatsName,datadateType,copyFileName,));  
                p.start()
            
                processName='optimustest';
                numprocess=countProcess(processName);
                while numprocess > 100:
                    time.sleep(15);
                    numprocess=countProcess(processName);
            
            uploadLoadableData(copyFileName,ServerName,logfilename);
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);
    
    
def optimusProcessor():
    structureList=string.split(GetEnabledData(),',');
    serverCount=1
    for statsrec in structureList:
        ServerName='callhomelab-vertica0'+str(serverCount)
        serverCount+=1;
        if serverCount > 3:
            serverCount=1;
        StatSplit=string.split(statsrec,':')
        StatsId=StatSplit[1];
        StatsStructureId=StatSplit[0];
        StatsName=StatSplit[2];
        p=Process(target=ProcessFiles,args=(StatsStructureId,StatsId,StatsName,ServerName,));
        p.start();

def updateLoadStatus(FileId,status,logfilename):
    try:
        sql='UPDATE DATASTORE.STATSOUTPUT SET STATS_FILE_LOAD_STATUS='+str(status)+', STATS_FILE_CREATE_DATE=now() WHERE STATS_OUTPUTFILE_ID='+str(FileId);
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        vdat=vconn.vertica_sql_execute(conn,sql);
        conn.commit();
    
        sql='UPDATE DATASTORE.VERTICA_COPY SET STATUS='+str(status)+', EXECUTE_DATE=now() WHERE STATS_OUTPUTFILE_ID='+str(FileId);
        vdat=vconn.vertica_sql_execute(conn,sql);
        conn.commit();
    
        vdat.close();
        conn.close();
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);

def GetLoads():
    maxLoads=10000;
    sql='SELECT STATS_OUTPUTFILE_ID,COPYSTMT FROM DATASTORE.VERTICA_COPY WHERE NVL(STATUS,0)=0 LIMIT '+str(maxLoads);
    conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
    vdat=vconn.vertica_sql_execute(conn,sql);
    
    datresult=vdat.fetchall();
    loaddat=[];
    
    for drec in datresult:
        loadrec=[];
        loadrec.append(drec[0]);
        loadrec.append(drec[1]);
        loaddat.append(loadrec);
    return loaddat;

def optimusLoad(LoadingFileId,copyStmt,logfilename,servername):
    getStatus=commands.getoutput('/opt/vertica/bin/vsql -c "'+copyStmt+'" -U dbadmin -w c@llhome -d callhomedb -h '+servername);
    if string.find(string.upper(getStatus),'ERROR')<0:
        updateLoadStatus(LoadingFileId,1,logfilename);
    else:    
        updateLoadStatus(LoadingFileId,-1,logfilename);
      

def optimusLoader():
    logfilename='Loader_'+time.strftime('%Y%m%d%H%M%S');
    try:
        allLoads=GetLoads();
        serverctr=1;
        for loads in allLoads:
            LoadingFileId=loads[0];
            copyStmt=loads[1];
            servername='callhomelab-vertica0'+str(serverctr);
            serverctr+=1;
            if serverctr > 3:
                serverctr=1;
            p=Process(target=optimusLoad,args=(LoadingFileId,copyStmt,logfilename,servername,));
            p.start();
            processName='optimusLoader';
            numprocess=countProcess(processName);
            while numprocess > 100:
                time.sleep(15);
                numprocess=countProcess(processName);
    except:
        logger("Error reported: "+str(sys.exc_info()[1]),logfilename);
    
    
    
    
    
