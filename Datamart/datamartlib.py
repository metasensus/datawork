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

os.environ["NLS_LANG"] = ".UTF8"



#This module will return number of threads associtaed for the machine
def get_process():
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);
    hstip=hst.retHostIP();
    sqlstmt= 'select NUMBEROFTHREADS from STATSPROCESSINGMACHINE where IPADDRESS=\''+hstip+'\'';
    result=oracon.execSql(oraconn,sqlstmt);
    p_count='';
    for count in result:
        p_count=count[0];
    oraconn.close();
    return p_count;

#This module will check the purpose of the machie i.e. datamart_extract,sync
def check_purpose():
    global machine_id;
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);
    hstip=hst.retHostIP();
    sqlstmt= 'select purpose,machineid from STATSPROCESSINGMACHINE where IPADDRESS=\''+hstip+'\'';
    purrec=oracon.execSql(oraconn,sqlstmt);
    purpose='';
    for prec in purrec:
        purpose=prec[0];
        machine_id=prec[1];
    oraconn.close();
    return purpose;

#This is sub module of setconnection that returns list from the resultset    
def redata(dbresulSet):
    retArr=[];
    
    for rec in dbresulSet:
        datArray=[];
        for datrec in rec:
            datArray.append(datrec);
        retArr.append(datArray);
    return retArr;

#This module is setting up the connection to specific database
def setconnection(dbhost,dbtype,dbsource,dbuser,sqlstmt):
    try:
                
        userList=[["produser","pr0duser"],["datamart","datamart"],["dbadmin","c@llhome"],["datapulluser","cAllhome"]];
        retArr=[];
        usr=[];
        for usr in userList:
            if usr[0]==dbuser:
                passwd=usr[1];
        
        
        if dbtype =='oracle':
           
            connstr=dbuser+'/'+passwd+'@'+dbhost+'/'+dbsource+':1521'
            oraconn = oracon.openconnect(connstr);
            dbresulSet = oracon.execSql(oraconn,sqlstmt);
           
        if dbtype =='mysql':
           
            myconn=mysql.connectMysql(dbsource,dbuser,passwd,dbhost);
            cur_mysql = myconn.cursor();
            cur_mysql.execute(sqlstmt);
            dbresulSet = cur_mysql.fetchall();
            
            
        if dbtype =='vertica':
            conn=vconn.vertica_connect(dbhost,5433,dbuser,passwd,dbsource);
            vdat=vconn.vertica_sql_execute(conn,sqlstmt);
            
            dbresulSet=vdat.fetchall();
            
        
            
        
        retArr=redata(dbresulSet);
        
        if dbtype =='oracle':
            dbresulSet.close();
            oraconn.close();
        if dbtype =='mysql':
            cur_mysql.close();
        if dbtype =='vertica':
            vdat.close();    
        return retArr;
    
    except:
        errlog=open('log/datamart_error.log','a');
        function='set connection';
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.write(timestr+'\t SQL STMT: '+sqlstmt+ '\n');
        errlog.close();   
   
    
    

#This module is resposible to create the table in mysql database based on oracle table decription
def create_table(tablename,part_column,sourcedb,dbuser,dbtype,hostname):
    try:
     
        dbname='ods';
        username='datamart';
        passwd='datamart';
        host='localhost';
        myconn=mysql.connectMysql(dbname,username,passwd,host);
        
        tableinsert=[];
        datamartlogger=open('log/datamart_extract.log','a');
        cur_mysql = myconn.cursor();
        columnList=[];
        
        sqlstmt='DROP TABLE IF EXISTS '+tablename;
        cur_mysql.execute(sqlstmt);
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        datamartlogger.write(timestr+'\t'+sqlstmt+'\n');
        insert_stmt='insert into '+tablename+'('
        createstmt='create table '+tablename+'(';
        selectstmt='select ';
        collist=[];
        colact=[];
        insertact=[];
    
        sqlstmt='select count(1) from '+tablename;
        OracleCount = setconnection(hostname,dbtype,sourcedb,dbuser,sqlstmt);
        
        for orct in OracleCount:
            reccount=orct[0];
        datamartlogger.write(timestr+'\t'+tablename+' has '+str(reccount)+' rows\n');
        
        sqlstmt='select COLUMN_NAME, lower(DATA_TYPE),DATA_LENGTH FROM USER_TAB_COLUMNS WHERE TABLE_NAME = \''+tablename+'\' ORDER BY COLUMN_ID';
       
        OracleColResultset =setconnection(hostname,dbtype,sourcedb,dbuser,sqlstmt);

#Creating table in MySQL based on different column datatype        
        for col in OracleColResultset:
            collist.append(col[0]);
            if col[1]=='varchar2':  #converting varchar2 to varchar
                datatype='varchar('+str(col[2])+')';
                colact.append('chr(39)||replace('+col[0]+',\'|\',\' \')||chr(39)');
                insertact.append('none');
            if col[1]=='number':    #converting number to decimal(20,2)
                datatype='decimal(20,2)';
                colact.append('to_char(nvl('+col[0]+',0))');
                insertact.append('none');
            if col[1]=='date':    #converting date to datetime
                datatype='datetime';
                colact.append('chr(39)||to_char('+col[0]+',\'YYYYMMDDHH24MISS\')||chr(39)');
                insertact.append('str_to_date(,\'%Y%m%D%H%i%S\'');
            if col[1]=='char':   #converting char to varchar
                datatype='varchar('+str(col[2])+')';
                colact.append('chr(39)||'+col[0]+'||chr(39)');
                insertact.append('none');    
            columnList.append(col[0]+' '+datatype);
        selectstmt+=string.join(colact,'||\'|\'||')+' from '+tablename
        partstmt=' ';
        if len(string.strip(part_column))>0 :   
            selectstmt+=' where '+part_column+' =';
            partstmt='select distinct '+part_column+' from '+tablename+' order by '+part_column;
            
        insert_stmt+=string.join(collist,',')+') values ';
        tableinsert.append([tablename,selectstmt,insert_stmt,string.join(insertact,';'),partstmt,reccount]);
        sqlstmt=createstmt+string.join(columnList,',')+')';
        cur_mysql.execute(sqlstmt);
        cur_mysql.close();
        datamartlogger.close();
        return tableinsert; #Tableinsert is the combined list of tablename,selectstmt,insertstmt
    except:
        errlog=open('log/datamart_error.log','a');
        function='create_table';
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported for the table: '+tablename+ '\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        #errlog.write(timestr+'\t'+col[0]+'\n');
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        
        errlog.close();
        
# This module is reposible to insert the data in MySQL
def building_insert(sqlstmt,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,insertfile,multi):

    try:
        if multi==1:
            insertfl=open(insertfile,'w');
            stmt='';
            OracleResultset=setconnection(hostname,dbtype,sourcedb,dbuser,sqlstmt);
           
            stmt='';
            allInserts=[];
            for rec in OracleResultset:
                insertstmt=dat[2];
                insertList=[];
                readdata=rec[0];
                data=string.split(readdata,'|');
                
                ctr=0;
               
                for dt in data:
                    if actions[ctr]=='none':
                        if string.find(dt,'\'')>= 0:
                           dt=string.replace(dt,'\'','');
                           dt='\''+dt+'\'';
                        insertList.append(dt);
                    elif string.find(actions[ctr],'str_to_date')>=0:
                        actionlist=string.split(actions[ctr],',');
                        insertList.append(actionlist[0]+dt+','+actionlist[1]+')');
                    ctr=ctr+1;
                    if ctr == len(data):
                        break;
                #insertstmt+=string.join(insertList,',')+')';
                allInserts.append('('+string.join(insertList,',')+')');
                if len(allInserts)==100:
                    insertfl.write( insertstmt+' '+string.join(allInserts,',')+';\n');
                    insertfl.flush();
                    allInserts=[];
            if len(allInserts)>0: 
                insertfl.write( insertstmt+' '+string.join(allInserts,',')+';\n');        
                insertfl.close();
                allInserts=[];
            os.system('mv '+insertfile+' '+string.replace(insertfile,'.prt','.sql'));
            return 1;
        else:
            stmt='';
            OracleResultset=setconnection(hostname,dbtype,sourcedb,dbuser,sqlstmt);
           
            stmt='';
            allInserts=[];
            for rec in OracleResultset:
                insertstmt=dat[2]+'(';
                insertList=[];
                readdata=rec[0];
                data=string.split(readdata,'|');
                
                ctr=0;
               
                for dt in data:
                    if actions[ctr]=='none':
                        if string.find(dt,'\'')>= 0:
                           dt=string.replace(dt,'\'','');
                           dt='\''+dt+'\'';
                        insertList.append(dt);
                    elif string.find(actions[ctr],'str_to_date')>=0:
                        actionlist=string.split(actions[ctr],',');
                        insertList.append(actionlist[0]+dt+','+actionlist[1]+')');
                    ctr=ctr+1;
                    if ctr == len(data):
                        break;
                insertstmt+=string.join(insertList,',')+')';
                allInserts.append(insertstmt);
             
            return allInserts;
    except:
        errlog=open('log/datamart_error.log','a');
        function='building_insert';
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t'+stmt+'\n');
        errlog.write(timestr+'\t Error reported: '+tablename+': '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
        return 0;
    
def multiread(orgsqlstmt,partdata,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,loopcount):
    try:
        status=[];
        insertfile='/odsarchstg/tmp/allinserts_'+str(loopcount)+'.prt';
        
        
        sqlstmt=string.replace(orgsqlstmt,'=',' between ')+partdata;
        building_insert(sqlstmt,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,insertfile,1); #Passing the sqlstmt along with argument to bulding_insert
        return 1;
    except:
        errlog=open('log/datamart_error.log','a');
        function='multiread';
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t'+str(partdata)+'\n');
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
    
#This module is creating insert statement from the table insert list that we derived from the create_table
def insert_table(tableinsert,partcolumn,tablename,sourcedb,dbuser,dbtype,hostname):
    try:
        datamartlogger=open('log/datamart_extract.log','a');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        datamartlogger.write(timestr+'\tStarting insert process in\t'+tablename+'\n');
        #datalog=open('log/datamart_multi.log','a');
        ctr=1
        #datalog.write(timestr+'\t time started \t\n');
        for dat in tableinsert:
            orgsqlstmt=dat[1];
            actions=string.split(dat[3],';');
            if len(string.strip(dat[4]))>0:
                #part_status=[]
                os.system('rm -rf /odsarchstg/tmp/*.sql')
                distOraRes = setconnection(hostname,dbtype,sourcedb,dbuser,dat[4]);
                status=[];
                partlist=[];
                loopcount=1;
                numpart=len(distOraRes);
                filecount=0
                for distRec in distOraRes:
                    partdata=distRec[0];
                    if ctr==1:
                        minpar=partdata
                    partlist.append(str(partdata));
                    if ctr>250:
                        maxpar=partdata
                        partdata= str(minpar)+' and '+str(maxpar);
                        p= multiprocessing.Process(target=multiread, args=(orgsqlstmt,partdata,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,loopcount,))
                        p.start();
                        numrec=int(commands.getoutput('ps -ef | grep python | wc -l'));
                        if numrec > 10:
                            time.sleep(120);
                            numrec=int(commands.getoutput('ps -ef | grep python | wc -l'));
                        loopcount=loopcount+1;
                        ctr=0;
                        partlist=[];
                        filecount+=1;
                    ctr=ctr+1;
                if len(partlist)>0:    
                    partdata=string.join(partlist,',');
                    p= multiprocessing.Process(target=multiread, args=(orgsqlstmt,partdata,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,loopcount,));
                records=numpart;
                writtenfilecount=int(commands.getoutput('ls -l /odsarchstg/tmp/*.sql | wc -l'));
                while writtenfilecount < filecount:
                    time.sleep(120);
                    writtenfilecount=int(commands.getoutput('ls -l /odsarchstg/tmp/*.sql | wc -l'));
                os.system('rm -rf /odsarchstg/tmp/final/allinserts_multi.sql');
                os.system('cat /odsarchstg/tmp/*.sql > /odsarchstg/tmp/final/allinserts_multi.sql');
                os.system('mysql -u datamart -p\'datamart\' ods < /odsarchstg/tmp/final/allinserts_multi.sql');
                os.system('rm -rf /odsarchstg/tmp/*.sql');
                os.system('rm -rf /odsarchstg/tmp/final/allinserts_multi.sql');
            else:
                insertfl=open('allinserts.sql','w');
                status=building_insert(orgsqlstmt,tablename,partcolumn,dat,actions,sourcedb,dbuser,dbtype,hostname,'',0);  #Passing the sqlstmt along with argument to bulding_insert
                insertfl.write(string.join(status,';\n')+';\n')
                insertfl.flush();
                
                insertfl.close();
                insertfile='allinserts.sql';
                os.system('mysql -u datamart -p\'datamart\' ods <'+insertfile);
        
        function='insert_table';
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        datamartlogger.write(timestr+'\tCompleted insert process\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        #datalog.write(timestr+'\t time ended \t\n');
        #datalog.close();
        return 1;    
    except:
            errlog=open('log/datamart_error.log','a');
            function='insert_table';
            timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
            errlog.write(timestr+'\t Error reported for the table: '+tablename+ '\n');
            timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
            #errlog.write(timestr+'\t'+string.join(tableinsert,',')+'\n');
            errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
            errlog.close();
    return 0;

#This is the primary module which is reponsible to run on datamart_extract machine to run the entire datamart process
def create_insert_table():
        syncprocess=commands.getoutput('ps -ef | grep [d]atamart | wc -l');
        if int(syncprocess) > 1 :
            return;
        import vertica_connect as vconn;
        connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521'
        oraconn = oracon.openconnect(connstr);
        insertfl=open('allinserts.sql','w');
        insertfl.close();
        
  
        sqlstmt='SELECT A.TABLE_NAME,nvl(A.PARTITION_COLUMN,\' \'),A.SOURCEDB,A.SOURCEDBUSER,A.SOURCETYPE,A.HOSTNAME, B.machineid FROM DATAMART_TABLE_EXTRACT A LEFT JOIN (select table_id,machineid from DATAMART_SOURCE) B on A.table_id= B.Table_id  where a.enabled=1 and a.agg_status=1 and a.agg_update_date> nvl(a.extracted,trunc(a.agg_update_date)) and b.machineid=\''+str(machine_id)+'\' order by a.TABLE_ID';
        OracleResultset = oracon.execSql(oraconn,sqlstmt);
        sqlstmt='';
        datamartlogger=open('log/datamart_extract.log','a');
        
    
        datamartlogger.write('\n---------------------------------------------------------------------------------------------------------------------------------------------\n')
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        datamartlogger.write(timestr+'\t'+'Starting new datamart extract process'+'\n');
    
        for rec in OracleResultset:
            tablename=rec[0];
            partcolumn=rec[1];
            sourcedb=rec[2];
            dbuser=rec[3];
            dbtype=rec[4];
            hostname=rec[5];
            
            
            try:
                tableinsert=create_table(tablename,partcolumn,sourcedb,dbuser,dbtype,hostname); #passing the argument to create_table , which is reponsilbe to create the table in MySQL database,
                insert_status=insert_table(tableinsert,partcolumn,tablename,sourcedb,dbuser,dbtype,hostname); #Return statement from the create table are being passed to insert_table along with different argument to ensure the insert process
                #result=master()
                if insert_status == 1:
                    sqlstmt='begin update DATAMART_TABLE_EXTRACT set extracted=sysdate where table_name=\''+tablename+'\'; commit; end;';
                    oracon.execSql(oraconn,sqlstmt);
                    sqlstmt='';
                else:
                    errlog=open('log/datamart_error.log','a');
                    function='create_insert_table';
                    timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
                    errlog.write(timestr+'\t'+sqlstmt+'\n');
                    errlog.write(timestr+'\t Error in table\t'+tablename+'\n');
                    errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
                    errlog.close();
                    #print
            except:
                errlog=open('log/datamart_error.log','a');
                function='create_insert_table';
                timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
                errlog.write(timestr+'\t'+sqlstmt+'\n');
                errlog.write(timestr+'\t Error in table\t'+tablename+'\n');
                errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
                errlog.close();
        OracleResultset.close();
        oraconn.close();
        datamartlogger.close()

def errorTrap(log):
    timestr=time.strftime('%m/%d/%Y %H:%M:%S');
    errlog.write(timestr+'\t'+sqlstmt); 
    errlog.write(timestr+'\t Error reported: '+log+ '\n');
    errlog.close();
    return;    
    
def datamart_sync():
    syncprocess=commands.getoutput('ps -ef | grep [d]atamart | wc -l');
    if int(syncprocess) > 1 :
        return;
    connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521'
    oraconn = oracon.openconnect(connstr);
    
    sqlstmt='SELECT A.TABLE_NAME,C.NAME,C.DATAPROCESSING_FOLDER,A.TABLE_ID FROM DATAMART_TABLE_EXTRACT A, DATAMART_TARGET B,STATSPROCESSINGMACHINE C WHERE A.ENABLED = 1 AND A.AGG_STATUS = 1 AND A.EXTRACTED > NVL (A.REPLICATION_DATE, TRUNC (A.EXTRACTED)) AND A.TABLE_ID = B.TABLE_ID AND B.MACHINEID=C.MACHINEID ORDER BY A.TABLE_ID';
    
    
    OracleResultset = oracon.execSql(oraconn,sqlstmt);
    sqlstmt='';
    datamartlogger=open('log/datamart_sync.log','a');
    
    datamartlogger.write('\n---------------------------------------------------------------------------------------------------------------------------------------------\n')
    timestr=time.strftime('%m/%d/%Y %H:%M:%S');
    datamartlogger.write(timestr+'\t'+'Starting new datamart sync process'+'\n');
    datamartlogger.flush();
        
    for rec in OracleResultset:
        try:
            mysqlorgdump=rec[0]+'.run'
            mysqldump_name=rec[0]+'.out'
            target=rec[1];
            processing_folder=rec[2];
            table_id=rec[3];
            table_name=rec[0];
            
            sqlstmt='SELECT B.IPADDRESS,B.NAME from DATAMART_SOURCE A, STATSPROCESSINGMACHINE B where A.MACHINEID=B.machineid AND A.table_id=\''+str(table_id)+'\'';
            Machineresultset= oracon.execSql(oraconn,sqlstmt);
            
            for name in Machineresultset:
                machine_ip=name[0];
                machine_name=name[1];
                #machine_name='callhome-vm58.3pardata.com'
                #machine_name='callhome-vm58'
                #print machine_ip;
                #print machine_name;
            
            dbhost=machine_name+'.3pardata.com';
            dbtype='mysql';
            dbuser='datamart';
            dbsource='ods';
            sqlstmt='select count(1) from '+table_name;
                      
            
            datrec=setconnection(dbhost,dbtype,dbsource,dbuser,sqlstmt);
            for drec in datrec:
                reccount=drec[0];
            if reccount > 0:    
                dumpfile='mysqldump -h%s -u datamart -p\'datamart\' ods %s > /root/proc/mysqldump/%s' %(machine_ip,rec[0],mysqlorgdump)
                log=commands.getoutput(dumpfile);
                if string.find(log,'Error')>=0:
                    errorTrap(log);
                else:    
                    datamartlogger.write(timestr+'\t'+log+'\n');
                    log=commands.getoutput('mv /root/proc/mysqldump/'+mysqlorgdump+' /root/proc/mysqldump/'+mysqldump_name);
                    if string.find(log,'Error')>=0:
                        errorTrap(log);
                    else:     
                        datamartlogger.write(timestr+'\t'+log+'\n');
                        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
                        file1= rec[0]
                        datamartlogger.write(timestr+'\t'+file1+"needs to transfer"+'\n');
                        extract= "ssh root@%s  mysql -u datamart -p'datamart' datamart_extract < %s%s" %(target,processing_folder,mysqldump_name)
                        log=commands.getoutput(extract);
                        if string.find(log,'Error')>=0:
                            errorTrap(log);
                        else:  
                            datamartlogger.write(timestr+'\t'+log+'\n');
                            timestr=time.strftime('%m/%d/%Y %H:%M:%S');
                            sqlstmt='begin update DATAMART_TABLE_EXTRACT set replication_date=sysdate where table_name=\''+file1+'\'; commit; end;';
                            oracon.execSql(oraconn,sqlstmt);
                            timestr=time.strftime('%m/%d/%Y %H:%M:%S');
                            datamartlogger.write(timestr+'\t'+file1+" replicated to publisher machine from "+machine_name+'\n');
            else:
                timestr=time.strftime('%m/%d/%Y %H:%M:%S');
                errlog.write(timestr+'\t'+sqlstmt); 
                errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
                errlog.close();
        except:
            errlog=open('log/datamart_sync_error.log','a');
            timestr=time.strftime('%m/%d/%Y %H:%M:%S');
            errlog.write(timestr+'\t'+sqlstmt); 
            errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
            errlog.close();
    datamartlogger.close();        
    OracleResultset.close();
    oraconn.close();