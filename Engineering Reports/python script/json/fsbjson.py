#!/usr/bin/env python

import time;
import oracleconnect as oracon;
import os;
import string;
import sys;
def redata(dbresulSet):
    retArr=[];
    
    for rec in dbresulSet:
        datArray=[];
        for datrec in rec:
            datArray.append(datrec);
        retArr.append(datArray);
    
    return retArr;

def setconnection(dbhost,dbtype,dbsource,dbuser,sqlstmt):
    try:
                
        #userList=[["produser","pr0duser"],["dbadmin","c@llhome"],["datapulluser","cAllhome"]];
        #retArr=[];
        #usr=[];
        #for usr in userList:
        #    if usr[0]==dbuser:
        #        passwd=usr[1];
        
        
       # if dbtype =='oracle':
        #    print "dbuser"+dbuser;
        #    print "sql in set\t"+sqlstmt;
            
        #    connstr=dbuser+'/'+passwd+'@'+dbhost+'/'+dbsource+':1521'
        #    oraconn = oracon.openconnect(connstr);
        connstr='produser/pr0duser@callhomedw/callhomedw:1521'
        oraconn = oracon.openconnect(connstr);
        dbresulSet = oracon.execSql(oraconn,sqlstmt);
           
        
        retArr=redata(dbresulSet);
        dbresulSet.close();
        if dbtype =='oracle':
            oraconn.close();
         
        return retArr;
    except:
        errlog=open('log/json_error.log','a');
        function='set connection';
        #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
    
    
    
    
def read_data(sourcedb,dbuser,dbtype,hostname,query_string,report_title,output_file,file_location,axis,title,column,xtitle,xcolumn,ytitle,ycolumn):
    try:
        resultset=setconnection(hostname,dbtype,sourcedb,dbuser,query_string);
        fl=open(file_location+output_file,'w');
        optimuslogger=open('log/optimus_json.log','a');
        datarray=[];
        datstr='{\n';
        datstr+='\n"title":[{"caption":"'+report_title+'"}],';
        datstr+='\n"xaxis":[{"name":"'+xcolumn+'","label":"'+xtitle+'"}],'
        datstr+='\n"yaxis":[{"name":"'+ycolumn+'","label":"'+ytitle+'"}],'
        datstr+='\n"table":[{';
        
        
        i=1; #this is counter for column so that when the columns comes to end , it can ',' comma in the end

        for col in column:
            
            if(i<len(column)):    
                datstr+='\n"column'+str(i)+'":"'+col+'",'
                
            if(i==len(column)):
                datstr+='\n"column'+str(i)+'":"'+col+'"'
            i=i+1;    
 
        datstr+='}],';
        datstr+='\n"data":[';
 
        j=1; ##this is counter for record so that when the columns comes to end , it can ',' comma in the end
        for rec in resultset:
           
            if(j<len(resultset)):
                datstr+='\n{"'+column[0]+'":"'+str(rec[2])+'\",\"'+column[1]+'":"'+str(rec[3])+'\"'+'},';
             
            if(j==len(resultset)):   
               datstr+='\n{"'+column[0]+'":"'+str(rec[2])+'\",\"'+column[1]+'":"'+str(rec[3])+'\"'+'}';
            
            j=j+1;   
                
    
        datstr+=']';
        datstr+=string.join(datarray,',\n')+'\n';               
        datstr+='\n}';
        fl.write(datstr);
        fl.close();
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        optimuslogger.write(timestr+'\t'+output_file+'JSON has been generated'+'\n');
        optimuslogger.close;
    except:
        errlog=open('log/json_error.log','a');
        errlog.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        function='read data';
        
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
    
def get_data():
    try:
        connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521';
        oraconn = oracon.openconnect(connstr);
        optimuslogger=open('log/optimus_json.log','a');
        optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        sqlstmt='SELECT QUERY_ID,DB_SOURCE,DB_USER,DB_TYPE,HOSTNAME,QUERY_STRING,REPORT_TITLE,OUTPUT_FILE_NAME,FILE_LOCATION FROM GRAPH_REPORT_DATA where enabled=1 and JSON_TEMPLATE=\'fsbjson.py\' and AGG_UPDATE> nvl(OUTPUT_FILE_UPDATE,trunc(sysdate)) order by QUERY_ID';
        reportResultset = oracon.execSql(oraconn,sqlstmt);
        
        for rec in reportResultset:
            query_id=rec[0];
            sourcedb=rec[1];
            dbuser=rec[2];
            dbtype=rec[3];
            hostname=rec[4];
            query_string=rec[5];
            report_title=rec[6];
            output_file=rec[7];
            file_location=rec[8];
            
            
            sqlstmt='SELECT AXIS,TITLE,COLUMN_NAME FROM GRAPH_DATA where QUERY_ID=\''+str(query_id)+'\'';
            queryResultset = oracon.execSql(oraconn,sqlstmt);
            
            axis=[];
            title=[];
            column=[];
            for res in queryResultset:
                axis.append(res[0]);
                title.append(res[1]);
                column.append(res[2]);
            queryResultset.close();
            
            sqlstmt='SELECT TITLE,COLUMN_NAME FROM GRAPH_DATA where AXIS=\'X\' and QUERY_ID=\''+str(query_id)+'\'';
            graphResultsetX = oracon.execSql(oraconn,sqlstmt);
            
            for res in graphResultsetX:
                xtitle=res[0];
                xcolumn=res[1];
            
            graphResultsetX.close();
            
            sqlstmt='SELECT TITLE,COLUMN_NAME FROM GRAPH_DATA where AXIS=\'Y\' and QUERY_ID=\''+str(query_id)+'\'';
            graphResultsetY = oracon.execSql(oraconn,sqlstmt);
            
            for res in graphResultsetY:
                ytitle=res[0];
                ycolumn=res[1];
            
            graphResultsetY.close()

            
            print report_title;
       
            
            read_data(sourcedb,dbuser,dbtype,hostname,query_string,report_title,output_file,file_location,axis,title,column,xtitle,xcolumn,ytitle,ycolumn);
            
        reportResultset.close();
        oraconn.close();
    except:
        
        errlog=open('log/json_error.log','a');
        function='get data';
        #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();    

get_data();        
