#!/usr/bin/env python

import oracleconnect as oracon;
import os;
import sys;
import datetime;
import time;
import string;
import commands;
from multiprocessing import Process;

constr='monitor/m0nitor@10.0.40.40:1521/monitordb';
maxrowctr=30000;

def subdirlist(dirname,dirn,filetype):
    try:
   
        subdirList=[];
        try:
            subdirList=os.listdir(dirname+"/"+dirn);
        except:    
            pass;
        oraconn=oracon.openconnect(constr); 
        for subdirn in subdirList:
            if string.find(subdirn,'.'+string.strip(filetype)) > 0:
                sqlstmt="begin MONITORPROC.ADDFILE (P_FILEPATH => \'"+dirname+'/'+dirn+"\',P_FILENAME => \'"+subdirn+"\'); end; ";
                oracon.execSql(oraconn,sqlstmt);
        oraconn.close();
    except:
        log = open("log/Aggregator.log","a");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();                        

def waitTime():
    pythonRuns=commands.getoutput("ps -ef | grep monitorAggregator | grep -v grep | wc -l");
    if int(pythonRuns) > 100:
        time.sleep(60);
        pythonRuns=commands.getoutput("ps -ef | grep monitorAggregator | grep -v grep | wc -l");
        while int(pythonRuns) > 100:
            time.sleep(60);
            pythonRuns=commands.getoutput("ps -ef | grep monitorAggregator | grep -v grep | wc -l");
    return;

def dirListingT2(dirName,filetype):
    try:
        fileList=[];
        subdirList=[];
        dirList=[];
        dirSort=[]
        dirname =dirName;
        try:
            dirList=os.listdir(dirname);
        except:    
            pass;      
        
        dirsort=sorted(dirList,reverse=True);
        
        for dirn in dirsort:
            #waitTime();
            #p=Process(target=subdirlist,args=(dirname,dirn,));
            #p.daemon=True;
            #p.start();
            subdirlist(dirname,dirn,filetype);
    except:
        log = open("log/Aggregator.log","w");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();
    

def ReadFileType():
    try:
        constr='monitor/m0nitor@10.0.40.40:1521/monitordb';
        oraconn=oracon.openconnect(constr);
        sqlstmt="select trim(foldername) from monitorsourcefolder";
        eventcur=oracon.execSql(oraconn,sqlstmt);
        sqlstmt="select trim(filetypename) from monitorsourcetype";
        typecur=oracon.execSql(oraconn,sqlstmt);
        for srcdir in eventcur:
            for typ in typecur:
                dirListingT2(srcdir[0],typ[0]);
        eventcur.close();
        typecur.close();
        oraconn.close() ;       
    except:
        log = open("log/Aggregator.log","a");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();

def loadfile(fileid,filepath,filename,thread):
    constr='monitor/m0nitor@10.0.40.40:1521/monitordb';
    oraconn=oracon.openconnect(constr);
    fl=open(filepath+'/'+filename);
    allStatements=fl.readlines();
    ctr=1;
    for st in allStatements:
	try:
	    if len(st)>4000:
		if string.find(st,'creating deployment')>=0:
		    st=st[:string.find(st,'dataSourceName')]
		    st=st+' Failed to deploy package\');';
	    st=string.replace(st,') ',',fileid) ')
	    st=string.replace(st,');',','+str(fileid)+');');
	        
	    oracon.execSql(oraconn,'begin '+st+' commit; end;');
	    ctr=ctr+1;
	    
	except:
	    log = open("log/loader_"+str(thread)+".log","a");
	    log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
	    log.close();
    fl.close();
    #print 'Done file '+filename+' added '+str(ctr)+' rows...';
    oracon.execSql(oraconn,'Begin update monitordatasource set loaded=1 where filepath=\''+filepath+'\' and filename=\''+filename+'\'; commit; end;'); 
    oraconn.close();
def loadprocess(thread):
	constr='monitor/m0nitor@10.0.40.40:1521/monitordb';
        oraconn=oracon.openconnect(constr);
        sqlstmt="select fileid,filepath,filename from monitordatasource where loaded=2 and threadnumber="+str(thread);
	eventcur=oracon.execSql(oraconn,sqlstmt);
	for evt in eventcur:
	    loadfile(evt[0],evt[1],evt[2],thread);
	oraconn.close();
def LoadData():
    try:
	threadcount=20;
	constr='monitor/m0nitor@10.0.40.40:1521/monitordb';
        oraconn=oracon.openconnect(constr);
	
	sqlstmt='select count(1) from monitordatasource where loaded=0';
	reccount=oracon.execSql(oraconn,sqlstmt);
	for rec in reccount:
	    reccountcurrent=rec[0];
	if (reccountcurrent > threadcount):
	    recproc=round(reccountcurrent/threadcount,0);
	else:
	    recproc=1;
	    
	print 'Number of records:'+str(reccountcurrent);
	ctr=1;
	while ctr <= threadcount:
	    sqlstmt="begin update monitordatasource set loaded=2,threadnumber="+str(ctr) +" where loaded=0 and rownum <="+str(recproc)+"; commit; end;"
	    oracon.execSql(oraconn,sqlstmt);
	    p=Process(target=loadprocess,args=(ctr,))
	    p.start();
	    time.sleep(60);
	    ctr=ctr+1;
	
        oraconn.close();
    except:
        log = open("log/loader.log","w");
	log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();
        
def main():
    while (1):
	ReadFileType();
	LoadData();
	time.sleep(3600);

if __name__ == '__main__':
	main();