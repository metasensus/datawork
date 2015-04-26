#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;
import statslog;
import sys;
import commands;
import time;
from multiprocessing import Process



def checkmakedir(dname,fname):
    try:
        d=os.path.dirname(dname);
        if not os.path.exists(d):
            os.makedirs(d);
    except:
        fl=statslog.logcreate(fname);
        statslog.logwrite(fl,"Error reported (create dir): "+str(sys.exc_info()[1]));
    return;

def inservwrite(inserv):
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    oraconn=oracon.openconnect(constr);
    
    checkmakedir('/report/data/'+string.strip(str(inserv))+'/','drilldown_create');
    fl=open('/report/data/'+string.strip(str(inserv))+'/perfMetricChartData.txt','w');
        
    catarray=[];
    datarray=[];
    datasetarr=[];
    montharr=[];
    sqlstmt="SELECT distinct to_char(datadate,'Mon-DD-YYYY'),to_number(to_char(datadate,'YYYYMMDD')),datadate  FROM diskperf90day WHERE inservserial='"+str(inserv)+"' order by datadate";
    catrec=oracon.execSql(oraconn,sqlstmt);
    montharr=[];
    for ct in catrec:
        catarray.append('"'+ct[0]+'"');
        catstring='['+string.join(catarray,',')+']';	
        montharr.append(ct[1]);
    catrec.close();	
    sqlstmt="SELECT distinct pdid,pdtype  FROM diskperf90day WHERE inservserial='"+str(inserv)+"' order by pdid";
        #print sqlstmt;
    pdrec=oracon.execSql(oraconn,sqlstmt);
    
    pdrecord=[];
    for pddat in pdrec:
	pdrow=[];
	pdrow.append(pddat[0]);
	pdrow.append(pddat[1]);
	pdrecord.append(pdrow);
    pdrec.close();
    	
    for pdr in pdrecord:
        pdid=pdr[0];
        pdtype=pdr[1];
        ioarrayr=[];
        kbsarrayr=[];
        svarrayr=[];
        szkbarrayr=[];
        qlarrayr=[];
	ioarrayw=[];
        kbsarrayw=[];
        svarrayw=[];
        szkbarrayw=[];
        qlarrayw=[];
	ioarrayt=[];
        kbsarrayt=[];
        svarrayt=[];
        szkbarrayt=[];
        qlarrayt=[];
	sqlstmt="SELECT TO_NUMBER(TO_CHAR(DATADATE,'YYYYMMDD')),IOPERSEC,KBPERSEC,SERVICETIMEMS,IOSIZEKB,QLENGTH,IOTYPE FROM diskperf90day WHERE pdid="+str(pdid)+" AND inservserial='"+str(inserv)+"' AND pdtype='"+pdtype+"' order by TO_NUMBER(TO_CHAR(DATADATE,'YYYYMMDD')),iotype";
	alldatrec=oracon.execSql(oraconn,sqlstmt);
	allpdrec=[];
	for allrec in alldatrec:
	    allrow=[];
	    allrow.append(allrec[0]);
	    allrow.append(allrec[1]);
	    allrow.append(allrec[2]);
	    allrow.append(allrec[3]);
	    allrow.append(allrec[4]);
	    allrow.append(allrec[5]);
	    allrow.append(allrec[6]);
	    allpdrec.append(allrow);
	alldatrec.close();
	
	
        for mnth in montharr:
	    for allrec in allpdrec:
		iotype='';
	        if mnth == allrec[0]:
		    io=allrec[1];
		    kbs=allrec[2];
		    sv=allrec[3];
		    szkb=allrec[4];
		    ql=allrec[5];
		    iotype=allrec[6];

		    if iotype == 'r':
		        ioarrayr.append(str(io));
		        kbsarrayr.append(str(kbs));
		        svarrayr.append(str(sv));
		        szkbarrayr.append(str(szkb));
		        qlarrayr.append(str(ql));

		    if iotype == 'w':
		        ioarrayw.append(str(io));
		        kbsarrayw.append(str(kbs));
		        svarrayw.append(str(sv));
		        szkbarrayw.append(str(szkb));
		        qlarrayw.append(str(ql));
		        break;
		    if iotype == 't':
		        ioarrayt.append(str(io));
		        kbsarrayt.append(str(kbs));
		        svarrayt.append(str(sv));
		        szkbarrayt.append(str(szkb));
		        qlarrayt.append(str(ql));
	    if iotype == '':
		ioarrayr.append('');
		kbsarrayr.append('');
		svarrayr.append('');
		szkbarrayr.append('');
		qlarrayr.append('');
		    
		ioarrayw.append('');
		kbsarrayw.append('');
		svarrayw.append('');
		szkbarrayw.append('');
		qlarrayw.append('');
		    
		ioarrayt.append('');
		kbsarrayt.append('');
		svarrayt.append('');
		szkbarrayt.append('');
		qlarrayt.append('');
		    
	datasetarr.append('['+str(pdid)+',"'+pdtype+'",\n['+string.join(ioarrayr,',')+'],\n['+string.join(ioarrayw,',')+'],\n['+string.join(ioarrayt,',')+'],\n['+string.join(kbsarrayr,',')+'],\n['+string.join(kbsarrayw,',')+'],\n['+string.join(kbsarrayt,',')+'],\n['+string.join(svarrayr,',')+'],\n['+string.join(svarrayw,',')+'],\n['+string.join(svarrayt,',')+'],\n['+string.join(szkbarrayr,',')+'],\n['+string.join(szkbarrayw,',')+'],\n['+string.join(szkbarrayt,',')+'],\n['+string.join(qlarrayr,',')+'],\n['+string.join(qlarrayw,',')+'],\n['+string.join(qlarrayt,',')+'],\n'+catstring+']');
	
    fl.write('{"aaData": ['+string.join(datasetarr,',')+']\n}');
    fl.close();
    
    oraconn.close();          
    return;

def checkjobs():
    numrecs=commands.getoutput('ps -ef | grep perfreport | wc -l');
    return numrecs;

def runJob(inservlist):
    ctr=1;
    inservs=1;
    for inrec in inservlist:
	p=Process(target=inservwrite,args=(inrec,));
        p.start();
	print "Number of systems done:"+str(inservs);
	print "Number of jobs:"+checkjobs();
	ctr+=1;
	inservs+=1;
	if ctr > 1000:
	    time.sleep(60);
	    ctr=1;
	while int(checkjobs()) > 100:
	    time.sleep(60);
    return;

def perfdrilldown():
    logfile=open('/root/proc/log/perfrun.log','w');
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    oraconn=oracon.openconnect(constr);
    sqlstmt='select count(distinct inservserial) from capacity_web_report';
    numrec=oracon.execSql(oraconn,sqlstmt);
    inservlist=[];
    ctr=0;
    for nrec in numrec:
        numInserv=nrec[0];
    numrec.close();
    print "########################Total Inservs : "+str(numInserv); 
    countInserv=1;
    sqlstmt='select distinct inservserial from capacity_web_report order by inservserial desc';
    insrec=oracon.execSql(oraconn,sqlstmt);

    for inrec in insrec:
	inservlist.append(inrec[0]);
    runJob(inservlist);	
    insrec.close();
    oraconn.close();
    return;
    
def main():
	perfdrilldown();

if __name__ == '__main__':
	main();
        
