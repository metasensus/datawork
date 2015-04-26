#!/usr/bin/env python
import drilldown as drill;
import string;
import oracleconnect as oracon;
import os;
import time;
import statslog;
import sys;
import commands;
from multiprocessing import Process;

def checkjobs():
    numrecs=commands.getoutput('ps -ef | grep heatmap | wc -l');
    return numrecs;

def runbysys(inserv):
    datadir='/report/data/';
    drill.checkmakedir(datadir+string.strip(inserv)+'/','drilldown_create');
    drill.heatmapfix(inserv,datadir);    
    return;


def startjob():
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    oraconn=oracon.openconnect(constr);
    sqlstmt='select count(distinct inservserial) from capacity_web_report';
    numrec=oracon.execSql(oraconn,sqlstmt);
    
    ct=0;
    for nrec in numrec:
        numInserv=nrec[0];
    print "########################Total Inservs : "+str(numInserv); 
    countInserv=1;
    sqlstmt='select distinct inservserial from capacity_web_report where inservserial like \'164%\' order by inservserial';
    insrec=oracon.execSql(oraconn,sqlstmt);
    
    inservrec=[];
    
    for inrec in insrec:
	inservrec.append(inrec[0]);
    insrec.close();
    oraconn.close();
    inservs=1;
    ctr=1;
    for inrec in inservrec:
	p=Process(target=runbysys,args=(inrec,));
        p.start();
	print "Number of systems done:"+str(inservs);
	print "Number of jobs:"+checkjobs();
	ctr+=1;
	inservs+=1;
	if ctr > 1000:
	    time.sleep(15);
	    ctr=1;
	    print "Last inserv :"+str(inrec);
	while int(checkjobs()) > 5:
	    time.sleep(15);

def main():
	startjob();

if __name__ == '__main__':
	main();
