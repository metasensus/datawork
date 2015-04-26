#!/usr/bin/env python
import statsprocessor as st;
from multiprocessing import Process;
import time;
import sys;
import commands;
import mysqlconnect as mysqlconn;

#stlib.splitter();
try:
    while (1):
        sqlstmt='select count(1) from STATS_SPLIT_FILES where ifnull(FILE_PROCESS_STATUS,0)=0';
        lconn = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
        lcurr=lconn.cursor();
        lcurr.execute(sqlstmt);
        recc=lcurr.fetchall();
        
        tobeprocessed=0;
        pythonRuns=commands.getoutput("ps -ef | grep parsetabber | grep -v grep | wc -l");
        
        for rec in recc:
            tobeprocessed=rec[0];
        
        if tobeprocessed > 0 and  int(pythonRuns) <= 1:
            p=Process(target=st.procExec());
            p.start();
        if tobeprocessed == 0 and  int(pythonRuns) == 1:
            p=Process(target=st.procExec());
            p.start();
        time.sleep(900);
    #stlib.tabFile(5469763,'/mysqlnew/dataprocessed/1000645','showpd.1000645.121121.144506.0001','showpd',1000645,'121121 144506','True','/test/dataprocessed/');
except:
    print "Error reported: "+str(sys.exc_info()[1]);

