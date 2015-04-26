#!/usr/bin/env python
import statsprocessor as st;
from multiprocessing import Process;
import time;
import sys;

#stlib.splitter();
try:
    while (1):
        p=Process(target=st.procExec());
        p.start();
        time.sleep(30);
    #stlib.tabFile(5469763,'/mysqlnew/dataprocessed/1000645','showpd.1000645.121121.144506.0001','showpd',1000645,'121121 144506','True','/test/dataprocessed/');
except:
    print "Error reported: "+str(sys.exc_info()[1]);

