#!/usr/bin/env python
import statsfilelib as stlib;
from multiprocessing import Process;
import time;
import sys;
#stlib.splitter();
try:
    #stlib.splitFile(2311677,'config.121215.114328.0001','/share/st11/prod/data/files/3PAR.INSERV/1203450/config','config',1203450,'/data/dataprocessed/splitter',1);
    while (1):
        pythonRuns=commands.getoutput("ps -ef | grep parser | grep -v grep | wc -l");
        if int(pythonRuns) > 1:
            time.sleep(900);
        else:
            p=Process(target=stlib.splitrunner());
            p.start();
        
except:
    print "Error reported: "+str(sys.exc_info()[1]);