#!/usr/bin/env python

import hostlib as hst;
import oracleconnect as oracon
import time;
import os;
from subprocess import *;
import commands;
import string;

p = commands.getoutput("ps -ef | grep machinestatus | grep -v grep");
try: 
   if len(string.trim(p)) > 0:
        exit();
except:
    while True:
        hstip=hst.retHostIP();
        constr='ods/ods@callhomeods:1521/callhomeods';
        oraconn=oracon.openconnect(constr);
        pythonRuns=commands.getoutput("ps -ef | grep python | grep -v grep | wc -l");
        sqlstmt='begin update STATSPROCESSINGMACHINE set status=1,status_post_time=sysdate,number_python_threads='+str(pythonRuns)+' where IPADDRESS=\''+hstip+'\'; commit; end;';
        oracon.execSql(oraconn,sqlstmt);
        time.sleep(900);
    


