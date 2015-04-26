#!/usr/bin/env python

import loaderlib as ldr;    
import oracleconnect as oracon;
import time;
from multiprocessing import Process;

constr='ods/ods@callhomeods:1521/callhomeods';
oraconn=oracon.openconnect(constr);

while True:
    sqlstmt="select trim(foldername) from statsfolders where enabled=1";
    eventcur=oracon.execSql(oraconn,sqlstmt);
    try:
        for evnt in eventcur:
            try:
                p=Process(target=ldr.filenameloadT2,args=(evnt[0],14,20,));
                p.daemon=True;
                p.start();
            except:
                print "Error reported: "+str(sys.exc_info()[1]);
    except:
        print "Error reported: "+str(sys.exc_info()[1]);
    time.sleep(86400);

