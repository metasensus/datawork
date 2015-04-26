#!/usr/bin/env python

import loaderlib as ldr;    
import oracleconnect as oracon;
import time;
from multiprocessing import Process;

constr='ods/ods@callhomeods:1521/callhomeods';
oraconn=oracon.openconnect(constr);

while True:
    ldr.filenameloadT2below(15,20,);
    time.sleep(4*3600);



