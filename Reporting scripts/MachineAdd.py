#!/usr/bin/env python

import hostlib as hst;
import oracleconnect as oracon;
import decimal;

constr='ods/ods@callhomeods:1521/callhomeods';
oraconn=oracon.openconnect(constr);

print "Begining to add machine for processing ........................"

splitterloc=raw_input("Splitter processing location:");
tabberloc=raw_input("Processed file location:")
purpose=raw_input("Purpose of the machine:");
hstname=hst.retHostName();
hstip=hst.retHostIP();
hstcpu=hst.retcpuCount();
hstmem=hst.retphyMem();

hstthread=int((round((hstmem - 4)/2,0))*hstcpu)*100;
sqlstmt='BEGIN DATALOAD.MACHINEADD(P_HOSTNAME=>\''+hstname+'\',P_IPADDRESS=>\''+hstip+'\',P_NUMBEROFTHREADS=>'+str(hstthread)+',P_SPLITTERLOC=>\''+splitterloc+'\',P_PROCESSLOC=>\''+tabberloc+'\',P_PURPOSE=>\''+purpose+'\'); END;';
oracon.execSql(oraconn,sqlstmt);

print "Machine successfully added.........."


