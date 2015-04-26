#!/usr/bin/env python

import hostlib as hst;
import oracleconnect as oracon;
import decimal;
import os;
import time;

constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
oraconn=oracon.openconnect(constr);

print "Begining to add machine for processing ........................"
purpose=raw_input("Purpose of the machine:");
if purpose=='splitter':
    splitterloc=raw_input("Splitter processing location:");
else:
    splitterloc=' ';
if purpose=='process':    
    tabberloc=raw_input("Processed file location:");
else:
    tabberloc=' ';

if purpose=='splitter' or purpose=='process':    
    numfiles=raw_input("Number of files that can be processed at a time:");
    waittime=raw_input("Wait time between each:");
else:
    numfiles=0;
    waittime=0;
    
if purpose == 'sync':
    try:
        pathList=['/root/proc','/root/proc/mysqldump','/root/proc/mysqldump/archive','/root/proc/log']
    
        #for pth in pathList:
        #    pthexist=False;
        #    print(pth);
        #    d=os.path.dirname(pth);
        #    pthexist=os.path.exists(d);
        #    print(pthexist);
        #    if(pthexist==False):
        #        print(pth);
        #        os.makedirs(pth);
        #        pthexist=False;
        
        dir=[os.path.exists(pathList[0]),os.path.exists(pathList[1]),os.path.exists(pathList[2]),os.path.exists(pathList[3])]
        
        #print dir[0];
        #print dir[1];
        #print dir[2];
        #print dir[3];

        if(dir[0]==False):
          
           os.makedirs(pathList[0])

        if(dir[1]==False):
         
           os.makedirs(pathList[1])

        if(dir[2]==False):
         
           os.makedirs(pathList[2])
        
        if(dir[3]==False):
         
           os.makedirs(pathList[3])

        
                
    except:            
         print sys.exc_info()[1];
             
hstname=hst.retHostName();
hstip=hst.retHostIP();
hstcpu=hst.retcpuCount();
hstmem=hst.retphyMem();

hstthread=int((round((hstmem - 4)/2,0))*hstcpu)*100;
sqlstmt='BEGIN DATALOAD.MACHINEADD(P_HOSTNAME=>\''+hstname+'\',P_IPADDRESS=>\''+hstip+'\',P_NUMBEROFTHREADS=>'+str(hstthread)+',P_SPLITTERLOC=>\''+splitterloc+'\',P_PROCESSLOC=>\''+tabberloc+'\',P_PURPOSE=>\''+purpose+'\',P_NUM_FILES_PER_RUN=>'+str(numfiles)+',P_DELAY_SECONDS=>'+str(waittime)+'); END;';
oracon.execSql(oraconn,sqlstmt);

print "Machine successfully added.........."


