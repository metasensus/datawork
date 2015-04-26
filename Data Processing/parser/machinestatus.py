#!/usr/bin/env python
import hostlib as hst;
import oracleconnect as oracon
import mysqlconnect as mconnect;
import time;
import os;
import commands;
import string;
import statslog;
import sys;
from subprocess import call;

p = commands.getoutput("ps -ef | grep [m]achinestatus |wc -l");
constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';

def remSpace(diskUtil):
    newUtil=[]
    diskUtil=string.split(diskUtil, ' ')
    for ln in diskUtil:
        if len(string.strip(ln))>0:
            newUtil.append(ln)
    return newUtil;

if int(p) > 1:
   print('MachineStatus process is already running on this machine! exiting....');
   exit();
while True:
   try:
      recCount=0;
      spaceused=0;
      #print "Getting host IP....";
      hstip=hst.retHostIP();
      hstname=hst.retHostName();
      oraconn=oracon.openconnect(constr);
      #print "Getting purpose of the machine....";
      sqlstmt='select purpose,substr(splitterloc,2,instr(splitterloc,\'/\',1,2)-instr(splitterloc,\'/\',1,1)-1) splitloc,substr(processloc,2,instr(processloc,\'/\',1,2)-instr(processloc,\'/\',1,1)-1)processloc,';
      sqlstmt+='machineid,substr(taggerloc,2,instr(taggerloc,\'/\',1,2)-instr(taggerloc,\'/\',1,1)-1)taggerloc,nvl(stop_file_process, 0) stop_file_process,nvl(stop_all,0) stop_all ';
      sqlstmt+='from statsprocessingmachine where ipaddress=\''+hstip+'\'';
      mpurpose=oracon.execSql(oraconn,sqlstmt);
      
      for mp in mpurpose:
         pdatacopy=0;
         pparser=0;
         pcpmeta=0;
         pdatamart=0;
         ptagger=0;
         dataloc=0;
         processed=0;
         tobeProcessed=0;
         errored=0;
         p1="dataCopy";
         p2="parse";
         p3="copymeta";
         p4="datamart";
         p5="filenameloaderCurrent"
         purpose=mp[0];
         macid=mp[3];
         stpProcess=mp[5];
         stpAll=mp[6];
      mpurpose.close();
      oraconn.close();
      
      if stpAll==1:
         pname='python'
         subprocess.call("./kill_proc.sh "+pname, shell=True)
         subprocess.call("./kill"+pname+".sh", shell=True)
         exit
         
      if purpose=='splitter':
         dataloc=mp[1];
         pname='parser';
         lconn = mconnect.connectMysql('ods','procuser','c@llhome','localhost');
         lcurr = lconn.cursor();
         lcurr.execute('select count(1) from STATS_SPLIT_FILES');
         splitCount=lcurr.fetchall();
         for rec in splitCount:
            recCount=rec[0];
         #print "dataCopy count is:"+str(recCount);
         sqlstmt='select ifnull(file_process_status,0),count(1) from STATSPROCESSTRANSACT group by ifnull(file_process_status,0)';
         lcurr.execute(sqlstmt);
         processData=lcurr.fetchall();
         for prec in processData:
            if prec[0]==0:
               tobeProcessed=prec[1];
               #print "To be Processed Count:"+str(tobeProcessed);
            if prec[0]==1:
               processed=prec[1];
               #print "Processed Count:"+str(processed);
            if prec[0]==3:
               errored=prec[1];
               #print "Failed Count:"+str(errored);
         lconn.close();
      
      if purpose=='process':
         dataloc=mp[2];
         pname='parsetabber';
         lconn = mconnect.connectMysql('ods','procuser','c@llhome','localhost');
         lcurr = lconn.cursor();
         lcurr.execute('select count(1) from STATSOUTPUT');
         tabberCount=lcurr.fetchall();
         for rec in tabberCount:
            recCount=rec[0];
         sqlstmt='select ifnull(file_process_status,0),count(1) from STATS_SPLIT_FILES group by ifnull(file_process_status,0)';
         lcurr.execute(sqlstmt);
         processData=lcurr.fetchall();
         for prec in processData:
            if prec[0]==0:
               tobeProcessed=prec[1];
            if prec[0]==1:
               processed=prec[1];
            if prec[0]==3:
               errored=prec[1];
         lconn.close();
         
      pythonRuns=commands.getoutput("ps -ef|grep [p]ython|wc -l");
      plist = os.popen("ps -Af|grep python").read();
      
      if p1 in plist[:]:
         pdatacopy=1;
      if p2 in plist[:]:
         pparser=1;
      if p3 in plist[:]:
         pcpmeta=1;
      if p4 in plist[:]:
         pdatamart=1;         
      if p5 in plist[:]:
         ptagger=1;
         
      cstring=("df -k | grep %s"%dataloc);
      istring=("df -i | grep %s"%dataloc);
      inodeutil=remSpace(commands.getoutput(istring));
      diskutil=remSpace(commands.getoutput(cstring));
      inodespace=inodeutil[4]
      usedspace=diskutil[4];
      spaceused=string.strip(usedspace,'%')
      #print "Total UsedSpace is :%s" %str(totused)
      if (pparser==1 and (str(spaceused)=='100' or stpProcess==1)):
         call("./kill_proc.sh "+pname, shell=True)
         call("./kill"+pname+".sh", shell=True)
      
      if purpose=='tagger':
         recCount=0;
         dataloc=mp[4];
         pname='filenameloaderCurrent'

      if (purpose=='datamartextract' or purpose == 'sync'):
         st = os.statvfs('/');
         free = st.f_bavail * st.f_frsize;
         total = st.f_blocks * st.f_frsize;
         used = (st.f_blocks - st.f_bfree) * st.f_frsize;
         spaceused= round((used*1.0/total*1.0)*100,2);
         inodespace=0;
         dataloc=str(hstname)      
         
      oraconn=oracon.openconnect(constr);
      sqlst='begin insert into OMI_MACHINE_STATUS (MACHINEID,MACHINENAME,IPADDRESS,STATUS_POST_TIME,PURPOSE,NUM_PYTHON_THREADS,DATALOCATION,DATACOPY_STATUS,';
      sqlst+='PARSER_STATUS,COPYMETA_STATUS,DATAMART_STATUS,TAGGER_STATUS,PROCESSED,TOBEPROCESSED,TOBECOPIED,FAILEDTOPROCESS,DISKSPACE_UTILIZATION,INODE_UTILIZATION) ';
      sqlst+='values (\''+str(macid)+'\',\''+str(hstname)+'\',\''+str(hstip)+'\',sysdate,\''+mp[0]+'\',\''+str(pythonRuns)+'\',\''+str(dataloc)+'\',\''+str(pdatacopy)+'\',';
      sqlst+='\''+str(pparser)+'\',\''+str(pcpmeta)+'\',\''+str(pdatamart)+'\',\''+str(ptagger)+'\','+str(processed)+','+str(tobeProcessed)+','+str(recCount)+',';
      sqlst+=''+str(errored)+',\''+str(spaceused)+'%\',\''+str(inodespace)+'\'); commit; end;';
      oracon.execSql(oraconn,sqlst);
      #print "closing connection";
      oraconn.close();

   except Exception:
      fl=statslog.logcreate("log/machineStatus.log");
      statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
   time.sleep(900);
