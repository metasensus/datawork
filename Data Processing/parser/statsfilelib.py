#!/usr/bin/env python
import oracleconnect as oracon;
import mysqlconnect as lconnect;
import string;
import hostlib;
import sys;
from multiprocessing import Process;
import time;
import os;
import statslog;
import commands;
import atexit;
import traceback;


constr='ods/ods@callhomeods:1521/callhomeods';
debug='False'

def checkmakedir(dname,fname):
    try:
        d=os.path.dirname(dname);
        if not os.path.exists(d):
            os.makedirs(d);
    except:
        fl=statslog.logcreate(fname);
        statslog.logwrite(fl,"Error reported (create dir): "+str(sys.exc_info()[1]));
    return;

#def readdata(srcfl):
#    datArr =[];
#    for line in srcfl:
#        datArr.append(line);
#    return datArr;

def splitFile(fileid,filename,filepath,datatype,inservserial,datafolder,machineid):
    lsconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
    lscurr = lsconn.cursor();
    osver='';
    #fl=statslog.logcreate('log/splitter_'+filename+'.log');
        
    try:
        # Setting the path to the individual inserv
        splitfilepath=datafolder+'/'+str(inservserial)+'/';
        #statslog.logwrite(fl,'Processing file ..'+ filename);
    
        inputfile = filepath+'/'+filename;
        #statslog.logwrite(fl,'Setup input file......'+inputfile);
        if os.path.isfile(inputfile):
            srcfl=open(inputfile);
            #statslog.logwrite(fl,'Reading file ..'+ filename);
            readalllines=srcfl.readlines();
            
            srcfl.close;
            sqlstmt ='select SPLIT_FILE_TYPE, SPLIT_FILE_SEARCH_TAG, SPLIT_FILE_SKIP_LINES, ifnull(SPLIT_FILE_END_TAG,\'          \') SPLIT_FILE_END_TAG,';
            sqlstmt +='SPLIT_FILE_LINE_SEPERATOR,STATSID from STAT_SPLIT_FILE_LOOKUP where datatype=\''+datatype+'\' order by statsid';
        
            #statslog.logwrite(fl,'Reading file Mysql structure for the input file...');
            #statslog.logwrite(fl,sqlstmt);
            lscurr.execute(sqlstmt);
            #statslog.logwrite(fl,'Received structure...');
            filenameList=filename.split('.');
            splitfllist=list();
            i=0;
            for splt in filenameList:
                if i>0:
                    splitfllist.append(splt);
                i+=1;
            currlinenum =0
            
            readfileln=readalllines;
            #statslog.logwrite(fl,'Reading os version...');
            for ln in readfileln:
                if string.find(ln,'Release version')>=0:
                    osver = string.strip(ln[15:len(ln)]);
            #statslog.logwrite(fl,'Looping through the structure...');
            datrec=lscurr.fetchall();
            for dat in datrec:
                currspllist=list();
                readline =0;
                readfileln=readalllines;
                cpstat=0;
                for ln in readfileln:
                    if string.find(ln,dat[1])>=0:
                        if readline == 0:
                            spflnm=dat[0]+'.'+str(inservserial)+'.'+string.join(splitfllist,'.');
                        cpstat = 1;
                    if cpstat == 1:
                        currspllist.append(ln);
                        readline+=1;
                        if datatype=='alert':
                            if  string.find(ln,dat[3])>0:
                                cpstat=0;
                                break;
                        if datatype == 'hwinvent':
                            if readline <=dat[2]:
                                pass;
                            else: 
                                if string.strip(ln[0:len(dat[3])])==string.strip(dat[3]):
                                    cpstat=0;
                                    break;
                                if not ln:
                                    print 'ln is null'
                                    if currlinenum >= len(readfileln):
                                        cpstat=0;
                                        break;
                                    nextstr=readfileln[currlinenum+1];
                                    print 'nexstr:'+nextstr;
                                    if not nextstr or string.strip(nextstr[0:len(dat[3])])== string.strip(dat[3]) or len(string.strip(nextstr)) == 0 or string.find(nextstr,'<') >=0 :
                                        cpstat=0;
                                        break;
                        else: 
                            if string.strip(ln[0:len(dat[3])])==string.strip(dat[3]):
                                cpstat=0;
                                break;
                            if not ln:
                                print 'ln is null'
                                if currlinenum >= len(readfileln):
                                    cpstat=0;
                                    break;
                                nextstr=readfileln[currlinenum+1];
                                print 'nexstr:'+nextstr;
                                if not nextstr or string.strip(nextstr[0:len(dat[3])])== string.strip(dat[3]) or len(string.strip(nextstr)) == 0 or string.find(nextstr,'<') >=0 :
                                    cpstat=0;
                                    break;
                            
                        currlinenum+=1;
                        
                splafterskip=list();
                currline=0;
                splafterskip.append(osver+'\n');
                #statslog.logwrite(fl,'Writing data....'); 
                if len(currspllist)>0:
                 #   statslog.logwrite(fl,'Creating directory......'+splitfilepath); 
                    checkmakedir(splitfilepath,'log/splitter_'+filename+'.log');
                 #   statslog.logwrite(fl,'Writing file ....'+splitfilepath+spflnm); 
                    spfl = open(splitfilepath+spflnm,'w');
                    
                    for curr in currspllist:
                        if currline >= dat[2] and currline < len(currspllist)-1 :
                            if len(string.strip(curr))>0:
                                if string.find(string.strip(curr),'-----') == -1 :
                                    if curr:
                                        splafterskip.append(curr);
                        currline+=1;
                    spfl.write(string.join(splafterskip));
                    spfl.close();
                    #sqlstmt='begin dataload.addsplitfile(inputfileid=>'+str(fileid)+',splitfiletype=>\''+lkp[0]+'\',splitfilename=>\''+spflnm+'\'';
                    #sqlstmt+=',splitfilepath=>\''+splitfilepath+'\',statsid=>'+str(lkp[5])+'); end;';
                    
                    sqlstmt='INSERT INTO STATS_SPLIT_FILES(STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, STATS_SPLIT_FILE_TYPE) values ';
                    sqlstmt+='('+str(fileid)+',\''+spflnm+'\',\''+splitfilepath+'\','+str(dat[5])+',\''+dat[0]+'\')';
                  #  statslog.logwrite(fl,sqlstmt);
                    lscurr.execute(sqlstmt);
        
            sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=1 where STATS_FILEID='+str(fileid);
            lscurr.execute(sqlstmt);
            lscurr.close();
            lsconn.close();
            if os.path.isfile('log/splitter_'+filename+'.log'):
                os.remove('log/splitter_'+filename+'.log');
        else:
            fl=statslog.logcreate('log/splitter_'+filename+'.log');
            statslog.logwrite(fl,"Error reported: "+inputfile+" does not exist..");
            sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=3 where STATS_FILEID='+str(fileid);
            lscurr.execute(sqlstmt);
            lscurr.close();
            lsconn.close();
    except:
        fl=statslog.logcreate('log/splitter_'+filename+'.log');
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
        sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=3 where STATS_FILEID='+str(fileid);
        lscurr.execute(sqlstmt);
        sqlstmt='delete from STATS_SPLIT_FILES where STATS_FILEID='+str(fileid);
        lscurr.execute(sqlstmt);
        lscurr.close();
        lsconn.close();
        
def splitter():
    oraconn=oracon.openconnect(constr);
    lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
    lcurr = lconn.cursor();
    try:
        sqlstmt='update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=0 where FILE_PROCESS_STATUS=2';
        lcurr.execute(sqlstmt);
        
        ipadd=hostlib.retHostIP();
        
        sqlstmt='select machineid,numberofthreads,dataprocessing_folder,splitterloc,number_of_files_per_run,delay_seconds from statsprocessingmachine where ipaddress=\''+ipadd+'\' and enable =1';
        numthreadrec=oracon.execSql(oraconn,sqlstmt);
        for numthread in numthreadrec:
            numthreads = numthread[1];
            machineid=numthread[0];
            datafolder=numthread[3];
            files_perrun=numthread[4];
            delay_seconds=numthread[5];
        
        nummysqltreads = int(numthreads);    
        numsplitterthreads =int(numthreads);
        sqlstmt='select distinct datatype from stat_split_file_lookup';
        datatyprec=oracon.execSql(oraconn,sqlstmt);
        #dtct=0;
        datatypes=[];
        for dt in datatyprec:
            #dtct+=1;
            datatypes.append(dt[0]);
        #numsplitthread=int(numsplitterthreads/dtct);
        #datatyprec=oracon.execSql(oraconn,sqlstmt);
        
        #lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
        #lcurr=lconn.cursor();
        sqlstmt='select count(1) from statsprocesstransact where machineid='+str(machineid)+' and FILE_PROCESS_STATUS=0 and datatype in (\''+string.join(datatypes,'\''+','+'\'')+'\')';
        datatypct=oracon.execSql(oraconn,sqlstmt);
        for dtctrec in datatypct:
            tot=dtctrec[0];
        
        datatypelist=[];
        sqlstmt='select datatype,count(1) from statsprocesstransact where machineid='+str(machineid)+' and FILE_PROCESS_STATUS=0 and datatype in (\''+string.join(datatypes,'\''+','+'\'')+'\') group by datatype';
        datatyprec=oracon.execSql(oraconn,sqlstmt);
        for dt in datatyprec:
            recperdatatype = int(((dt[1]*1.0)/(tot*1.0))*numsplitterthreads);
            datatypelist.append(dt[0]+','+str(recperdatatype));
    
        for dtype in datatypelist:
            datatype=string.split(dtype,',');
            sqlstmt='SELECT COUNT(1) FROM STATSPROCESSTRANSACT WHERE IFNULL(FILE_PROCESS_STATUS,0)=0 AND DATATYPE= \''+datatype[0]+'\'';
            lcurr.execute(sqlstmt);
            reccount=lcurr.fetchall();
            numInMySql=0;
            for rec in reccount:
                numInMySql=rec[0];
            numsplitterthreads=int(datatype[1]) - int(numInMySql);
            if numsplitterthreads > 0:
                sqlstmt='SELECT STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE FROM STATSPROCESSTRANSACT ';
                sqlstmt+='WHERE FILE_PROCESS_STATUS=0 AND MACHINEID='+str(machineid);
                sqlstmt+=' AND ROWNUM <='+str(numsplitterthreads)+' AND DATATYPE= \''+datatype[0]+'\' ORDER BY STATS_FILEID';
                filerec=oracon.execSql(oraconn,sqlstmt);
            
                sqlstmt='INSERT INTO STATSPROCESSTRANSACT (STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE) VALUES ';
                sql=[];
            
                for flrec in filerec:
                    sql.append('('+str(flrec[0])+',\''+flrec[1]+'\',\''+flrec[2]+'\',\''+flrec[3]+'\')');
                    sqlst='begin Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=2 where STATS_FILEID='+str(flrec[0])+'; commit; end;';
                    oracon.execSql(oraconn,sqlst);
                if len(sql) > 0:
                    sqlstmt=sqlstmt+string.join(sql,',');
                    lcurr.execute(sqlstmt);
        lconn.close();
        oraconn.close();
        lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
        lcurr = lconn.cursor();
        ctr =0;
        pythonRuns=commands.getoutput("ps -ef | grep parser | grep -v grep | wc -l");
        
        if int(pythonRuns) - 3 < nummysqltreads:
            residual=(nummysqltreads - int(pythonRuns));
            sqlstmt='SELECT COUNT(1) FROM STATSPROCESSTRANSACT where ifnull(FILE_PROCESS_STATUS,0)=0';
            lcurr.execute(sqlstmt);
            totalcurr=lcurr.fetchall();
            for lcur in totalcurr:
                total_recs=lcur[0];
            sqlstmt='SELECT DATATYPE,COUNT(1) FROM STATSPROCESSTRANSACT where ifnull(FILE_PROCESS_STATUS,0)=0 GROUP BY DATATYPE';
            lcurr.execute(sqlstmt);
            datrec=lcurr.fetchall();
            for lcur in datrec:
                datatype=lcur[0];
                numrecs=int(round((lcur[1]*1.0/total_recs*1.0)*residual*1.0,0));
                sqlstmt='SELECT distinct STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE FROM STATSPROCESSTRANSACT where ifnull(FILE_PROCESS_STATUS,0)=0 and DATATYPE=\''+ datatype+'\' LIMIT '+str(numrecs);
                lcurr.execute(sqlstmt);    
                ctr =0;
                statrow=lcurr.fetchall();
            
                for statrec in statrow:
                    flpthlist=string.split(statrec[2],'/');
                    inservserial=flpthlist[len(flpthlist)-2];
                    p=Process(target=splitFile,args=(statrec[0],statrec[1],statrec[2],statrec[3],inservserial,datafolder+'/splitter',machineid,));
                    p.daemon = True;
                    p.start();
                                 
                    while ctr > files_perrun:
                        time.sleep(delay_seconds);
                        sqlstmt='SELECT COUNT(1) FROM STATSPROCESSTRANSACT WHERE FILE_PROCESS_STATUS=2';
                        lcurr.execute(sqlstmt);
                        rowRec=lcurr.fetchall();
                        for rec in rowRec:
                            ctr=rec[0];
                    sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=2 where STATS_FILEID='+str(statrec[0]) +' and ifnull(FILE_PROCESS_STATUS,0)=0';
                    lcurr.execute(sqlstmt);  
                    ctr+=1;
        lcurr.close();
        lconn.close();
        #os.remove('log/splitter.log');
    except:
        fl=statslog.logcreate('log/splitter.log');
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));

def splitrunner():
    fl=statslog.logcreate('log/splitrunner.log');
    
    try:
        ipadd=hostlib.retHostIP();
        sqlstmt='select number_of_files_per_run from statsprocessingmachine where ipaddress=\''+ipadd+'\' and enable =1';
        oraconn=oracon.openconnect(constr);
        numthreadrec=oracon.execSql(oraconn,sqlstmt);
        
        for numthread in numthreadrec:
            files_perrun=numthread[0];
        numthreadrec.close();
        oraconn.close();
        
        while (1):
            pythonRuns=commands.getoutput("ps -ef | grep parser | grep -v grep | wc -l");
            if int(pythonRuns) < files_perrun + 1:
                p=Process(target=splitter);
                p.start();
            time.sleep(900);
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]),'splitrunner');
