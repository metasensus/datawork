#!/usr/bin/env python
import oracleconnect as oracon;
import os;
import sys;
import datetime;
import time;
import string;
import commands;
from multiprocessing import Process;

constr='ods/ods@callhomeods:1521/callhomeods';
maxrowctr=300000;

def subDirList(subdirn,subn,dirname,datatype,dirn,dirNum):
    try:
        #code
        fileList=[];
        
        fList=[];
        goodfiles=0;
        rowctr = 1;
        try:
            fileList=os.listdir(dirname+"/"+dirn+"/"+subdirn+"/"+datatype);
        except:    
            pass;
        for filenm in fileList:
            if string.find(filenm, ".bz2") == -1 and string.find(filenm, ".bad") == -1:
                fList.append(str(subdirn)+"\t"+dirname+"/"+dirn+"/"+subdirn+"/"+datatype+"\t"+datatype+"\t"+filenm);
        return fList;
    except:
        log = open("log/filename_"+subn+"_"+datatype+"_"+str(dirNum)+"_"+str(subdirn)+"_"+ time.strftime('%Y%m%d%H%M%S')+".log","w");   
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();


def subdirlistcurr(dirname,dirn,datatype,dirNum):
    try:
        maxrowctr=100000;   
        subdirList=[];
        filelist=[];
        filepath="/ods145/goldenvm/filename/";
        orafilepath="/ods145/goldenvm/filename/";
        try:
            subdirList=os.listdir(dirname+"/"+dirn);
        except:    
            pass;
        
        subn=string.replace(dirn,'-','');
        filename="filename_"+datatype+"_"+str(dirNum)+"_"+subn+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
        flnmlist = open (filepath+filename,"w");
        numDir = 1;
        numLines = 0;
	foldersinarray=0;
	filenameList=[];
	subdirList=sorted(subdirList);
        for subdirn in subdirList:
            fileList=subDirList(subdirn,subn,dirname,datatype,dirn,dirNum);
	    filenameList.append(string.join(fileList,'\n'));
	    foldersinarray+=1;
	    if foldersinarray > 1000:
		flnmlist.write(string.join(filenameList)+'\n');
		flnmlist.flush();
		flnmlist.close();
		sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename,created_date) values (\'"+orafilepath+"\',\'"+filename+"\',sysdate); commit; end; ";
		oracon.execSql(oraconn,sqlstmt);
		oraconn.close();
	
		filename="filename_"+datatype+"_"+str(dirNum)+"_"+subn+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
		flnmlist = open (filepath+filename,"w");
		foldersinarray=0;
		filenameList=[];
	flnmlist.write(string.join(filenameList)+'\n');
	flnmlist.flush();	
	flnmlist.close();
	oraconn=oracon.openconnect(constr); 
	sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename,created_date) values (\'"+orafilepath+"\',\'"+filename+"\',sysdate); commit; end; ";
        oracon.execSql(oraconn,sqlstmt);
        oraconn.close();
    except:
	log = open("log/filename_"+datatype+"_"+str(dirNum)+"_"+subn+"_"+ time.strftime('%Y%m%d%H%M%S')+".log","w");        
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();                        

def waitTime():
    pythonRuns=commands.getoutput("ps -ef | grep filenameloadert2below | grep -v grep | wc -l");
    if int(pythonRuns) > 100:
        time.sleep(60);
        pythonRuns=commands.getoutput("ps -ef | grep filenameloadert2below | grep -v grep | wc -l");
        while int(pythonRuns) > 100:
            time.sleep(60);
            pythonRuns=commands.getoutput("ps -ef | grep filenameloadert2below | grep -v grep | wc -l");
    return;

def dirListingCurrT2(datatype,dirNum):
    try:
        dirOfInterest = "/prod/data/files/3PAR.INSERV/TierTwo";
        fileList=[];
        subdirList=[];
        dirList=[];
        dirSort=[]
        dirname ="/share/st"+str(dirNum)+"ro"+dirOfInterest;
        try:
            dirList=os.listdir(dirname);
        except:    
            pass;      
        
        dirsort=sorted(dirList,reverse=True);    
        for dirn in dirsort:
            waitTime();
            p=Process(target=subdirlistcurr,args=(dirname,dirn,datatype,dirNum,));
            #p.daemon=True;
            p.start();
            #subdirlist(dirname,dirn,datatype,dirNum);
    except:
        log = open("log/"+datatype+"_filenameloader_tiertwobelow_"+str(dirNum)+".log","w");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();

def filenameloadT2belowCurr(startfolder,endfolder):
    try:
        constr='ods/ods@callhomeods:1521/callhomeods';
        oraconn=oracon.openconnect(constr);
        log = open("log/filenameloader_tiertwoCurrent.log","w");
        sqlstmt="select trim(foldername) from statsfolders where enabled=1";
        eventcur=oracon.execSql(oraconn,sqlstmt);
        for evnt in eventcur:
	    log.write("Starting event :"+evnt[0]+"\n");
	    log.flush();
	    i=startfolder;
	    while(i<=endfolder):
		log.write(" Folder :"+str(i)+"\n");
		log.flush();
		waitTime();
		p=Process(target=dirListingCurrT2,args=(evnt[0],i,));
		p.start();
		i=i+1;
	log.close();
    except:
     #   log = open("log/filenameloader_tiertwobelow.log","w");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();

