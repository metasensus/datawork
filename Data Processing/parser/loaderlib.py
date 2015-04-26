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
maxrowctr=30000;


def dirListing(datatype,dirNum):
    try:
        dirOfInterest = "/prod/data/files/3PAR.INSERV";
        fileList=[];
        filepath="/ods30/vm1/filename/";
        orafilepath="/ods30/vm1/filename/";
        rowcounter=1;
        dirList=[];
        filename="filenamelist_"+datatype+"_"+str(dirNum)+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
        flnmlist = open (filepath+filename,"w");
        dirname ="/share/st"+str(dirNum)+"ro"+dirOfInterest;
        try:
            dirList=os.listdir(dirname);
            log.write(dirList);
        except:
            pass;
        for dirn in dirList:
            goodfiles=0;
            try:
                fileList=os.listdir(dirname+"/"+dirn+"/"+datatype);
            except:
                pass;
            for filenm in fileList:
                if string.find(filenm, ".bz2") == -1 and string.find(filenm, ".bad") == -1:
                    flnmlist.write(str(dirn)+"\t"+dirname+"/"+dirn+"/"+datatype+"\t"+datatype+"\t"+filenm+"\n");
                    goodfiles=goodfiles+1;
                    rowcounter=rowcounter+1;
                if rowcounter >= maxrowctr:
                    rowcounter=0;
                    flnmlist.close();    
                    oraconn=oracon.openconnect(constr);    
                    sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
                    oracon.execSql(oraconn,sqlstmt);
                    oraconn.close();
                    filename="filenamelist_"+datatype+"_"+str(dirNum)+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
                    flnmlist = open (filepath+filename,"w");
            fileList=[];
        dirList=[];
        flnmlist.close();
        oraconn=oracon.openconnect(constr);    
        sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
        oracon.execSql(oraconn,sqlstmt);
        oraconn.close();
    except:
        log = open("log/"+datatype+'_'+str(dirNum)+'_'+"filenameloader.log","w");
        log.write("Error reported: "+str(sys.exc_info()[1])+" in filenameloader");
        log.close();
   
# Function loads data filenames from the inserv directory
def filenameload(datatype,startfolder,endfolder):
    i=startfolder;
    while(i<=endfolder):
        p=Process(target=dirListing,args=(datatype,i,));
        p.start();
        i=i+1;
    return;
    
    
    
def filenameloadT2(datatype,startfolder,endfolder):
    log = open("log/"+datatype+"filenameloader_tiertwo.log","w");
    dirOfInterest = "/prod/data/files/3PAR.INSERV/TierTwo";

    fileList=[];
    subdirList=[];
    filepath="/ods30/vm1/filename/";
    orafilepath="/ods30/vm1/filename/";
    filename="filenamelist_tiertwo_"+datatype+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
    flnmlist = open (filepath+filename,"w");
    i=startfolder;
   
    while(i<=endfolder):
        dirname ="/share/st"+str(i)+dirOfInterest;
        try:
            dirList=os.listdir(dirname);
        except:
            log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n");
        for dirn in dirList:
            goodfiles=0;
            try:
                fileList=os.listdir(dirname+"/"+dirn+"/"+datatype);
            except:
                log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n");
            for filenm in fileList:
                if string.find(filenm, ".bz2") == -1 and string.find(filenm, ".bad") == -1:
                    flnmlist.write(str(dirn)+"\t"+dirname+"/"+dirn+"/"+datatype+"\t"+datatype+"\t"+filenm+"\n");
                    goodfiles=goodfiles+1;
                    sqlstmt="";
            fileList=[];
        dirList=[];
        i=i+1;
    
    flnmlist.close();   
    oraconn=oracon.openconnect(constr);    
    sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
    oracon.execSql(oraconn,sqlstmt);
    oraconn.close();
    log.close();
    return;

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
                
def subdirlist(dirname,dirn,datatype,dirNum):
    try:
   
        subdirList=[];
        filelist=[];
        filepath="/ods30/vm1/filename/";
        orafilepath="/ods30/vm1/filename/";
        try:
            subdirList=os.listdir(dirname+"/"+dirn);
        except:    
            pass;
        
        subn=string.replace(dirn,'-','');
        filename="filename_"+datatype+"_"+str(dirNum)+"_"+subn+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
        flnmlist = open (filepath+filename,"w");
        numDir = 1;
        log = open("log/filename_"+datatype+"_"+str(dirNum)+"_"+subn+"_"+ time.strftime('%Y%m%d%H%M%S')+".log","w");
       
        for subdirn in subdirList:
            fileList=subDirList(subdirn,subn,dirname,datatype,dirn,dirNum);
            flnmlist.write(string.join(fileList,'\n'));
            log.write("Written "+str(len(fileList))+" for "+subdirn+"\n");
            log.flush();
            flnmlist.write('\n');
            flnmlist.flush();
            #if numDir > 1000:
        oraconn=oracon.openconnect(constr); 
        sqlstmt="begin insert into statfilenamelist_start(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
        oracon.execSql(oraconn,sqlstmt);
        flnmlist.close();
        oraconn.close();
        log.close();
    except:
        
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

def dirListingT2(datatype,dirNum):
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
            p=Process(target=subdirlist,args=(dirname,dirn,datatype,dirNum,));
            #p.daemon=True;
            p.start();
            #subdirlist(dirname,dirn,datatype,dirNum);
    except:
        log = open("log/"+datatype+"_filenameloader_tiertwobelow_"+str(dirNum)+".log","w");
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();
    

def filenameloadT2below(startfolder,endfolder):
    try:
        constr='ods/ods@callhomeods:1521/callhomeods';
        oraconn=oracon.openconnect(constr);
        log = open("log/filenameloader_tiertwobelow.log","w");
        sqlstmt="select trim(foldername) from statsfolders where enabled=1";
        eventcur=oracon.execSql(oraconn,sqlstmt);
        for evnt in eventcur:
            log.write("Starting event :"+evnt[0]+"\n"); 
            i=startfolder;
            while(i<=endfolder):
                waitTime();
                p=Process(target=dirListingT2,args=(evnt[0],i,));
                p.start();
                i=i+1;
    except:
        
        log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n"); 
        log.close();

