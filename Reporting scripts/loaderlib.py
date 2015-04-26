#!/usr/bin/env python
import oracleconnect as oracon;
import os;
import sys;
import datetime;
import time;
import string;
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
        dirname ="/share/st"+str(dirNum)+dirOfInterest;
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
                    sqlstmt="begin insert into statfilenamelist(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
                    oracon.execSql(oraconn,sqlstmt);
                    oraconn.close();
                    filename="filenamelist_"+datatype+"_"+str(dirNum)+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
                    flnmlist = open (filepath+filename,"w");
            fileList=[];
        dirList=[];
        flnmlist.close();
        oraconn=oracon.openconnect(constr);    
        sqlstmt="begin insert into statfilenamelist(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
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
    sqlstmt="begin insert into statfilenamelist(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
    oracon.execSql(oraconn,sqlstmt);
    oraconn.close();
    log.close();
    return;

def filenameloadT2below(datatype,startfolder,endfolder):
    log = open("log/"+datatype+"filenameloader_tiertwobelow.log","w");
    dirOfInterest = "/prod/data/files/3PAR.INSERV/TierTwo";

    fileList=[];
    subdirList=[];
    dirList=[];
    filepath="/ods30/vm1/filename/";
    orafilepath="/ods30/vm1/filename/";
    filename="filenamelist_tiertwobelow_"+datatype+"_"+ time.strftime('%Y%m%d%H%M%S')+".lst";
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
                subdirList=os.listdir(dirname+"/"+dirn);
            except:
                log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n");    
            for subdirn in subdirList:
                try:
                    fileList=os.listdir(dirname+"/"+dirn+"/"+subdirn+"/"+datatype);
                except:
                    log.write(str(time.ctime())+" Error :"+ str(sys.exc_info()[1])+"\n");
                for filenm in fileList:
                    if string.find(filenm, ".bz2") == -1 and string.find(filenm, ".bad") == -1:
                        flnmlist.write(str(dirn)+"\t"+dirname+"/"+dirn+"/"+subdirn+"/"+datatype+"\t"+datatype+"\t"+filenm+"\n");
                        goodfiles=goodfiles+1;
                        sqlstmt="";
                fileList=[];
            subdirList=[];
        dirList=[];
        i=i+1;
    
    flnmlist.close();   
    oraconn=oracon.openconnect(constr);    
    sqlstmt="begin insert into statfilenamelist(filenamepath,statfilename) values (\'"+orafilepath+"\',\'"+filename+"\'); commit; end; ";
    oracon.execSql(oraconn,sqlstmt);
    oraconn.close();
    log.close();
    return;