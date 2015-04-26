#!/usr/bin/env python
import sys;
import oracleconnect as oracon;
import statslog;
from multiprocessing import Process;
import commands;
import time;
import shutil;
import os;

constr='ods/ods@callhomeods:1521/callhomeods';

def copyFile(FileName,OriginalPath,NewPath,fileID):
    oraconn=oracon.openconnect(constr);
    sqlstmt='begin update alternatepath =\''+NewPath+'\' where STATS_OUTPUTFILE_ID='+str(fileID)+'; commit; end;';
        
    shutil.copyfile(OriginalPath+FileName,NewPath+FileName);
    oracon.execSql(oraconn,sqlstmt);
    

def rmFile(File):
    if os.path.isfile(File):
        os.remove(File);
    
def copyRemoveFile():
    try:
        oraconn=oracon.openconnect(constr);
        sqlstmt='SELECT DISTINCT STATSNAME FROM STATSSTRUCTURE';
        stat=oracon.execSql(oraconn,sqlstmt);
        for statrec in stat:
            sqlstmt='SELECT STATS_OUTPUTFILE_ID,STATS_OUTPUTFILE_PATH,STATS_OUTPUTFILE_NAME FROM STATSOUTPUT a, STATSSTRUCTURE b';
            sqlstmt+=' WHERE STATS_FILE_LOAD_STATUS = 0 AND ROWNUM <= 500 AND b.STATSNAME = '+STATSNAME+' AND A.STATS_STRUCTURE_TYPE_ID = B.STATS_STRUCTURE_ID';
            sqlstmt+=' ORDER BY STATS_OUTPUTFILE_ID DESC';
            files=oracon.execSql(oraconn,sqlstmt);
            for flrec in files:
                p=Process(target=copyFile,args=(flrec[2],flrec[1],'/odstmp/'));
                p.daemon = True;    
