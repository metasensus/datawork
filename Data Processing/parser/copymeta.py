#!/usr/bin/env python

import oracleconnect as oracon;
import mysqlconnect as lconnect;
import time;
import statslog;
import sys;

constr='ods/ods@callhomeods:1521/callhomeods';
def updateMetadata():
    try:
        lsconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
        lscurr = lsconn.cursor();
    
        oraconn=oracon.openconnect(constr);
    
        sqlstmt='SELECT STATSID, STATS_STRUCTURE_ID, STATSNAME, STATS_VERSION, STATS_FIRST_ROW_VERSION, STATS_SINGLE_ROW, STATS_END_OF_ROW FROM STATSSTRUCTURE ORDER BY 1';
        statrec=oracon.execSql(oraconn,sqlstmt);
    
        sqlstmt='SELECT DATATYPE, SPLIT_FILE_TYPE, SPLIT_FILE_SEARCH_TAG, SPLIT_FILE_SKIP_LINES, SPLIT_FILE_END_TAG, SPLIT_FILE_LINE_SEPERATOR, STATSID FROM STAT_SPLIT_FILE_LOOKUP';
        splitrec=oracon.execSql(oraconn,sqlstmt);
        
        sqlstmt='SELECT STATS_STRUCTURE_ID, PROCESS_SEQUENCE, FUNCTIONID FROM PROCESSLOGIC'
        processrec=oracon.execSql(oraconn,sqlstmt);
        
        sqlstmt='SELECT FUNCTIONID, FUNCTIONNAME, FUNCTIONPARAMETERS, FUNCTIONDESC FROM PROCESSFUNCTION'
        profunc=oracon.execSql(oraconn,sqlstmt);
        
        
        for rec in statrec:
            sqlstmt='SELECT COUNT(1) FROM STATSSTRUCTURE WHERE STATSID='+str(rec[0])+' and STATS_STRUCTURE_ID='+str(rec[1]);
            lscurr.execute(sqlstmt);
            recData=lscurr.fetchall();
            recCount=0;
            for recdat in recData:
                recCount=recdat[0];
            if recCount == 0:
                if not rec[6]:
                    end_row='';
                else:
                    end_row=rec[6];
                    
                sqlstmt='INSERT INTO STATSSTRUCTURE (STATSID, STATS_STRUCTURE_ID, STATSNAME, STATS_VERSION, STATS_FIRST_ROW_VERSION, STATS_SINGLE_ROW, STATS_END_OF_ROW) VALUES ';
                sqlstmt+=' ('+str(rec[0])+','+str(rec[1])+',\''+rec[2]+'\',\''+ rec[3]+'\',\''+rec[4]+'\',\''+rec[5]+'\',\''+end_row+'\')';
                lscurr.execute(sqlstmt);
                
        for rec in splitrec:
            sqlstmt='SELECT COUNT(1) FROM STAT_SPLIT_FILE_LOOKUP WHERE DATATYPE=\''+str(rec[0])+'\' and  SPLIT_FILE_TYPE=\''+rec[1]+'\'';
            lscurr.execute(sqlstmt);
            recData=lscurr.fetchall();
            recCount=0;
            for recdat in recData:
                recCount=recdat[0];
            if recCount == 0:
                if not rec[5]:
                    line_sep='';
                else:
                    line_sep=rec[5];
                sqlstmt='INSERT INTO STAT_SPLIT_FILE_LOOKUP (DATATYPE, SPLIT_FILE_TYPE, SPLIT_FILE_SEARCH_TAG, SPLIT_FILE_SKIP_LINES, SPLIT_FILE_END_TAG, SPLIT_FILE_LINE_SEPERATOR, STATSID) ';
                sqlstmt+='VALUES (\''+str(rec[0])+'\',\''+str(rec[1])+'\',\''+rec[2]+'\','+ str(rec[3])+',\''+rec[4]+'\',\''+line_sep+'\','+str(rec[6])+')';
                lscurr.execute(sqlstmt);
        
        for rec in processrec:
            sqlstmt='SELECT COUNT(1) FROM PROCESSLOGIC WHERE STATS_STRUCTURE_ID='+str(rec[0])+' and PROCESS_SEQUENCE='+str(rec[1])+' and FUNCTIONID='+str(rec[2]);
            #sqlstmt='SELECT COUNT(1) FROM PROCESSLOGIC WHERE STATS_STRUCTURE_ID='+str(rec[0]);
            lscurr.execute(sqlstmt);
            recData=lscurr.fetchall();
            recCount=0;
            for recdat in recData:
                recCount=recdat[0];
            if recCount == 0:
                sqlstmt='INSERT INTO PROCESSLOGIC (STATS_STRUCTURE_ID, PROCESS_SEQUENCE, FUNCTIONID) ';
                sqlstmt+='VALUES ('+str(rec[0])+','+str(rec[1])+','+str(rec[2])+')';
                lscurr.execute(sqlstmt);
            #else:    
            ##fix logic if needed
            #    sqlstmt='select STATS_STRUCTURE_ID, PROCESS_SEQUENCE, FUNCTIONID FROM PROCESSLOGIC WHERE STATS_STRUCTURE_ID='+str(rec[0])+' and PROCESS_SEQUENCE='+str(rec[1]);
            #    lscurr.execute(sqlstmt);
            #    dat=lscurr.fetchall();
            #    for dt in dat:
            #        if dt[2] != rec[2]:
            #            sqlstmt='update PROCESSLOGIC SET FUNCTIONID='+str(rec[2])+' WHERE STATS_STRUCTURE_ID='+str(rec[0])+' and PROCESS_SEQUENCE='+str(rec[1]);
            #            lscurr.execute(sqlstmt);
        
                    
        sqlstmt='SELECT STATS_STRUCTURE_ID, PROCESS_SEQUENCE, FUNCTIONID FROM PROCESSLOGIC';
        lscurr.execute(sqlstmt);
        mySqlDat=lscurr.fetchall();
        for myRec in mySqlDat:
            sqlstmt='SELECT COUNT(1) FROM PROCESSLOGIC where STATS_STRUCTURE_ID='+str(myRec[0])+' AND PROCESS_SEQUENCE='+str(myRec[1])+' AND FUNCTIONID='+str(myRec[2]);
            processrec=oracon.execSql(oraconn,sqlstmt);
            for recdat in processrec:
                recCount=recdat[0];
            if recCount == 0:
                sqlstmt='delete from PROCESSLOGIC where STATS_STRUCTURE_ID='+str(myRec[0])+' and PROCESS_SEQUENCE ='+str(myRec[1])+' and FUNCTIONID ='+str(myRec[2]);
                lscurr.execute(sqlstmt);
        
        
                
        for rec in profunc:
            sqlstmt='SELECT COUNT(1) FROM PROCESSFUNCTION WHERE FUNCTIONID='+str(rec[0])+' and FUNCTIONNAME=\''+str(rec[1])+'\'';
            lscurr.execute(sqlstmt);
            recData=lscurr.fetchall();
            recCount=0;
            for recdat in recData:
                recCount=recdat[0];
            if recCount == 0:
                sqlstmt='INSERT INTO PROCESSFUNCTION (FUNCTIONID, FUNCTIONNAME, FUNCTIONPARAMETERS, FUNCTIONDESC) ';
                sqlstmt+='VALUES ('+str(rec[0])+',\''+str(rec[1])+'\',\''+str(rec[2])+'\',\''+str(rec[3])+'\')';
                lscurr.execute(sqlstmt);       
                
        lscurr.close();
        lsconn.close();
        oraconn.close();
    except:
        fl=statslog.logcreate('log/copymeta.log');
        statslog.logwrite(fl,'Error reported: '+str(sys.exc_info()[1]))

    
def main():
    while (1):
        updateMetadata(); 
        time.sleep(300);

if __name__ == '__main__':
    main();