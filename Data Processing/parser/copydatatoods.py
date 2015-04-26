#!/usr/bin/env python
import oracleconnect as oracon;
import mysqlconnect as mysqlconn;
import string;
import sys;

constr='ods/ods@callhomeods:1521/callhomeods';
try:
    lc = mysqlconn.connectMysql('ods','procuser','c@llhome','localhost');
    curr=lc.cursor();
    sqlstmt='SELECT STATS_FILEID FROM STATSPROCESSTRANSACT WHERE IFNULL(FILE_PROCESS_STATUS,0) IN (0,2)';
    curr.execute(sqlstmt);
    fileidrec=curr.fetchall();
    fileidList=[];
    print 'Getting filelist from mysql.........';
    recordsprocessed=0;
    totaldone=0;
    for idrec in fileidrec:
        fileidList.append(str(idrec[0]));
        recordsprocessed+=1;
        if recordsprocessed >999:
            fileids=string.join(fileidList,',');
            sqlstmt='begin update STATSPROCESSTRANSACT SET FILE_PROCESS_STATUS=0 WHERE STATS_FILEID IN ('+fileids+'); commit; end;'
            oraconn=oracon.openconnect(constr);
            oracon.execSql(oraconn,sqlstmt);
            totaldone+=recordsprocessed;
            recordsprocessed=0;
            print 'Total records updated...:'+str(totaldone);
            fileidList=[];
    
    if len(fileidList)>0:    
        fileids=string.join(fileidList,',');
        sqlstmt='begin update STATSPROCESSTRANSACT SET FILE_PROCESS_STATUS=0 WHERE STATS_FILEID IN ('+fileids+'); commit; end;'
        oraconn=oracon.openconnect(constr);
        oracon.execSql(oraconn,sqlstmt);
        totaldone+=recordsprocessed;
        print 'Total records updated...:'+str(totaldone);
    
    
    print 'Starting mysql data cleanup.........';  
    sqlstmt='DELETE FROM STATSPROCESSTRANSACT WHERE IFNULL(FILE_PROCESS_STATUS,0) IN (0,2)';    
    curr.execute(sqlstmt);
    print 'Completed mysql data cleanup.........';
    curr.close;
    lc.close;
    oraconn.close();
except:
    print "Error reported: "+str(sys.exc_info()[1]);
        