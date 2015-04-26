#!/usr/bin/env python
import oracleconnect as oracon;
import os;
import sys;
import datetime;
import time;
import string;

constr='ods/ods@callhomeods:1521/callhomeods';

while True:
    try:
        oraconn=oracon.openconnect(constr);
        sqlstmt='SELECT STATFILENAME, FILENAMEPATH FROM STATFILENAMELIST WHERE LOADSTATUS=0';
        FileRec=oracon.execSql(oraconn,sqlstmt);
        sqllist=[];
        ctr=0;
        totctr=0;
        for frec in FileRec:
            if os.path.isfile(frec[1]+frec[0]):
                infl=open(frec[1]+frec[0])
                print frec[1]+frec[0]
                lines=infl.readlines();
                infl.close();
                for ln in lines:
                    lnsplit=string.split(ln,'\t');
                    sqllist.append('INSERT INTO TMP_FILENAME_LOAD(INSERVSERIAL,FILEPATH,DATATYPE,FILENAME) SELECT  '+lnsplit[0]+','+'\''+lnsplit[1]+'\',\''+lnsplit[2]+'\',\''+lnsplit[3]+'\' FROM DUAL;');
                    ctr+=1;
                    totctr+=1;
                    if ctr > 999:
                        ctr=0;
                        sqlstmt='begin '+string.join(sqllist,'\n')+'\n commit;\n end;';
                        oracon.execSql(oraconn,sqlstmt);
                        sqllist=[];
                print 'Inserted '+str(totctr)+' rows';    
                sqlstmt='BEGIN UPDATE STATFILENAMELIST SET LOADSTATUS=1 WHERE STATFILENAME=\''+frec[0]+'\' AND FILENAMEPATH=\''+frec[1]+'\'; COMMIT; END;';
                oracon.execSql(oraconn,sqlstmt);
            else:
                sqlstmt='BEGIN UPDATE STATFILENAMELIST SET LOADSTATUS=3 WHERE STATFILENAME=\''+frec[0]+'\' AND FILENAMEPATH=\''+frec[1]+'\'; COMMIT; END;';
                oracon.execSql(oraconn,sqlstmt);
        if len(sqllist)>0:
            sqlstmt='begin '+string.join(sqllist,'\n')+'\n commit;\n end;';
            oracon.execSql(oraconn,sqlstmt);
    except:
        log = open("log/fileclubber.log","w");
        log.write("Error reported: "+str(sys.exc_info()[1])+" in filenameloader");
        log.close();
    #time.sleep(900);    