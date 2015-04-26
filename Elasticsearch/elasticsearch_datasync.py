#!/usr/bin/env python
import oracleconnect as oracon;
import os;
import sys;
import commands;
import string;

def loadelasticsearch():
    try:
        sqlstmt='SELECT REPORTID, REPORT_SQL, INDEXNAME, JDBC_STRING, USERNAME, PASSWD,A.TABLE_ID FROM OCULUSELASTICSEARCH A,DATAMART_TABLE_EXTRACT B WHERE A.TABLE_ID=B.TABLE_ID AND NVL(SYNCDATE,TRUNC(SYSDATE)) <=  AGG_UPDATE_DATE ORDER BY REPORTID';
        constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
        oraconn=oracon.openconnect(constr);   
        datrec=oracon.execSql(oraconn,sqlstmt);
        
        for dt in datrec:
            reportid=dt[0];
            reportsql=dt[1];
            reportindex=dt[2];
            reportjdbc=dt[3];
            reportuser=dt[4];
            reportpasswd=dt[5];
            reporttableid=dt[6];
            
            
            indexcheck=commands.getoutput('curl -XGET \'localhost:9200/'+reportindex+'/_search?q=*\'');
            if string.find(indexcheck,'"status":404')<0:
                        print 'dropping index '+reportindex;
                        os.system('curl -XDELETE \'localhost:9200/'+reportindex+'\'');
                        
            creatstatus=commands.getoutput('curl -XPUT \'localhost:9200/_river/'+reportindex+'/_meta\' -d \'{"type" : "jdbc","jdbc" : {"strategy": "simple","driver" : "com.mysql.jdbc.Driver", "url" : "'+reportjdbc+'","user" : "'+reportuser+'","password" : "'+reportpasswd+'","index" : "'+reportindex+'","autocommit" : true,"sql" : "'+reportsql+'"}}\'');
            print creatstatus;
            if string.find(creatstatus,'"created":true'):
                oracon.execSql(oraconn,' begin update OCULUSELASTICSEARCH set syncdate=sysdate where table_id='+str(reporttableid)+'; commit; end;');
        datrec.close();
        oraconn.close();
    except:
        print 'Error reported: '+str(sys.exc_info()[1]);

    
def main():
   # while (1):
    loadelasticsearch();
    #    time.sleep(3600);

if __name__ == '__main__':
    main();    