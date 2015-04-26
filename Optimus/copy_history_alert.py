#!/usr/bin/env python

import oracleconnect as oracon;
import os;
import time;
import string;
import datetime;
import commands;

def checkcreatehdfsfolder(datatype,monthyr):
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=MKDIRS&user.name=hadoop"');
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+monthyr+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+monthyr+'?op=MKDIRS&user.name=hadoop" ');
    

def alert():
    montharr=[['may2011','MAY2011'],['april2011','APR2011'],['march2011','MAR2011'],['february2011','FEB2011'],['january2011','JAN2011'],['december2010','DEC2010']];
    
    for mnth in montharr:
        month=mnth[0];
        partition=mnth[1];
        log=open('log/alertdnew_'+month+'.log','w');
        log.write('Retreiving data.....for '+month+'\n');
        print ('Retieving available dates .for '+month);
        starttime=time.time();
        constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
        oraconn=oracon.openconnect(constr);
        sqlstmt='SELECT DISTINCT TRUNC(DATADATE) DATADATE,to_char(datadate,\'YYYYMMDD\') yrmnth FROM DATASTORE.ALERTDNEW partition(ALERTDNEW_'+partition+') order by TRUNC(DATADATE)';
        datrows=oracon.execSql(oraconn,sqlstmt);
        dbloadfl=open('sql/vertica_'+month+'.sql','w');
        vertflsql='sql/vertica_'+month+'.sql';
        endtime=time.time();
        print ('Retieving available dates..done..'+str(endtime-starttime));
        
            
        for datr in datrows:
            print ("Processing "+str(datr[1])+'..........');
            sqlstmt='SELECT INSERVSERIAL, to_char(DATADATE,\'YYYY-MM-DD HH24:MI:SS\'), ID, STATE, MESSAGE_CODE, REPEAT_COUNT, TO_CHAR(TO_TIMESTAMP_TZ(TIME_STRING, \'YYYY-MM-DD HH24:MI:SS TZD\')';
            sqlstmt+=',\'YYYY-MM-DD HH24:MI:SS \')||\'PST\' TIME_OF_OCCURANCE, SEVERITY, TYPE_STRING, COMPONENT, MESSAGE, FILEID FROM DATASTORE.ALERTDNEW partition(ALERTDNEW_'+partition+') WHERE TIME_STRING IS NOT NULL'; 
            sqlstmt+=' and trim(TIME_STRING) like \'20%\'  AND TO_CHAR(DATADATE,\'YYYYMMDD\')=\''+str(datr[1])+'\'';
           
            resultrec=oracon.execSql(oraconn,sqlstmt);
            log.write('Got data..... for '+str(datr[1])+'\n');
            print 'Got data..... for '+str(datr[1]);
        
            log.flush();
            datarray=[];
            ctr=0;
            datatype='alertdnew';
            fctr=0;
            outfl=open('data/alertdnew_'+str(datr[1]),'w');
            filename='alertdnew_'+str(datr[1]);
            log.write('writing to data/alertdnew_'+str(datr[1])+'\n');
            ctr=0;
            try:
                for rec in resultrec:
                    i=0;
                    datarec=[];
                    for r in rec:
                        datarec.append(str(r));
                        i+=1;
                    datarray.append(string.join(datarec,'|'));
                    ctr+=1;
            except:
                log.write("Error : "+str(sys.exc_info()[1])+"\n");
                break;
            
            outfl.write(string.join(datarray,'\n'));
            log.write('written to data/alertdnew_'+str(datr[1])+' :'+str(ctr)+'\n');
            outfl.close();
            checkcreatehdfsfolder(datatype,month);
            os.system(' curl -v -X PUT -T data/'+filename+' "http://callhomelab-17:50075/webhdfs/v1/'+datatype+'/'+month+'/'+filename+'?op=CREATE&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004&overwrite=true" >> log/alertdnew_'+month+'.log' )
            log.write('Finished writing alertdnew_'+str(datr[1])+' to HDFS\n');
            log.flush();
            dbloadfl.write('copy datastore.'+datatype+' SOURCE Hdfs(url=\'http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+month+'/'+filename+'\',username=\'hadoop\') ABORT ON ERROR;\n');
            resultrec.close();
            os.system('rm -rf data/alertdnew_'+str(datr[1]));
    
        oraconn.close();
        dbloadfl.close();
        log.write('Executing Vertica Script for '+vertflsql+'\n');
        os.system('/opt/vertica/bin/vsql -f '+vertflsql+' -U dbadmin -w c@llhome -h callhomelab-vertica01 callhomedb  >> /root/proc/log/alertdnew_'+month+'.sql.log');
    
        log.close();
    
def main():
    alert();
    
    
if __name__ == '__main__':
    main();    
