#!/usr/bin/env python
#!/usr/bin/env python
#!/usr/bin/env python
from multiprocessing import Process;
import string;
import oracleconnect as oracon;
import os;
import sys;
import time;
import commands;

def checkcreatehdfsfolder(datatype,yymmdd,inserv):
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'?op=MKDIRS&user.name=hadoop"');
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'?op=MKDIRS&user.name=hadoop" ');
    flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'/'+str(inserv)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
    if string.find(flstatus,'FileNotFoundException')>0:
        os.system('curl -v -X PUT "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+str(yymmdd)+'/'+str(inserv)+'?op=MKDIRS&user.name=hadoop" &');


def processdate(datatype,yymm):
    hwlog = open('/root/proc/log/'+datatype+'_processing_'+yymm+'.log','w');
    hwlog.write( "#####################################################\n");
    hwlog.write( " Starting "+datatype+'_'+yymm+"\n");
    hwlog.write( "#####################################################\n");
    hwlog.write( "Creating HDFS directory for "+datatype+" ("+yymm+") ........\n");
   
     
    sqlstmt='select INSERVSERIAL, to_char(DATADATE,\'MM/DD/YYYY\'), YYMMDD, FILENAMEPATH, STATS_OUTPUTFILE_NAME, STATS_OUTPUTFILE_ID from vw_'+datatype+'_current where yymmdd=\''+yymm+'\' order by inservserial';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    hwlog.write("Collecting files from DB");
         
    resultrec=oracon.execSql(oraconn,sqlstmt);
    dbloadfl=open('/root/proc/sql/'+datatype+'_'+yymm+'.sql','w');
    vertflsql='/root/proc/sql/'+datatype+'_'+yymm+'.sql';
    ctr=0;
    fctr=0;
    totfiles=0;
    maxthread=50;
    for rec in resultrec:
        inserv=rec[0];
        datadate=rec[1];
        yymmdd=rec[2];
        filenamepath=rec[3];
        filename=rec[4];
        fileid=rec[5];
        
        try:
            datfl=open(filenamepath);
            currdir='/root/proc/data/';
            outfl=open(currdir+filename,'w');
            data=datfl.readlines();
            newdat=[];
            hwlog.write("Processing file :"+filenamepath+"\n");
            hwlog.flush();
            for dt in data:
                orgdt=string.replace(dt,'\n','');
                orgdt=string.replace(orgdt,'\r','');
                newdt=str(inserv)+'\t'+str(datadate)+'\t'+orgdt+'\t'+str(fileid);
                newdt=string.replace(newdt,'\t','|');
                newdat.append(newdt);
                
            outfl.write(string.join(newdat,'\n'));
            outfl.close();
            checkcreatehdfsfolder(datatype,str(yymmdd),inserv);
            
            flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
            if string.find(flstatus,'FileNotFoundException')>0:
                os.system(' curl -v -X PUT -T /root/proc/data/'+filename+' "http://callhomelab-17:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'?op=CREATE&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004&overwrite=true" &')
                hwlog.write("Finished writing "+filenamepath+" to HDFS\n");
                dbloadfl.write('copy datastore.'+datatype+' SOURCE Hdfs(url=\'http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'\',username=\'hadoop\') ABORT ON ERROR;\n');
                os.system('rm -rf /root/proc/data/'+filename);
            hwlog.flush();
            
            if ctr > maxthread:
                hwlog.write('Executing Vertica Script for '+vertflsql+'\n');
                dbloadfl.close();
                os.system('/opt/vertica/bin/vsql -f '+vertflsql+' -U dbadmin -w c@llhome -h callhomelab-vertica01 callhomedb & > /root/proc/log/'+datatype+'_'+yymm+'.sql.log');
                
                
                dbloadfl=open('/root/proc/sql/'+datatype+'_'+yymm+'.sql','w');
                vertflsql='/root/proc/sql/'+datatype+'_'+yymm+'.sql';
                numjobs=int(commands.getoutput('ps -ef | grep curl | wc -l'));
                while (numjobs-1) > maxthread:
                    time.sleep(30);
                    numjobs=int(commands.getoutput('ps -ef | grep curl | wc -l'));
                maxthread=maxthread-(numjobs-1);
                if maxthread < 0:
                    maxthread = 50;
                crt=0;    
            ctr+=1;
            totfiles+=1;
        except:
            hwlog.write( "Error : "+str(sys.exc_info()[1])+"\n");
            break;
    hwlog.write('Files for '+yymm+': are '+str(totfiles)+'\n');
    totfiles=0;
    hwlog.flush();

    dbloadfl.close();
    time.sleep(60);
    hwlog.write('Executing Vertica Script for '+vertflsql+'\n');
    os.system('/opt/vertica/bin/vsql -f '+vertflsql+' -U dbadmin -w c@llhome -h callhomelab-vertica01 callhomedb & > /root/proc/log/'+datatype+'_'+yymm+'.sql.log');
    os.system('rm -rf data/*.'+str(yymm)+'.*');
    resultrec.close();
    hwlog.write("Done processing "+yymm);
    hwlog.close()
    oraconn.close();

def processhwdata(datatype):
    dat=time.strftime('%Y%m%d');
    #hwlog.close();
    #createHdfsDir(datatype,'log/'+datatype+'_processing'+dat+'.log');
    #hwlog = open('log/'+datatype+'_processing'+dat+'.log','a');
    sqlstmt='select distinct YYMMDD from vw_'+datatype+'_current order by yymmdd desc';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    yyres=oracon.execSql(oraconn,sqlstmt);
    numdays =0;
    maxthread=10;
    for yrec in yyres:
        #p=Process(target=processdate,args=(datatype,yrec[0],))
        processdate(datatype,yrec[0]);
        #p.start();
        #numdays+=1;
        #if numdays > 2:
        #    time.sleep(1200);
        #    numdays=0;
            
    yyres.close();
    oraconn.close();

def copyHw():
    datalist=['alertdnew'];
    for datatype in datalist:
        print 'Starting '+datatype+' ....'
        processhwdata(datatype);
        #p.daemon = True;
      
def main():
    while (1):
        copyHw();
        time.sleep(7200);
    
    
if __name__ == '__main__':
    main();    
    
    
    
    



