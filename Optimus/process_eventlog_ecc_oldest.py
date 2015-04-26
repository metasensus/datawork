#!/usr/bin/env python
#!/usr/bin/env python
from multiprocessing import Process;
import string;
import oracleconnect as oracon;
import os;
import sys;
import time;
import eventloglib as evt;
import commands;
import vertica_connect as vconn;


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

def parsedata(dat):
    datArray=string.split(dat,' ');
    newdt='';
    spaceflag=0;
    tabApplied=0;
    for dt in datArray:
        if len(string.strip(dt))> 0:
            newdt+=dt+'\t';
            spaceflag=0;
            tabApplied=0;
        elif len(string.strip(dt))==0:
            if spaceflag == 1:
                if tabApplied==0:
                    newdt+='\t';
                    tabApplied=1;
            spaceflag=1;
    
def checkinvertica(fileid):
    try:
        conn=vconn.vertica_connect('callhomelab-vertica01',5433,'dbadmin','c@llhome','callhomedb');
        sqlstmt='select count(1) from datastore.eventlog where fileid='+str(fileid);
        
        vdat=vconn.vertica_sql_execute(conn,sqlstmt);
        datres=vdat.fetchall();
        for datrec in datres:
            reccount=datrec[0];
        vdat.close();
        conn.close();
        return reccount;
    except:
        print("Error reported: "+str(sys.exc_info()[1]));

def processdate(datatype,yymm):
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');
    logtime=time.strftime('%Y%m%d%H%M%S');
    hwlog = open('log/'+datatype+'_processing_'+yymm+'_'+logtime+'.log','a');
    hwlog.write( "#####################################################\n");
    hwlog.write( " Starting "+datatype+'_'+yymm+"\n");
    hwlog.write( "#####################################################\n");
    hwlog.write( currtime+':'+"Creating HDFS directory for "+datatype+" ("+yymm+") ........\n");
    
    loadlog = open('log/'+datatype+'_loading_'+yymm+'_'+logtime+'.log','a');
     
    sqlstmt='select INSERVSERIAL,YYMMDD, FILEPATH, FILE_NAME,STATS_FILEID from vw_'+datatype+'_current_ecc where yymmdd=\''+yymm+'\' and file_name like \'%debug%\'  order by inservserial desc';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');
    hwlog.write(currtime+':'+"Collecting files from DB");
         
    resultrec=oracon.execSql(oraconn,sqlstmt);
    dbloadfl=open('sql/'+datatype+'_'+str(yymm)+'_'+logtime+'.sql','w');
    vertflsql='sql/'+datatype+'_'+str(yymm)+'_'+logtime+'.sql';
    ctr=0;
    fctr=0;
    totfiles=0;
    maxthread=100;
    previnserv=0;
    filectr=0;
    nctr=1;
    hdfsctr=4;
    for rec in resultrec:
        inserv=rec[0];
        if previnserv < inserv:
            if previnserv>0:
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+'Records for the inserv '+str(previnserv)+' : '+str(filectr));
                hwlog.flush();
            previnserv=inserv;
            filectr=0;
        yymmdd=rec[1];
        filenamepath=rec[2];
        filename=rec[3];
        fileid=rec[4];
        
        try:
            checkcreatehdfsfolder(datatype,str(yymmdd),inserv);
            flstatus=commands.getoutput('curl -v -X GET "http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'.'+str(inserv)+'?op=GETFILESTATUS&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004"')
            if string.find(flstatus,'FileNotFoundException')>0:
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+"Processing file :"+filenamepath+"\n");
                hwlog.flush();
                filectr=filectr+1;
                evt.eventlog_readfile(filenamepath,fileid,inserv,filename);
                filename=rec[3]+'.'+str(inserv);
                if hdfsctr >= 9:
                    hdfsctr=4;
                hdfsnode='callhomelab-0'+str(hdfsctr);
                if hdfsnode == 'callhomelab-09':
                    hdfsctr=4;
                    hdfsnode='callhomelab-0'+str(hdfsctr);
                os.system(' curl -v -X PUT -T /odsarchstg/eventlog/new/'+filename+' "http://'+hdfsnode+':50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'?op=CREATE&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004&overwrite=true"')
                  
                #os.system(' curl -v -X PUT -T /odsarchstg/eventlog/new/'+filename+' "http://callhomelab-17:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'?op=CREATE&user.name=hadoop&namenoderpcaddress=callhomelab-01:9004&overwrite=true"')
                os.system('rm -rf /odsarchstg/eventlog/new/'+filename);
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+"Finished writing "+filenamepath+" to HDFS\n");
                hdfsctr+=1;
    
            if checkinvertica(fileid) == 0:
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+"File not found in vertica "+filename+" "+str(fileid)+'\n');
                filename=rec[3]+'.'+str(inserv);
                dbloadfl.write('copy datastore.'+datatype+' SOURCE Hdfs(url=\'http://callhomelab-01:50075/webhdfs/v1/'+datatype+'/'+string.strip(str(yymmdd))+'/'+string.strip(str(inserv))+'/'+filename+'\',username=\'hadoop\') ABORT ON ERROR;\n');
                dbloadfl.flush();
                ctr+=1;    
            hwlog.flush();
            
            if ctr > 500:
                dbloadfl.close();
                time.sleep(60);
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+'Done 1000 files loading to vertica');
                os.system('mv '+vertflsql+' '+vertflsql+'.run');
                
                if nctr > 3:
                    nctr=1;
                verticanode='callhomelab-vertica0'+str(nctr);
                if verticanode=='callhomelab-vertica04':
                    verticanode=='callhomelab-vertica01';
                    nctr=1;
                os.system('/opt/vertica/bin/vsql -f '+vertflsql+'.run -U dbadmin -w c@llhome -h '+verticanode+' callhomedb &');
                loadlog.write( currtime+':'+'/opt/vertica/bin/vsql -f '+vertflsql+'.run -U dbadmin -w c@llhome -h '+verticanode+' callhomedb\n');
                loadlog.write( currtime+':'+loading+'\n');
                loadlog.flush();
                
                currtime=time.strftime('%Y/%m/%d %H:%M:%S');
                hwlog.write(currtime+':'+'Done loading to vertica\n');
                nctr+=1;
                
                ctr=0;
                logtime=time.strftime('%Y%m%d%H%M%S');
                dbloadfl=open('sql/'+datatype+'_'+str(yymm)+'_'+logtime+'.sql','w');
                vertflsql='sql/'+datatype+'_'+str(yymm)+'_'+logtime+'.sql';
                hwlog.flush();
                
            totfiles+=1;
        except:
            currtime=time.strftime('%Y/%m/%d %H:%M:%S');
            hwlog.write( currtime+':'+"Error : "+str(sys.exc_info()[1])+"\n");
            break;
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');    
    hwlog.write(currtime+':'+'Files for '+str(yymm)+': are '+str(totfiles)+'\n');
    totfiles=0;
    hwlog.flush();

    dbloadfl.close();
    time.sleep(60);
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');
    hwlog.write(currtime+':'+'Executing Vertica Script for '+vertflsql+'\n');
    os.system('mv '+vertflsql+' '+vertflsql+'.run');
                
    if nctr > 3:
        nctr=1;
    verticanode='callhomelab-vertica0'+str(nctr);
    if verticanode=='callhomelab-vertica04':
        verticanode=='callhomelab-vertica01';
        nctr=1;
    loadlog.write( currtime+':'+'/opt/vertica/bin/vsql -f '+vertflsql+'.run -U dbadmin -w c@llhome -h '+verticanode+' callhomedb\n');
    os.system('/opt/vertica/bin/vsql -f '+vertflsql+'.run -U dbadmin -w c@llhome -h '+verticanode+' callhomedb &');
    loadlog.flush();
    
    os.system('mv '+vertflsql+'.run '+vertflsql+'.bak');
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');
    hwlog.write(currtime+':'+'Done loading to vertica\n');
    resultrec.close();
    currtime=time.strftime('%Y/%m/%d %H:%M:%S');
    hwlog.write(currtime+':'+"Done processing "+str(yymm));
    hwlog.close();
    loadlog.close();
    oraconn.close();

def processhwdata(datatype):
    dat=time.strftime('%Y%m%d');
    #hwlog.close();
    #createHdfsDir(datatype,'log/'+datatype+'_processing'+dat+'.log');
    #hwlog = open('log/'+datatype+'_processing'+dat+'.log','a');
    sqlstmt='select distinct YYMMDD from vw_'+datatype+'_current_ecc order by yymmdd';
    constr='ods/ods@callhomeods.3pardata.com:1521/callhomeods';
    oraconn=oracon.openconnect(constr);   
    yyres=oracon.execSql(oraconn,sqlstmt);
    numdays =0;
    maxthread=10;
    for yrec in yyres:
        p=Process(target=processdate,args=(datatype,yrec[0],))
        #print datatype;
        #print yrec[0];
        #processdate(datatype,yrec[0]);
        p.start();
        #numdays+=1;
        #if numdays > 2:
        #    time.sleep(1200);
        #    numdays=0;
        numproc = commands.getoutput("ps -ef | grep [p]rocess_eventlog_ecc |wc -l");
        while int(numproc) > maxthread:
            time.sleep(600);
            numproc = commands.getoutput("ps -ef | grep [p]rocess_eventlog_ecc |wc -l");
    yyres.close();
    oraconn.close();

def copyHw():
    datalist=['eventlog'];
    for datatype in datalist:
        print 'Starting '+datatype+' ....'
        processhwdata(datatype);
        #p.daemon = True;
      
def main():
    while (1):
        copyHw();
        time.sleep(86400);
    
if __name__ == '__main__':
    main();    

