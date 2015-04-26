#!/usr/bin/env python
import oracleconnect as oracon;
import mysqlconnect as lconnect;
import string;
import hostlib;
import sys;
from multiprocessing import Process;
import time;
import os;
import statslog;
import commands;



constr='ods/ods@callhomeods:1521/callhomeods';
debug='False'

def checkmakedir(dname,fname):
    try:
        d=os.path.dirname(dname);
        if not os.path.exists(d):
            os.makedirs(d);
    except:
        fl=statslog.logcreate(fname);
        statslog.logwrite(fl,"Error reported (create dir): "+str(sys.exc_info()[1]));
    return;
    
def splitFile(fileid,filename,filepath,datatype,inservserial,datafolder,machineid):
    lsconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
    lscurr = lsconn.cursor();
    try:
        # Setting the path to the individual inserv
        splitfilepath=datafolder+'/'+str(inservserial)+'/';
     #   statslog.logwrite(fl,'Processing file ..'+ filename);
    
        inputfile = filepath+'/'+filename;
     #   statslog.logwrite(fl,'Setup input file......'+inputfile);
        srcfl=open(inputfile);
     #   statslog.logwrite(fl,'Reading file ..'+ filename);
        readalllines=srcfl.readlines();
        srcfl.close;
        sqlstmt ='select SPLIT_FILE_TYPE, SPLIT_FILE_SEARCH_TAG, SPLIT_FILE_SKIP_LINES, ifnull(SPLIT_FILE_END_TAG,\'          \') SPLIT_FILE_END_TAG,';
        sqlstmt +='SPLIT_FILE_LINE_SEPERATOR,STATSID from STAT_SPLIT_FILE_LOOKUP where datatype=\''+datatype+'\'';
        
     #   statslog.logwrite(fl,'Reading file Mysql structure for the input file...');
     #   statslog.logwrite(fl,sqlstmt);
        lscurr.execute(sqlstmt);
     #   statslog.logwrite(fl,'Received structure...');
        filenameList=filename.split('.');
        splitfllist=list();
        i=0;
        for splt in filenameList:
            if i>0:
                splitfllist.append(splt);
            i+=1;
        currlinenum =0
        
        readfileln=readalllines;
     #   statslog.logwrite(fl,'Reading os version...');
        for ln in readfileln:
            if string.find(ln,'Release version')>=0:
                osver = string.strip(ln[15:len(ln)]);
      #  statslog.logwrite(fl,'Looping through the structure...');
        datrec=lscurr.fetchall();
        for dat in datrec:
            currspllist=list();
            readline =0;
            readfileln=readalllines;
            cpstat=0;
            for ln in readfileln:
                if string.find(ln,dat[1])>=0:
                    if readline == 0:
                        spflnm=dat[0]+'.'+str(inservserial)+'.'+string.join(splitfllist,'.');
                    cpstat = 1;
                if cpstat == 1:
                    currspllist.append(ln);
                    readline+=1;
                            
                    if string.strip(ln[0:len(dat[3])])==string.strip(dat[3]):
                        cpstat=0;
                        break;
                    if not ln:
                        print 'ln is null'
                        if currlinenum >= len(readfileln):
                            cpstat=0;
                            break;
                        nextstr=readfileln[currlinenum+1];
                        print 'nexstr:'+nextstr;
                        if not nextstr or string.strip(nextstr[0:len(dat[3])])== string.strip(dat[3]) or len(string.strip(nextstr)) == 0 or string.find(nextstr,'<') >=0 :
                            cpstat=0;
                            break;
                        
                    currlinenum+=1;
                    
            splafterskip=list();
            currline=0;
            splafterskip.append(osver+'\n');
            if len(currspllist)>0:
                # statslog.logwrite(fl,'Creating directory......'+splitfilepath); 
                checkmakedir(splitfilepath,'log/splitter_'+filename+'.log');
                # statslog.logwrite(fl,'Writing file ....'+splitfilepath+'/'+spflnm); 
                spfl = open(splitfilepath+'/'+spflnm,'w');
                
                for curr in currspllist:
                    if currline >= dat[2] and currline < len(currspllist)-1 :
                        if len(string.strip(curr))>0:
                            if string.find(string.strip(curr),'-----') == -1 :
                                if curr:
                                    splafterskip.append(curr);
                    currline+=1;
                spfl.write(string.join(splafterskip));
                spfl.close();
                #sqlstmt='begin dataload.addsplitfile(inputfileid=>'+str(fileid)+',splitfiletype=>\''+lkp[0]+'\',splitfilename=>\''+spflnm+'\'';
                #sqlstmt+=',splitfilepath=>\''+splitfilepath+'\',statsid=>'+str(lkp[5])+'); end;';
                
                sqlstmt='INSERT INTO STATS_SPLIT_FILES(STATS_FILEID, STATS_SPLITFILE_NAME, STATS_SPLITFILE_PATH, STATSID, STATS_SPLIT_FILE_TYPE) values ';
                sqlstmt+='('+str(fileid)+',\''+spflnm+'\',\''+splitfilepath+'\','+str(dat[5])+',\''+dat[0]+'\')';
                # statslog.logwrite(fl,sqlstmt);
                lscurr.execute(sqlstmt);
    
        sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=1 where STATS_FILEID='+str(fileid);
        lscurr.execute(sqlstmt);
        lscurr.close();
        lsconn.close();
        #os.remove('log/splitter_'+filename+'.log');
    except:
        fl=statslog.logcreate('log/splitter_'+filename+'.log');
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
        sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=3 where STATS_FILEID='+str(fileid);
        lscurr.execute(sqlstmt);
        sqlstmt='delete from STATS_SPLIT_FILES where STATS_FILEID='+str(fileid);
        lscurr.execute(sqlstmt);
        lscurr.close();
        lsconn.close();
        
def splitter():
    oraconn=oracon.openconnect(constr);
    lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
    lcurr = lconn.cursor();
    fl=statslog.logcreate('log/splitter.log');
    
    try:
        statslog.logwrite(fl,'Getting Host IP...\n');
        ipadd=hostlib.retHostIP();
        statslog.logwrite(fl,'Getting number of threads...\n');
        sqlstmt='select machineid,numberofthreads,dataprocessing_folder,splitterloc from statsprocessingmachine where ipaddress=\''+ipadd+'\' and enable =1';
        numthreadrec=oracon.execSql(oraconn,sqlstmt);
        for numthread in numthreadrec:
            numthreads = numthread[1];
            machineid=numthread[0];
            datafolder=numthread[3];
            
        numsplitterthreads =int(numthreads/2);
        statslog.logwrite(fl,'Getting number of datatypes...\n');
        sqlstmt='select distinct datatype from stat_split_file_lookup';
        datatyprec=oracon.execSql(oraconn,sqlstmt);
        dtct=0;
        for dt in datatyprec:
            dtct+=1;
        numsplitthread=int(numsplitterthreads/dtct);
        datatyprec=oracon.execSql(oraconn,sqlstmt);
        
        lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
        lcurr=lconn.cursor();
        
        for dtype in datatyprec:
            sqlstmt='SELECT COUNT(1) FROM STATSPROCESSTRANSACT WHERE FILE_PROCESS_STATUS=0 and DATATYPE=\''+dtype[0]+'\'';
            lcurr.execute(sqlstmt);
            reccount=lcurr.fetchall();
            numInMySql=0;
            for rec in reccount:
                numInMySql=rec[0];
            numsplitterthreads=numsplitthread - numInMySql;
            if numsplitterthreads > 0:
                statslog.logwrite(fl,'Processing for... :'+dtype[0]);
                sqlstmt='SELECT STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE FROM STATSPROCESSTRANSACT ';
                sqlstmt+='WHERE FILE_PROCESS_STATUS=0 AND MACHINEID='+str(machineid);
                sqlstmt+=' AND ROWNUM <='+str(numsplitterthreads)+' AND DATATYPE= \''+dtype[0]+'\' ORDER BY STATS_FILEID';
                filerec=oracon.execSql(oraconn,sqlstmt);
            
                statslog.logwrite(fl,'Inserting data into mysql...\n');
                sqlstmt='INSERT INTO STATSPROCESSTRANSACT (STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE) VALUES ';
                sql=[];
            
                for flrec in filerec:
                    sql.append('('+str(flrec[0])+',\''+flrec[1]+'\',\''+flrec[2]+'\',\''+flrec[3]+'\')');
                    sqlst='begin Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=2 where STATS_FILEID='+str(flrec[0])+'; commit; end;';
                    oracon.execSql(oraconn,sqlst);
                if len(sql) > 0:
                    sqlstmt=sqlstmt+string.join(sql,',');
                    lcurr.execute(sqlstmt);
        lconn.close();
        oraconn.close();
        lconn = lconnect.connectMysql('ods','procuser','c@llhome','localhost');
        lcurr = lconn.cursor();
        statslog.logwrite(fl,'Querying mysql...');
        sqlstmt='SELECT STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE FROM STATSPROCESSTRANSACT where ifnull(FILE_PROCESS_STATUS,0)=0 LIMIT '+str(numsplitthread)
        lcurr.execute(sqlstmt);    
        ctr =0;
        statrow=lcurr.fetchall();
        pythonRuns=commands.getoutput("ps -ef | grep python | grep -v grep | wc -l");
        if int(pythonRuns) - 3 < numsplitthread:
            residual=(numsplitthread - int(pythonRuns)) +3;
            sqlstmt='SELECT STATS_FILEID,STATS_FILE_NAME,STATS_FILE_PATH,DATATYPE FROM STATSPROCESSTRANSACT where ifnull(FILE_PROCESS_STATUS,0)=0 LIMIT '+str(residual);
            lcurr.execute(sqlstmt);    
            ctr =0;
            statrow=lcurr.fetchall();
            
            for statrec in statrow:
                statslog.logwrite(fl,'Splitting file...:'+statrec[1]);
                flpthlist=string.split(statrec[2],'/');
                inservserial=flpthlist[len(flpthlist)-2];
                statslog.logwrite(fl,'Starting process for ... '+statrec[1]);
                p=Process(target=splitFile,args=(statrec[0],statrec[1],statrec[2],statrec[3],inservserial,datafolder+'/splitter',machineid,));
                p.daemon = True;
                p.start();
                
                sqlstmt='Update STATSPROCESSTRANSACT set FILE_PROCESS_STATUS=2 where STATS_FILEID='+str(statrec[0]);
                lcurr.execute(sqlstmt);  
                ctr+=1;
        lcurr.close();
        lconn.close();
        os.remove('log/splitter.log');
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]));
    
def getcolStruct(stats_structureid,stats_single_row,stats_end_row,logfile):
    oraconn=oracon.openconnect(constr);
    colstartpos=list();
    colendpos=list();
    lastcol=list();
    colsearchstr=list();
    colsize=list();
    coltype=list();
    statslog.logwrite(logfile,'Generating columns...','getcolStruct')
    sqlstmt='select stats_column_name,stats_column_start_pos,stats_column_end_pos,';
    sqlstmt+=' stats_column_searchstr,stats_column_size,stats_column_type from statsstructuredetail where stats_structure_id='+str(stats_structureid)+' order by stats_column_id';
    structrec=oracon.execSql(oraconn,sqlstmt);
    for struct in structrec:
        colstartpos.append(struct[1]);
        colendpos.append(struct[2]);
        colsearchstr.append(struct[3]);
        colsize.append(struct[4]);
        coltype.append(struct[5]);
        if stats_single_row=='False':
            if stats_end_row == struct[0]:
                lastcol.append(1);
            else:
                lastcol.append(0);
    return colstartpos,colendpos,colsearchstr,colsize,coltype,lastcol

def getstructversion(version,filetype,logfile):
    oraconn=oracon.openconnect(constr);
    statslog.logwrite(logfile,'Generating structure from database...','getstructversion')
    sqlstmt='select stats_structure_id,stats_single_row,stats_end_of_row from statsstructure where statsname=\''+filetype+'\' and stats_version=trim(\''+version[0:5]+'\')';
    statslog.logwrite(logfile,sqlstmt,'getstructversion')
    strec=oracon.execSql(oraconn,sqlstmt);
    columnstruct={};
    strucid=0;
    singlerow='';
    endrow='';
    datarow=list();
    for stre in strec:
        strucid=stre[0];
        singlerow=stre[1];
        endrow=stre[2];
        
    columnstruct=getcolStruct(strucid,singlerow,endrow,logfile);
    statslog.logwrite(logfile,'Got structure now returning...','getstructversion')
    return columnstruct,strucid;

def processdata(ln,i,colstartpos,colendpos,colsearchstr,coltype,logfile):
    try:
        element='';
        k=0;
        #endpos=colendpos;
        startpos=colstartpos;
        endpos=colendpos+startpos;
        scheck=ln[startpos];
        
        
        if startpos>0:
            if string.strip(scheck) != '':
                k=0;
                newpos=colstartpos;
                if string.strip(ln[startpos+1]) == '':
                    newpos=startpos+1;
                    scheck=ln[newpos];
                    while string.strip(scheck) == "" or newpos < colstartpos + colendpos:
                        newpos=newpos+1;
                        scheck=ln[newpos];
                        if string.strip(ln[newpos]) !='':
                            startpos=newpos;
                            break;
                else:
                    while string.strip(scheck) != '':
                        k+=1;
                        startpos=newpos-k;
                        scheck=ln[startpos];

                    startpos=startpos+1;    
            else:
                k=0;
                while string.strip(scheck) == '':
                    if startpos == endpos:
                        break;
                    startpos=colstartpos+k;
                    scheck=ln[startpos];
                    k+=1;
                #startpos=startpos -1;    
 
        endpos=colendpos+startpos;
                
        if endpos > len(ln):
            endpos =len(ln)-1;
        
        if not colsearchstr:
            element=ln[startpos:endpos]; 
        else:
            if string.find(ln,colsearchstr)>=0:
                element=ln[startpos:endpos];
        el='';
        if string.strip(coltype)=='number':
            if len(string.strip(element))==0 or not element:
                element=0;
            else:
                for dat in string.strip(element):
                    el+=dat;
                    if dat==' ':
                        break;
                element=el;
        #print str(i)+':ln['+str(startpos)+':'+str(endpos)+'] :'+str(element);
        return element;
    except:
        statslog.logwrite(logfile,"Error reported: "+str(sys.exc_info()[1]),'processdata');
        
def generateData(lines,filetype,inservserial,datepart,logfile):
    ctr=0;
    datrow=list();
    datastruct={};
    colstartpos=list();
    colendpos=list();
    colsearchstr=list();
    colsize=list();
    coltype=list();
    lastcol=list();
    try:
        for ln in lines:
            j=0;
            if ctr == 0:
                statslog.logwrite(logfile,'Generating structure...','generateData')
                structures={};
                structures=getstructversion(ln,filetype,logfile);
                datastruct=structures[0];
                structid=structures[1];
                statslog.logwrite(logfile,'Got structure...now proceeding','generateData')
            else:
                colstartpos=datastruct[0];
                colendpos=datastruct[1];
                colsearchstr=datastruct[2];
                coltype=datastruct[4];
                if datastruct[3]:
                    colsize=datastruct[3];
                if datastruct[5]:
                    lastcol=datastruct[5];
                dataelement=list();
                i=0;
                j=0;
                dataelement.append(inservserial);
                dataelement.append(datepart);
                lenadj=0;
                if not lastcol:
                    while i<len(colstartpos):
                        element=processdata(ln,i,colstartpos[i],colendpos[i],colsearchstr[i],coltype[i],logfile);
                        dataelement.append(element);
                        i+=1;                
                                    
                    #data=string.join(dataelement,'\t');
                    data='';
                    for dt in dataelement:
                        data+=string.strip(str(dt))+'\t';
                    datrow.append(data);
                else:
                    if len(colsearchstr[j])==0:
                        if string.find(ln,colsearchstr[j])>=0:
                            dataelement.append(ln[colstartpos[i]:colendpos[i]]);
                    else:
                        dataelement.append(ln[colstartpos[j]:colendpos[j]]);    
                    if lastcol[j]==1:
                        datrow.append(string.join(dataelement,'\t'));
                        j=1;
                    j+=1;
            ctr+=1;
        datstr=string.join(datrow,'\n');
        return datstr,structid;
    except:
        statslog.logwrite(logfile,"Error reported: "+str(sys.exc_info()[1]),'generateData');
                
def tabFile(fileid,filepath,filename,filetype,inservserial,datepart,statsfirstrowversion,datafolder):
    oraconn=oracon.openconnect(constr);
    fl=statslog.logcreate('log/tabber_'+str(fileid)+'.log');
    try:
        outdir=datafolder+'/processed/'+str(inservserial)+'/';
        checkmakedir(outdir);
        dtsplit=string.split(datepart,' ');
        dt=string.join(dtsplit,'.');
        outputfilename=filetype+'.'+str(inservserial)+'.'+str(dt)+'.out';
        outfile=open(outdir+outputfilename,'w');
        statslog.logwrite(fl,'Opening file....'+filename)
        infile=open(filepath+'/'+filename);
        lines=infile.readlines();
        infile.close();
        datastruct={};
        if statsfirstrowversion == 'True':
            if len(lines) > 1:
                statslog.logwrite(fl,'Generating data...','tabFile')
                datarowstruct={};
                datarowstruct=generateData(lines,filetype,inservserial,datepart,fl);
                datarow=datarowstruct[0];
                structid=datarowstruct[1];
                outfile.write(datarow);
                sqlstmt='begin dataload.ADDOUTPUTFILE (STATS_SPLITFILE_ID=>'+str(fileid)
                sqlstmt+=',STATS_OUTPUTFILE_NAME=>\''+outputfilename+'\',STATS_OUTPUTFILE_PATH =>\''+outdir+'\',STATS_STRUCTURE_ID=>'+str(structid)+'); end;';
                oracon.execSql(oraconn,sqlstmt);
                sqlstmt='begin Update stats_split_files set FILE_PROCESS_STATUS=1,file_process_date=sysdate where stats_splitfile_id='+str(fileid)+'; commit; end;';
                oracon.execSql(oraconn,sqlstmt);
                statslog.logwrite(fl,'Done processing :'+filename,'tabFile');
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]),'tabFile');
        sqlstmt='begin Update stats_split_files set FILE_PROCESS_STATUS=3 where stats_splitfile_id='+str(fileid)+'; commit; end;';
        oracon.execSql(oraconn,sqlstmt);
        
def tabber():
    oraconn=oracon.openconnect(constr);
    fl=statslog.logcreate('log/parser.log');
    try:
        statslog.logwrite(fl,'Getting structure.....','tabber');
        sqlstmt='select distinct statsname from statsstructure';
        statsrec=oracon.execSql(oraconn,sqlstmt);
    
        statList=[];
        dtct=0;
        for strec in statsrec:
            statList.append(strec[0]);
            
        
        statslog.logwrite(fl,'Getting IP.....','tabber')
        ip=hostlib.retHostIP();
        statslog.logwrite(fl,'Getting machine configuration.....','tabber');    
        sqlstmt='select machineid,numberofthreads,dataprocessing_folder from statsprocessingmachine where ipaddress=\''+ip+'\'';
    
        macrec=oracon.execSql(oraconn,sqlstmt);
        mid=0;
        num_threads=0;
        datafolder='';
        
        for mrec in macrec:
            mid=mrec[0];
            num_threads=mrec[1];
            datafolder=mrec[2];
        
        statslog.logwrite(fl,'Getting files to process.....','tabber');    
        
        sqlstmt='select stats_splitfile_id,stats_splitfile_path,stats_splitfile_name,stats_split_file_type,stats_first_row_version from stats_split_files a,';
        sqlstmt+='(select distinct statsname,stats_first_row_version from statsstructure) b where machineid='+str(mid);
        sqlstmt+=' and stats_split_file_type in (\''+string.join(statList,'\',\'')+'\') and a.stats_split_file_type=b.statsname  and a.File_process_status=0 ';
        sqlstmt+=' AND ROWNUM <='+str(num_threads/2)+' order by stats_splitfile_id';
               
        procfiles=oracon.execSql(oraconn,sqlstmt);
        statslog.logwrite(fl,'Recieved list of files.....','tabber');        
        for procrec in procfiles:
            flpthlist=string.split(procrec[2],'.');
            inservserial=flpthlist[1];
            datepart=str(flpthlist[2])+' '+str(flpthlist[3]);
            statslog.logwrite(fl,'Starting file process for '+procrec[2],'tabber');    
            p=Process(target=tabFile,args=(procrec[0],procrec[1],procrec[2],procrec[3],inservserial,datepart,procrec[4],datafolder,));
            p.daemon = True;
            p.start();
            sqlstmt='begin Update stats_split_files set FILE_PROCESS_STATUS=2 where stats_splitfile_id='+str(procrec[0])+'; commit; end;';
            oracon.execSql(oraconn,sqlstmt);
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]),'tabber');
    
def splitrunner():
    fl=statslog.logcreate('log/splitrunner.log');
    try:
        while (1):
            p=Process(target=splitter);
            p.start();
            time.sleep(30);
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]),'splitrunner');

def parserunner():
    fl=statslog.logcreate('log/parserunner.log');
    try:
        while True:
            p=Process(target=tabber);
            p.start();
            time.sleep(600);
    except:
        statslog.logwrite(fl,"Error reported: "+str(sys.exc_info()[1]),'parserunner');
    
    
    