#!/usr/bin/env python
import string;
import sys;

# Output structure Inserv,datadate,node,sequence,eventclass,severity,eventtype,component,message

def check_ln(ln):
    checkarr=['sw','hw','undefined','port:','remote_copy_link:','cli','system_manager','node','ld:'];
    for chk in checkarr:
        if string.find(ln,chk)>=0:
            return 1;
    return 0;

def eventlog_debug(line):
    #print line;
    linearr=string.split(line,'\t');
    datadate=linearr[0]+' '+linearr[1]+' '+linearr[2];
    node=linearr[3];
    sequence=linearr[4];
    eventclass=linearr[5];
    severity=linearr[6];
    if string.strip(linearr[7])==0:
        ctr=8;
        current_stop=8;
        eventtype='';
        wordcount=0;
    else:    
        ctr=7;
        current_stop=7;
        eventtype='';
        wordcount=0;
    for ln in linearr:
      #  print ln;
       # print wordcount;
        if wordcount < current_stop:
            wordcount += 1;    
        else:
            if check_ln(ln) == 1: 
                break;
            else:
                #print ln;
                eventtype+=ln+' ';
                ctr+=1;
    #print eventtype;        
    component=string.strip(linearr[ctr]);
    #print component;
    message='';
    wordcount=0;
    current_stop=ctr+1;
    for ln in linearr:
        if wordcount < current_stop:
            wordcount += 1;
        else:
            message+=ln+' ';
    #print line;        
    #print datadate+'|'+node+'|'+sequence+'|'+eventclass+'|'+severity+'|'+eventtype+'|'+component+'|'+message;       
    return string.strip(datadate)+'|'+string.strip(node)+'|'+string.strip(sequence)+'|'+string.strip(eventclass)+'|'+string.strip(severity)+'|'+string.strip(eventtype)+'|'+string.strip(component)+'|'+string.strip(message);


def eventlog_internal_comm(line):
    #print line;
    linearr=string.split(line,'\t');
    datadate=linearr[0]+' '+linearr[1]+' '+linearr[2];
    node=linearr[3];
    sequence=linearr[4];
    eventclass=linearr[5]+' '+linearr[6];
    severity=linearr[7];
    if string.strip(linearr[8])==0:
        ctr=9;
        current_stop=9;
        eventtype='';
        wordcount=0;
    else:    
        ctr=8;
        current_stop=8;
        eventtype='';
        wordcount=0;
    for ln in linearr:
      #  print ln;
       # print wordcount;
        if wordcount < current_stop:
            wordcount += 1;    
        else:
            if string.find(ln,'sw')>=0 or string.find(ln,'hw')>=0 or string.find(ln,'undefined')>=0 or string.find(ln,'Comp:')>0:
                break;
            else:
                #print ln;
                eventtype+=ln+' ';
                ctr+=1;
    #print eventtype;        
    component=string.strip(linearr[ctr]);
    if string.find(component,'Comp:')>0:
        component+=string.strip(linearr[ctr+1]);
    #print component;
    message='';
    wordcount=0;
    current_stop=ctr+2;
    for ln in linearr:
        if wordcount < current_stop:
            wordcount += 1;
        else:
            message+=ln+' ';
    #print line;        
    #print datadate+'|'+node+'|'+sequence+'|'+eventclass+'|'+severity+'|'+eventtype+'|'+component+'|'+message;       
    return string.strip(datadate)+'|'+string.strip(node)+'|'+string.strip(sequence)+'|'+string.strip(eventclass)+'|'+string.strip(severity)+'|'+string.strip(eventtype)+'|'+string.strip(component)+'|'+string.strip(message);

def eventlog_readfile(filename,fileid,inservserial,filenm):
    fl=open(filename);
    flnArr=string.split(filename,'/');
    inserv=inservserial;
    lines=fl.readlines();
    outflile='/odsarchstg/eventlog/new/'+filenm+'.'+str(inserv);
    outfl=open(outflile,'w');
    datarr=[];
    startproc=0;
    for line in lines:
        try:
            if string.find(line,'- showeventlog -d -debug -oneline -')>0:
                startproc=1;
            if startproc==1:
                lineArr=string.split(line);
                transposArr=[];
                
                for ln in lineArr:
                    if len(string.strip(ln)) > 0:
                        transposArr.append(string.strip(ln));
                #print transposArr;
                
                #print transposArr[5]
                if transposArr[5]=='Debug':
                    datarr.append(str(inserv)+'|'+eventlog_debug(string.join(transposArr,'\t'))+'|'+str(fileid));
                    #break
                if transposArr[5]=='Notification':
                    datarr.append(str(inserv)+'|'+eventlog_debug(string.join(transposArr,'\t'))+'|'+str(fileid));
                if transposArr[5]=='Internal':
                    datarr.append(str(inserv)+'|'+eventlog_internal_comm(string.join(transposArr,'\t'))+'|'+str(fileid));
                if transposArr[5]=='Status':
                    datarr.append(str(inserv)+'|'+eventlog_internal_comm(string.join(transposArr,'\t'))+'|'+str(fileid));
        except:
            logfl=open('log/'+filenm+'.'+str(inserv)+'.log','a');
    
            logfl.write(line+'\n');
            logfl.write("Error reported: "+str(sys.exc_info()[1])+"\n");
            #break;
            logfl.flush();
            logfl.close();        
    outfl.write(string.join(datarr,'\n'));
    outfl.close();