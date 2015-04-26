#!/usr/bin/env python
import string;
import fileinput;
import time;

def splitattachdata(msgarr):
    hostip='\''+msgarr[0]+'\'';
    unk1='\''+msgarr[1]+'\'';
    unk2='\''+msgarr[2]+'\'';
    datadate='\''+string.strip(string.replace(msgarr[3],'[',''))+'\'';
    tzoffset='\''+string.strip(string.replace(msgarr[4],']',''))+'\'';
    if len(msgarr) == 13:
        connectstring='\''+string.replace(string.replace(msgarr[5],'"',''),'&',';')+' '+msgarr[6]+' '+string.replace(string.replace(msgarr[7],'"',''),'&',';')+'\'';
        httpconnectstatus='\''+msgarr[8]+'\'';
        connectinterval='\''+msgarr[9]+'\'';
        unk3='\''+msgarr[10]+'\'';
        axedavers='\''+string.replace(msgarr[11],'"','')+' '+string.replace(msgarr[12],'"','')+'\'';
    else:
        connectstring='\''+string.replace(msgarr[5],'"','')+' '+string.replace(msgarr[6],'"','')+'\'';
        httpconnectstatus='\''+msgarr[7]+'\'';
        connectinterval='\''+msgarr[8]+'\'';
        unk3='\''+msgarr[9]+'\'';
        axedavers='\''+string.replace(msgarr[10],'"','')+'\'';
    return hostip+','+unk1+','+unk2+','+datadate+','+tzoffset+','+connectstring+','+httpconnectstatus+','+connectinterval+','+unk3+','+axedavers;
    
    
def accesslog():
    timestr=time.strftime('%Y%m%d%H%M%S');
    outfl=open('/tmp/sql/accelogload'+str(timestr)+'.sql','w');
    #outfl=open('sql/accelogload'+str(timestr)+'.sql','w');
    dataArr=[];
    ctr=0;
    for line in fileinput.input():
        dataStmt='Insert into accesslogs(hostip,unk1,unk2,datadate,tzoffset,connectstring,httpconnectstatus,connectinterval,unk3,axedavers) values('
        msgarr=string.split(string.replace(line,'&',';'));
        dataStmt+=splitattachdata(msgarr)+');';
        dataArr.append(dataStmt);
        ctr=ctr+1;
        if ctr > 1000:
            outfl.write(string.join(dataArr,'\n')+'\ncommit;\n');
            outfl.close();
            ctr=0;
            timestr=time.strftime('%Y%m%d%H%M%S');
            outfl=open('/tmp/sql/accelogload'+str(timestr)+'.sql','w');
            dataArr=[];
    if len(dataArr) > 0:
        outfl.write(string.join(dataArr,'\n')+'\ncommit;\n');
        outfl.close();
        
def main():
    accesslog();
    
if __name__ == '__main__':
    main();    
