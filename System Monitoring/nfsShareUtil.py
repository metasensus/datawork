#!/usr/bin/env python
import subprocess;
import commands;
import string;
import oracleconnect as oracon;
import time;
import statslog;
import sys;

constr='ods/ods@callhomeods:1521/callhomeods';

def nfsShareStatus():
    try:
        newdiskdt=[];
        newnodedt=[]
        diskArr=[];
        nodeArr=[];
        
        diskdt=subprocess.check_output("df -h|grep share", shell=True).split('\n');
        nodedt=subprocess.check_output("df -i|grep share", shell=True).split('\n');
        
        for dt in diskdt:
            if len(dt)>4:
                strarr=dt.split(' ');
                #print "String Array is:" +str(strarr);
                newdt=[];
                for st in strarr:
                    if st !='':
                        newdt.append(st);            
                newdiskdt.append(string.join(newdt,' '));
            
        for nt in nodedt:
            if len(nt)>4:
                strarr=nt.split(' ');
                #print "String Array is:" +str(strarr);
                newndt=[];
                for st in strarr:
                    if st !='':
                        newndt.append(st);            
                newnodedt.append(string.join(newndt,' '));
                
        for line in newdiskdt:
            mntpoints = line.split(' ');
            if len(mntpoints) > 4:
                mntpoint=mntpoints[4].replace('/share/', '');
                diskutil=mntpoints[3].strip('%');
                groupid=time.strftime('%Y%m%d%H%M');
                sqlstmt=' insert /*+ append +*/ into omi_sharest_disk_status (MOUNTNAME, STATUS_POST_TIME, DISKSPACE_UTILIZATION, SNAPSHOT_GROUPID) values ';
                sqlstmt+='(\''+str(mntpoint)+'\',sysdate,\''+str(diskutil)+'\',\''+str(groupid)+'\');';
                diskArr.append(sqlstmt);
        
        for line in newnodedt:
            mntpoints = line.split(' ');
            if len(mntpoints) >4:
                mntpoint=mntpoints[4].replace('/share/', '');
                inodeutil=mntpoints[3].strip('%');
                groupid=time.strftime('%Y%m%d%H%M');
                sqlstmt=' insert /*+ append +*/ into omi_sharest_inode_status (MOUNTNAME, STATUS_POST_TIME, INODE_UTILIZATION, SNAPSHOT_GROUPID) values ';
                sqlstmt+='(\''+str(mntpoint)+'\',sysdate,\''+str(inodeutil)+'\',\''+str(groupid)+'\');';
                nodeArr.append(sqlstmt);

        oraconn=oracon.openconnect(constr);
        sqld='begin\n'+string.join(diskArr,'\n')+'\n'+'commit; end;'
        oracon.execSql(oraconn,sqld);
        sqln='begin\n'+string.join(nodeArr,'\n')+'\n'+'commit; end;'
        oracon.execSql(oraconn,sqln);
        oraconn.close();
            
    except:
        fl=statslog.logcreate('log/sharestutil.log');
        statslog.logwrite(fl,'Error reported: '+str(sys.exc_info()[1]))
def main():
    while (1):
        nfsShareStatus(); 
        time.sleep(2*3600);

if __name__ == '__main__':
    main();