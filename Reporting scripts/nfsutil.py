import subprocess;
import commands;
import string;
import oracleconnect as oracon;
import time;
import statslog;
import sys;

constr='ods/ods@callhomeods:1521/callhomeods';

def nfsUtildata():
    try:
        newdiskdt=[];
        newnodedt=[]
        diskArr=[];
        nodeArr=[];
        
        diskdt=subprocess.check_output("df -h|grep ods", shell=True).split('\n');
        nodedt=subprocess.check_output("df -i|grep ods", shell=True).split('\n');
        
        for dt in diskdt:
            strarr=dt.split(' ');
            #print "String Array is:" +str(strarr);
            newdt=[];
            for st in strarr:
                if st !='':
                    newdt.append(st);            
            newdiskdt.append(string.join(newdt,' '));
            
        for nt in nodedt:
            strarr=nt.split(' ');
            #print "String Array is:" +str(strarr);
            newndt=[];
            for st in strarr:
                if st !='':
                    newndt.append(st);            
            newnodedt.append(string.join(newndt,' '));
            
        for line in newdiskdt:
            mntpoints = line.split(' ');
            if len(mntpoints) >5:
                mntserver=mntpoints[0];
                mntpoint=mntpoints[5].strip('/');
                diskutil=mntpoints[4].strip('%');
                #dataloc=mntpoint
                #istring=("df -i|grep %s|awk '{print $5}'"%mntpoint);
                #inodeutil=commands.getoutput(istring);
                #sqlstmt=' insert /*+ append +*/ into omi_nfs_status (MOUNTNAME, MOUNTSERVER,STATUS_POST_TIME, DISKSPACE_UTILIZATION,INODE_UTILIZATION) values ';
                #sqlstmt+='(\''+str(mntpoint)+'\',\''+str(mntserver)+'\',sysdate,\''+str(diskutil)+'\',\''+str(inodeutil)+'\');';
                sqlstmt=' insert /*+ append +*/ into omi_nfsdisk_status (MOUNTNAME, MOUNTSERVER,STATUS_POST_TIME, DISKSPACE_UTILIZATION) values ';
                sqlstmt+='(\''+str(mntpoint)+'\',\''+str(mntserver)+'\',sysdate,\''+str(diskutil)+'\');';
                diskArr.append(sqlstmt);
                
        for line in newnodedt:
            mntpoints = line.split(' ');
            if len(mntpoints) >5:
                mntserver=mntpoints[0];
                mntpoint=mntpoints[5].strip('/');
                inodeutil=mntpoints[4].strip('%');
                sqlstmt=' insert /*+ append +*/ into omi_nfsinode_status (MOUNTNAME, MOUNTSERVER,STATUS_POST_TIME, INODE_UTILIZATION) values ';
                sqlstmt+='(\''+str(mntpoint)+'\',\''+str(mntserver)+'\',sysdate,\''+str(inodeutil)+'\');';
                nodeArr.append(sqlstmt);
                
        oraconn=oracon.openconnect(constr);
        sqld='begin\n'+string.join(diskArr,'\n')+'\n'+'commit; end;'
        oracon.execSql(oraconn,sqld);
        sqln='begin\n'+string.join(nodeArr,'\n')+'\n'+'commit; end;'
        oracon.execSql(oraconn,sqln);
        oraconn.close();
            
    except:
        fl=statslog.logcreate('log/nfsutil.log');
        statslog.logwrite(fl,'Error reported: '+str(sys.exc_info()[1]))
def main():
    while (1):
        nfsUtildata(); 
        time.sleep(2*3600);

if __name__ == '__main__':
    main();
