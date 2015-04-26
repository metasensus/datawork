#!/usr/bin/env python
import psutil; 
import re;
import os;
import sys;
import string;
import socket;
import time;

hostname=socket.gethostname();
hostIP=socket.gethostbyname(socket.gethostname());
ver=psutil.version_info;
majorVer=ver[0];

def cpu_monitoring():
    try:
        outflname="/tmp/sql/cpustats_"+ time.strftime('%Y%m%d%H%M%S')+".sql";
        sqlArr=[];
        currentTime=time.strftime('%Y'+'-'+'%m'+'-'+'%d'+' '+'%H'+':'+'%M'+':'+'%S');
        if majorVer < 2:
            numCPUs=psutil.NUM_CPUS;
            percs = psutil.cpu_times_percent(interval=0.5, percpu=False);
            perallcpuuser = str(int(percs.user));
            perallcpunice = str(int(percs.nice));
            perallcpusystem = str(int(percs.system));
            perallcpuidle = str(int(percs.idle));
            perallcpuiowait = str(int(percs.iowait));
            perallcpuirq = str(int(percs.irq));
            perallcpusoftirq = str(int(percs.softirq));
            perallcpusteal = str(int(percs.steal));
            perallcpuguest = 0;
            sqlstmt= 'insert into sm_cpu_stats (HOSTIP,HOSTNAME,CPU_USER_PERCENT,CPU_NICE_PERCENT,CPU_SYSTEM_PERCENT,CPU_IDLE_PERCENT,CPU_IOWAIT_PERCENT,CPU_IRQ_PERCENT,CPU_SOFTIRQ_PERCENT,CPU_STEAL_PERCENT,';
            sqlstmt+='CPU_GUEST_PERCENT,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(perallcpuuser)+'\',\''+str(perallcpunice)+'\',\''+str(perallcpusystem)+'\',\''+str(perallcpuidle)+'\',';
            sqlstmt+='\''+str(perallcpuiowait)+'\',\''+str(perallcpuirq)+'\',\''+str(perallcpusoftirq)+'\',\''+str(perallcpusteal)+'\',\''+str(perallcpuguest)+'\',timestamp \''+str(currentTime)+'\');';
            sqlArr.append(sqlstmt);
            # Individual CPUs
            if numCPUs > 1:
                percs = psutil.cpu_times_percent(interval=0.5, percpu=True)
                for NUM_CPUS, perc in enumerate(percs):
                    percpuuser = str(int(perc.user));
                    percpunice = str(int(perc.nice));
                    percpusystem = str(int(perc.system));
                    percpuidle = str(int(perc.idle));
                    percpuiowait = str(int(perc.iowait));
                    percpuirq = str(int(perc.irq));
                    percpusoftirq = str(int(perc.softirq));
                    percpusteal = str(int(perc.steal));
                    percpuguest = 0;
                    sqlstmt= 'insert into sm_cpu_stats (HOSTIP,HOSTNAME,CPU_USER_PERCENT,CPU_NICE_PERCENT,CPU_SYSTEM_PERCENT,CPU_IDLE_PERCENT,CPU_IOWAIT_PERCENT,CPU_IRQ_PERCENT,CPU_SOFTIRQ_PERCENT,CPU_STEAL_PERCENT,';
                    sqlstmt+='CPU_GUEST_PERCENT,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(percpuuser)+'\',\''+str(percpunice)+'\',\''+str(percpusystem)+'\',\''+str(percpuidle)+'\',';
                    sqlstmt+='\''+str(percpuiowait)+'\',\''+str(percpuirq)+'\',\''+str(percpusoftirq)+'\',\''+str(percpusteal)+'\',\''+str(percpuguest)+'\',timestamp \''+str(currentTime)+'\');';
                    sqlArr.append(sqlstmt);
        else:
            numCPUs=psutil.cpu_count();
            # All CPUs    
            percs = psutil.cpu_times_percent(interval=0.5, percpu=False)
            perallcpuuser = str(int(percs.user));
            perallcpunice = str(int(percs.nice));
            perallcpusystem = str(int(percs.system));
            perallcpuidle = str(int(percs.idle));
            perallcpuiowait = str(int(percs.iowait));
            perallcpuirq = str(int(percs.irq));
            perallcpusoftirq = str(int(percs.softirq));
            perallcpusteal = str(int(percs.steal));
            perallcpuguest = str(int(percs.guest));
            sqlstmt= 'insert into sm_cpu_stats (HOSTIP,HOSTNAME,CPU_USER_PERCENT,CPU_NICE_PERCENT,CPU_SYSTEM_PERCENT,CPU_IDLE_PERCENT,CPU_IOWAIT_PERCENT,CPU_IRQ_PERCENT,CPU_SOFTIRQ_PERCENT,CPU_STEAL_PERCENT,';
            sqlstmt+='CPU_GUEST_PERCENT,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(perallcpuuser)+'\',\''+str(perallcpunice)+'\',\''+str(perallcpusystem)+'\',\''+str(perallcpuidle)+'\',';
            sqlstmt+='\''+str(perallcpuiowait)+'\',\''+str(perallcpuirq)+'\',\''+str(perallcpusoftirq)+'\',\''+str(perallcpusteal)+'\',\''+str(perallcpuguest)+'\',timestamp \''+str(currentTime)+'\');';
            sqlArr.append(sqlstmt);
            # Individual CPUs
            if numCPUs > 1:
                percs = psutil.cpu_times_percent(interval=0.5, percpu=True)
                for cpu_num, perc in enumerate(percs):
                    percpuuser = str(int(perc.user));
                    percpunice = str(int(perc.nice));
                    percpusystem = str(int(perc.system));
                    percpuidle = str(int(perc.idle));
                    percpuiowait = str(int(perc.iowait));
                    percpuirq = str(int(perc.irq));
                    percpusoftirq = str(int(perc.softirq));
                    percpusteal = str(int(perc.steal));
                    percpuguest = str(int(perc.guest));
                    sqlstmt= 'insert into sm_cpu_stats (HOSTIP,HOSTNAME,CPU_USER_PERCENT,CPU_NICE_PERCENT,CPU_SYSTEM_PERCENT,CPU_IDLE_PERCENT,CPU_IOWAIT_PERCENT,CPU_IRQ_PERCENT,CPU_SOFTIRQ_PERCENT,CPU_STEAL_PERCENT,';
                    sqlstmt+='CPU_GUEST_PERCENT,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(percpuuser)+'\',\''+str(percpunice)+'\',\''+str(percpusystem)+'\',\''+str(percpuidle)+'\',';
                    sqlstmt+='\''+str(percpuiowait)+'\',\''+str(percpuirq)+'\',\''+str(percpusoftirq)+'\',\''+str(percpusteal)+'\',\''+str(percpuguest)+'\',timestamp \''+str(currentTime)+'\');';
                    sqlArr.append(sqlstmt);
                    
        statfl = open (outflname,"w");
        statfl.write(string.join(sqlArr,'\n')+'\n'+'commit;');
        statfl.close();
    except:
        pass
    
###
# Memory utilization (all output in K)
###

def memory_util():
    outflname="/tmp/sql/memstats_"+ time.strftime('%Y%m%d%H%M%S')+".sql";
    try:
        currentTime=time.strftime('%Y'+'-'+'%m'+'-'+'%d'+' '+'%H'+':'+'%M'+':'+'%S')
        # memory usage:
        mem = psutil.virtual_memory();
        totalMem=str(int(mem.total / 1024 ));
        availMem=str(int(mem.available / 1024 ));
        usedMem=str(int(mem.used / 1024 ));
        percentMem=str(mem.percent);
        freeMem=str(int(mem.free / 1024 ));
        activeMem=str(int(mem.active / 1024 ));
        inactiveMem=str(int(mem.inactive / 1024 ));
        bufferMem=str(int(mem.buffers / 1024 ));
        cachedMem=str(int(mem.cached / 1024 ));
        
        # swap usage
        swap = psutil.swap_memory();
        totalSwap=str(int(swap.total / 1024));
        usedSwap=str(int(swap.used / 1024));
        freeSwap=str(int(swap.free / 1024));
        percentSwap=str(swap.percent);
        sinSwap=str(int(swap.sin / 1024));
        soutSwap=str(int(swap.sout / 1024));
        sqlstmt ='insert into sm_memory_stats (hostip, hostname, total_memory, available_memory, used_memory, percent_memory, free_memory, active_memory, inactive_memory, buffer_memory, cached_memory,';
        sqlstmt+=' total_swap_mem, used_swap_mem, free_swap_mem, percent_swap_mem, sin_swap_mem, sout_swap_mem, snaptime) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(totalMem)+'\',';
        sqlstmt+=' \''+str(availMem)+'\',\''+str(usedMem)+'\',\''+str(percentMem)+'\',\''+str(freeMem)+'\',\''+str(activeMem)+'\',\''+str(inactiveMem)+'\',\''+str(bufferMem)+'\',';
        sqlstmt+=' \''+str(cachedMem)+'\',\''+str(totalSwap)+'\',\''+str(usedSwap)+'\',\''+str(freeSwap)+'\',\''+str(percentSwap)+'\',\''+str(sinSwap)+'\',\''+str(soutSwap)+'\',timestamp \''+str(currentTime)+'\');';
        statfl = open (outflname,"w");
        statfl.write(sqlstmt+'\n'+'commit;');
        statfl.close();
    except:
        pass
    
###
# Lists users currently connected to the system
###

def get_users():
    sqlArr=[];
    usrArr=[];
    outflname="/tmp/sql/connUsers_"+ time.strftime('%Y%m%d%H%M%S')+".sql";
    try:
        if majorVer < 2:
            usrArr=psutil.get_users();
        else:
            usrArr=psutil.users();
            
        totUsers=len(usrArr);
        i=0;
        while i < totUsers:
            user='user'+str(i);
            uname=usrArr[i].name;
            hname=string.replace(usrArr[i].host,':','');
            started=usrArr[i].started;
            connStartTime=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(started));
            currentTime=time.strftime('%Y'+'-'+'%m'+'-'+'%d'+' '+'%H'+':'+'%M'+':'+'%S');
            #print "Epoch Time connection started is %s" %started;
            #print "Time connection started is %s" %connStartTime;
            sqlstmt ='insert into sm_connected_users (hostip, hostname, num_of_users, conn_user, conn_user_name, conn_host_name, time_conn_start, snaptime) values ';
            sqlstmt+=' (\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(totUsers)+'\',\''+str(user)+'\',\''+str(uname)+'\',\''+str(hname)+'\',timestamp \''+str(connStartTime)+'\',timestamp \''+str(currentTime)+'\');';
            sqlArr.append(sqlstmt);
            i=i+1;
        statfl = open (outflname,"w");
        statfl.write(string.join(sqlArr,'\n')+'commit;');
        statfl.close();
    except:
        pass

def iostats():
    interval = 1
    # first get a list of all processes and disk io counters
    procs = [p for p in psutil.process_iter()];
    for p in procs[:]:
        try:
            if majorVer < 2:
                p._before = p.get_io_counters();
            else:
                p._before = p.io_counters();
            
        except psutil.Error:
            procs.remove(p);
            continue
    disks_before = psutil.disk_io_counters();
    # sleep some time
    time.sleep(interval);
    # then retrieve the same info again
    for p in procs[:]:
        try:
            if majorVer < 2:
                p._after = p.get_io_counters();
            else:
                p._after = p.io_counters();
            
            p._cmdline = ' '.join(p.cmdline);
            if not p._cmdline:
                if majorVer < 2:
                    p._cmdline = p.name;
                else:
                    p._cmdline = p.name();
                
            if majorVer < 2:
                p._username = p.username;
            else:
                p._username = p.username();
                
        except psutil.NoSuchProcess:
            procs.remove(p);
    disks_after = psutil.disk_io_counters();
    # finally calculate results by comparing data before and after the interval
    for p in procs:
        p._read_per_sec = p._after.read_bytes - p._before.read_bytes;
        p._write_per_sec = p._after.write_bytes - p._before.write_bytes;
        p._total = p._read_per_sec + p._write_per_sec;
    disks_read_per_sec = disks_after.read_bytes - disks_before.read_bytes;
    disks_write_per_sec = disks_after.write_bytes - disks_before.write_bytes;
    # sort processes by total disk IO so that the more intensive
    # ones get listed first
    processes = sorted(procs, key=lambda p: p._total, reverse=True);
    return (processes, disks_read_per_sec, disks_write_per_sec)

def piostats(procs, disks_read, disks_write):
    outflname="/tmp/sql/iostats_"+ time.strftime('%Y%m%d%H%M%S')+".sql";
    sqlArr=[];
    currentTime=time.strftime('%Y'+'-'+'%m'+'-'+'%d'+' '+'%H'+':'+'%M'+':'+'%S');
    disks_tot_read = str(int(disks_read / 1024));
    disks_tot_write= str(int(disks_write / 1024));
    sqlstmt= 'insert into sm_io_stats (HOSTIP,HOSTNAME,TOTAL_DISK_READ_KB,TOTAL_DISK_WRITE_KB,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',';
    sqlstmt+='\''+str(disks_tot_read)+'\',\''+str(disks_tot_write)+'\',timestamp \''+str(currentTime)+'\');';
    sqlArr.append(sqlstmt);
    i=0
    for p in procs:
        # top n=20 processes output
        if i<20:
            processID=p.pid;
            userName=p._username[:7];
            readBytes=str(int(p._read_per_sec / 1024));
            writeBytes=str(int(p._write_per_sec / 1024));
            cmdLine=p._cmdline;
            sqlstmt= 'insert into sm_io_stats (HOSTIP,HOSTNAME,PROCESS_ID,USER_NAME,READ_KB_PER_SEC, WRITE_KB_PER_SEC, COMMAND,SNAPTIME) values (\''+str(hostIP)+'\',\''+str(hostname)+'\',';
            sqlstmt+='\''+str(processID)+'\',\''+str(userName)+'\',\''+str(readBytes)+'\',\''+str(writeBytes)+'\',\''+str(cmdLine)+'\',timestamp \''+str(currentTime)+'\');';
            sqlArr.append(sqlstmt);
            i=i+1;
        else:
            break
    statfl = open (outflname,"w");
    statfl.write(string.join(sqlArr,'\n')+'\n'+'commit;');
    statfl.close();

def disk_util():
    outflname="/tmp/sql/diskstats_"+ time.strftime('%Y%m%d%H%M%S')+".sql";
    sqlArr=[];
    currentTime=time.strftime('%Y'+'-'+'%m'+'-'+'%d'+' '+'%H'+':'+'%M'+':'+'%S');
    for part in psutil.disk_partitions(all=True):
        usage = psutil.disk_usage(part.mountpoint);
        device=part.device
        totalAlloc=str(int(usage.total / 1024));
        usedSpace=str(int(usage.used / 1024));
        freeSpace=str(int(usage.free / 1024));
        usePercent=int(usage.percent);
        fileSysType=part.fstype;
        mountPoint=part.mountpoint;
        readWrite=part.opts;
        sqlstmt= 'insert into sm_disk_usage_stats (HOSTIP,HOSTNAME,DEVICE,TOTAL_SPACE_KB,USED_SPACE_KB,FREE_SPACE_KB,USED_PERCENT,FILE_SYSTEM_TYPE,MOUNT_POINT,READ_WRITE_OPTS,SNAPTIME)';
        sqlstmt+=' values(\''+str(hostIP)+'\',\''+str(hostname)+'\',\''+str(device)+'\',\''+str(totalAlloc)+'\',\''+str(usedSpace)+'\',\''+str(freeSpace)+'\',\''+str(usePercent)+'\',';
        sqlstmt+=' \''+str(fileSysType)+'\',\''+str(mountPoint)+'\',\''+str(readWrite)+'\',timestamp \''+str(currentTime)+'\');';
        sqlArr.append(sqlstmt);
       
    statfl = open (outflname,"w");
    statfl.write(string.join(sqlArr,'\n')+'\n'+'commit;');
    statfl.close();    
        
        
def main():
    cpu_monitoring();
    memory_util();
    get_users();
    disk_util()
    pargs = iostats();
    piostats(*pargs);

if __name__ == '__main__':
    main();