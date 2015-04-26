#!/usr/bin/env python
import psutil; 
import re;
import os;

# ===================
# Main Python section
# ===================
 
def cpu_monitoring (): 
   # Current system-wide CPU utilization as a percentage
   # ---------------------------------------------------
   # Server as a whole:
   percs = psutil.cpu_percent(interval=0.5, percpu=False)
   print " CPU ALL: ",percs," %";
 
   # Individual CPUs
   percs = psutil.cpu_percent(interval=0.5, percpu=True)
   for cpu_num, perc in enumerate(percs):
      print " CPU%-2s %5s%% " % (cpu_num, perc);
   # end for
 
 
   # Details on Current system-wide CPU utilziation as a percentage
 
   # --------------------------------------------------------------
   # Server as a whole
   perc = psutil.cpu_times_percent(interval=0.5, percpu=False)
   print " CPU ALL:";
   str1 = "   user:    %5s%%  nice:  %5s%%" % (perc.user, perc.nice);
   str2 = "   system:  %5s%%  idle:  %5s%%  " % (perc.system, perc.idle);
   str3 = "   iowait:  %5s%%  irq:   %5s%% " % (perc.iowait, perc.irq );
   str4 = "   softirq: %5s%%  steal: %5s%% " % (perc.softirq, perc.steal);
   str5 = "   guest:   %5s%% " % (perc.guest);
   print str1
   print str2
   print str3
   print str4
   print str5;
 
   # Individual CPUs
   percs = psutil.cpu_times_percent(interval=0.5, percpu=True)
   for cpu_num, perc in enumerate(percs):
      print " CPU%-2s" % (cpu_num);
      str1 = "   user:    %5s%%  nice:  %5s%%" % (perc.user, perc.nice);
      str2 = "   system:  %5s%%  idle:  %5s%%  " % (perc.system, perc.idle);
      str3 = "   iowait:  %5s%%  irq:   %5s%% " % (perc.iowait, perc.irq );
      str4 = "   softirq: %5s%%  steal: %5s%% " % (perc.softirq, perc.steal);
      str5 = "   guest:   %5s%% " % (perc.guest);
      print str1
      print str2
      print str3
      print str4
      print str5;
   # end for
 
# end if

def commify3(amount):
    amount = str(amount)
    amount = amount[::-1]
    amount = re.sub(r"(\d\d\d)(?=\d)(?!\d*\.)", r"\1,", amount)
    return amount[::-1]
# end def commify3(amount):
 
# ===================
# Main Python section
# ===================
 
def memory_test():
   # memory usage:
   mem = psutil.virtual_memory();
   used = mem.total - mem.available;
   a1 = str(int(used / 1024 / 1024)) + "M";
   a2 = str(int(mem.total / 1024 / 1024)) + "M";
   a3 = commify3(a1) + "/" + commify3(a2);
   print ("Memory:                %-10s   (used)/(total)" % (a3) );
   print ("Memory Total:     %10sM" % (commify3(str(int(mem.total / 1024 / 1024)))) );
   print ("Memory Available: %10sM" % (commify3(str(int(mem.available / 1024 / 1024)))) );
   print "Memory Percent:  %10s" % (str(mem.percent)),"%";
   print ("Memory used:      %10sM" % (commify3(str(int(mem.used / 1024 / 1024)))) );
   print ("Memory free:      %10sM" % (commify3(str(int(mem.free / 1024 / 1024)))) );
   print ("Memory active:    %10sM" % (commify3(str(int(mem.active / 1024 / 1024)))) );
   print ("Memory inactive:  %10sM" % (commify3(str(int(mem.inactive/ 1024 / 1024)))) );
   print ("Memory buffers:   %10sM" % (commify3(str(int(mem.buffers / 1024 / 1024)))) );
   print ("Memory cached:    %10sM" % (commify3(str(int(mem.cached / 1024 / 1024)))) );
   
   # swap usage
   swap = psutil.swap_memory();
   a1 = str(int(swap.used / 1024 / 1024)) + "M";
   a2 = str(int(swap.total / 1024 / 1024)) + "M";
   a3 = commify3(a1) + "/" + commify3(a2);
   print " ";
   print ("Swap Space:                %-10s (used)/(total)" % (a3) );
   print ("Swap Total:       %10sM" % (commify3(str(int(swap.total / 1024 / 1024)))) );
   print ("Swap Used:        %10sM" % (commify3(str(int(swap.used / 1024 / 1024)))) );
   print ("Swap Free:        %10sM" % (commify3(str(int(swap.free / 1024 / 1024)))) );
   print "Swap Percentage: %10s" % (str(swap.percent)),"%";
   print ("Swap sin:         %10sM" % (commify3(str(int(swap.sin / 1024 / 1024)))) );
   print ("Swap sout:        %10sM" % (commify3(str(int(swap.sout / 1024 / 1024)))) );
 
# end if

def get_mount_point(pathname):
    pathname= os.path.normcase(os.path.realpath(pathname));
    parent_device= path_device= os.stat(pathname).st_dev;
    while parent_device == path_device:
        mount_point= pathname;
        pathname= os.path.dirname(pathname);
        if pathname == mount_point: break
        parent_device= os.stat(pathname).st_dev;
    return mount_point;

def get_mounted_device(pathname):
    # uses "/proc/mounts"
    pathname= os.path.normcase(pathname) # might be unnecessary here
    try:
        with open("/proc/mounts", "r") as ifp:
            for line in ifp:
                fields= line.rstrip('\n').split();
                # note that line above assumes that
                # no mount points contain whitespace
                if fields[1] == pathname:
                    return fields[0];
    except EnvironmentError:
        pass
    return None # explicit

def get_fs_freespace(pathname):
    stat= os.statvfs(pathname);
    # use f_bfree for superuser, or f_bavail if filesystem
    # has reserved space for superuser
    return stat.f_bfree*stat.f_bsize;


    