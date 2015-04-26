#!/usr/bin/env python

import socket;
import multiprocessing;
import string;
import decimal;

def retHostName():
    return socket.gethostname();

def retHostIP():
    ipaddr='';
    try:
        s = socket.socket(socket.AF_INET,socket.SOCK_DGRAM);
        s.connect(('1.1.1.1',8000));
        ipaddr = s.getsockname()[0];
        s.close();
    except:
        pass
    
    return ipaddr;

def retcpuCount():
    return multiprocessing.cpu_count();

def retphyMem():
    mem=0;
    with open('/proc/meminfo') as f:
        memstr='';
        for ln in iter(f.readline()):
            memstr+=ln;
        
        lnlist=memstr.split(':');
        
        if lnlist[0]=='MemTotal':
            lstr = lnlist[1].strip();
            memvallist=lstr.split(' ');
            if string.lower(memvallist[1]) == 'kb':
                mem = decimal.Decimal(memvallist[0])/1024/1024;
            if string.lower(memvallist[1]) == 'mb':
                mem= decimal.Decimal(memvallist[0])/1024;
            return mem;

