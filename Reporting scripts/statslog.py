#!/usr/bin/env python
import os;
import time;


def logcreate(logfilename):
    currentdate=time.strftime('%Y%m%d%H%M%S');
    if os.path.isfile(logfilename):
        os.rename(logfilename,logfilename+'.'+currentdate);
    return logfilename;

def logwrite(flnm,message,proc='',debug='False',onlydebug=0):
    if time.strftime('%H')=='0' or time.strftime('%H')=='00':
        logcreate(flnm);
    if debug=='True' and onlydebug == 1:
        fl=open(flnm+'.debug','a');
        fl.write(time.strftime('%m/%d/%Y %H:%M:%S')+' '+proc+'\(debug-message\) : '+message+'\n');
    if debug=='True' and onlydebug == 0:
        fl=open(flnm,'a');
        fl.write(time.strftime('%m/%d/%Y %H:%M:%S')+' '+proc+' : '+message+'\n');
    if debug=='False':
        fl=open(flnm,'a');
        fl.write(time.strftime('%m/%d/%Y %H:%M:%S')+' '+proc+' : '+message+'\n');
    fl.flush();
    fl.close();

