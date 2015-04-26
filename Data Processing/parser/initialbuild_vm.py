#!/usr/bin/env python
import CopyConfigLib as runconfig;
import os;
machine_type=raw_input('Type of machine:');

if machine_type=='process' or  machine_type=='splitter':
    runconfig.connectDb();

os.system('python /ods84/proc/machinestatus.py &');