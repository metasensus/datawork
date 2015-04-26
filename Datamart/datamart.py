#!/usr/bin/env python
import time;
import datamartlib as dlib;
from multiprocessing import Process;
#print
purpose= dlib.check_purpose();  
    
if purpose=='sync':
        
    while (1):
        p=Process(target=dlib.datamart_sync());
        p.start();
        time.sleep(1800);
    
if purpose=='datamartextract':
    while (1):
        p=Process(target=dlib.create_insert_table());
        time.sleep(1800);
        
        
