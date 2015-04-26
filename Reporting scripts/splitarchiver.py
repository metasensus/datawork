import archiverlib as arclib;
from multiprocessing import Process;
import time;

try:
    p=Process(target=arclib.splitFileArchive());
    p.daemon=True;
    p.start();
except:
    print "Error reported: "+str(sys.exc_info()[1]);
