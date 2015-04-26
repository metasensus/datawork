#!/usr/bin/env python

#!/usr/bin/env python
#!/usr/bin/env python
#!/usr/bin/env python
from multiprocessing import Process;
import string;
import oracleconnect as oracon;
import os;
import sys;
import time;
import commands;
import vertica_connect as vconn;



def copyOracleDatatoVertica():
    datalist=[['statpd',' where pdtype in (\'FC\',\'NL\',\'SSD\')']];
    for datatype in datalist:
        print 'Starting '+datatype+' ....'
        processstatpd(datatype[0],datatype[1]);
        #p.daemon = True;
      
def main():
    while (1):
        copyOracleDatatoVertica();
        time.sleep(86400);
    
if __name__ == '__main__':
    main();    


