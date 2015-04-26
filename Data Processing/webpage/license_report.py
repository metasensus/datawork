#!/usr/bin/env python
#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;

def license():
    constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
    sqlstmt='select INSERVSERIAL, CUSTOMER_NAME, LOCATION, COUNTRY, REGION, THINPROVISIONIG, THINCONVERSION, THINPERSISTENCE, VIRTUALCOPY, REMOTECOPY, PEERPERSISTENCE, DYNAMICOPTIMIZATION, ADAPTIVEOPTIMIZATION, PRIORITYOPTIMIZATION, PEERMOTION, DATAENCRYPTION, VIRTUALDOMAINS, VIRTUALLOCK, SYSTEMREPORTER, RMEXCHANGE, RMHYPERV, RMO, RMS, RMVV, SYSTEMTUNER from LICENSE_FILTER ORDER BY INSERVSERIAL'; 
    oraconn=oracon.openconnect(constr);
    resultrec=oracon.execSql(oraconn,sqlstmt);
    fl=open('report/LicenseGlobal.txt','w');

    datstr='{"aaData": [\n'

    datarray=[];
    for rec in resultrec:
        datarray.append('['+str(rec[0])+',\"'+string.strip(str(rec[1]))+'\",\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",\"'+str(rec[7])+'\",\"'+str(rec[8])+'\",\"'+str(rec[9])+'\",\"'+str(rec[10])+'\",\"'+str(rec[11])+'\",\"'+str(rec[12])+'\",\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",\"'+str(rec[19])+'\",\"'+str(rec[20])+'\",\"'+str(rec[21])+'\",\"'+str(rec[22])+'\",\"'+str(rec[23])+'\",\"'+str(rec[24])+'\"]');
    datstr+=string.join(datarray,',\n')+'\n]\n}';
    fl.write(datstr);
    fl.close();
    region=['Americas','APJ','EMEA'];
    for reg in region:
        sqlstmt='select INSERVSERIAL, CUSTOMER_NAME, LOCATION, COUNTRY, REGION, THINPROVISIONIG, THINCONVERSION, THINPERSISTENCE, VIRTUALCOPY, REMOTECOPY, PEERPERSISTENCE, DYNAMICOPTIMIZATION, ADAPTIVEOPTIMIZATION, PRIORITYOPTIMIZATION, PEERMOTION, DATAENCRYPTION, VIRTUALDOMAINS, VIRTUALLOCK, SYSTEMREPORTER, RMEXCHANGE, RMHYPERV, RMO, RMS, RMVV, SYSTEMTUNER from LICENSE_FILTER WHERE REGION=upper(\''+reg+'\') ORDER BY INSERVSERIAL'; 
        oraconn=oracon.openconnect(constr);
        resultrec=oracon.execSql(oraconn,sqlstmt);
        fl=open('report/License'+reg+'.txt','w');

        datstr='{"aaData": [\n'

        datarray=[];
        for rec in resultrec:
                datarray.append('['+str(rec[0])+',\"'+string.strip(str(rec[1]))+'\",\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",\"'+str(rec[7])+'\",\"'+str(rec[8])+'\",\"'+str(rec[9])+'\",\"'+str(rec[10])+'\",\"'+str(rec[11])+'\",\"'+str(rec[12])+'\",\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",\"'+str(rec[19])+'\",\"'+str(rec[20])+'\",\"'+str(rec[21])+'\",\"'+str(rec[22])+'\",\"'+str(rec[23])+'\",\"'+str(rec[24])+'\"]');
        datstr+=string.join(datarray,',\n')+'\n]\n}';
        fl.write(datstr);
        fl.close();
    
    oraconn.close();

def main():
	license();

if __name__ == '__main__':
	main();


