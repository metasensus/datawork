#!/usr/bin/env python
#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;

def license():
    constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
    sqlstmt='SELECT INSERVSERIAL,MODEL,TRIM(CUSTOMER_NAME),replace(replace(replace(TRIM(TRIM(ADDRESS1)||\',\'||TRIM(CITY)||\',\'||TRIM(case when STATE =\'Unknown\' then \'\' else state||\',\' end)||COUNTRY_NAME),CHR(10),\'\'),chr(12),\'\'),chr(13),\'\') LOCATION,COUNTRY_NAME,LICENSE_TYPE,nvl(LICENSE_EXP_DATE,\' \') Exp_date,LICENSE_STATUS,COUNTRY_NAME FROM LICENSE_REPORT ORDER BY INSERVSERIAL'; 
    oraconn=oracon.openconnect(constr);
    resultrec=oracon.execSql(oraconn,sqlstmt);
    fl=open('report/LicenseGlobal.txt','w');

    datstr='{"aaData": [\n'

    datarray=[];
    for rec in resultrec:
        datarray.append('['+str(rec[0])+',\"'+string.strip(str(rec[1]))+'\",\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",\"'+str(rec[7])+'\",\"'+str(rec[8])+'\"]');
    datstr+=string.join(datarray,',\n')+'\n]\n}';
    fl.write(datstr);
    fl.close();
    region=['Americas','APJ','EMEA'];
    for reg in region:
        sqlstmt='SELECT INSERVSERIAL,MODEL,TRIM(CUSTOMER_NAME),replace(replace(replace(TRIM(TRIM(ADDRESS1)||\',\'||TRIM(CITY)||\',\'||TRIM(case when STATE =\'Unknown\' then \'\' else state||\',\' end)||COUNTRY_NAME),CHR(10),\'\'),chr(12),\'\'),chr(13),\'\') LOCATION,COUNTRY_NAME,LICENSE_TYPE,nvl(LICENSE_EXP_DATE,\' \') Exp_date,LICENSE_STATUS,COUNTRY_NAME FROM LICENSE_REPORT WHERE REGION=upper(\''+reg+'\') ORDER BY INSERVSERIAL'; 
        oraconn=oracon.openconnect(constr);
        resultrec=oracon.execSql(oraconn,sqlstmt);
        fl=open('report/License'+reg+'.txt','w');

        datstr='{"aaData": [\n'

        datarray=[];
        for rec in resultrec:
            datarray.append('['+str(rec[0])+',\"'+string.strip(str(rec[1]))+'\",\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",\"'+str(rec[7])+'\",\"'+str(rec[8])+'\"]');
        datstr+=string.join(datarray,',\n')+'\n]\n}';
        fl.write(datstr);
        fl.close();
    
    oraconn.close();

def main():
	license();

if __name__ == '__main__':
	main();


