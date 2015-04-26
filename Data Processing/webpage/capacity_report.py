#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;

def report():
	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
	
        sqlstmt='select COMPANY, INSERVSERIAL, HP_SERIAL_NUMBER, INSTALLDATE, trim(MODEL), OSVERSION, DATADATE, FREE_PERCENT, TOTAL_SPACE, TOTAL_FREE_SPACE,TOTAL_CAGES,TOTAL_DISKS,TOTAL_VIRTUAL_SIZE, COUNTRY, ACTIVE, ';
        sqlstmt+=' CUSTOMER_SYSTEM, FREE_SPACE_LT20, OLDERTHAN25,TRIM(MODEL_TYPE), FC_FREE_PERCENT, NL_FREE_PERCENT, SSD_FREE_PERCENT FROM capacity_web_report';

        oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/InstallBaseGlobal.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		datarray.append('[\"'+str(rec[0])+'\",'+string.strip(str(rec[1]))+',\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",'+str(rec[7])+','+str(rec[8])+','+str(rec[9])+','+str(rec[10])+','+str(rec[11])+','+str(rec[12])+',\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",'+str(rec[19])+','+str(rec[20])+','+str(rec[21])+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();
	
	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
	
        sqlstmt='select COMPANY, INSERVSERIAL, HP_SERIAL_NUMBER, INSTALLDATE, trim(MODEL), OSVERSION, DATADATE, FREE_PERCENT, TOTAL_SPACE, TOTAL_FREE_SPACE,TOTAL_CAGES,TOTAL_DISKS,TOTAL_VIRTUAL_SIZE, COUNTRY, ACTIVE, ';
        sqlstmt+=' CUSTOMER_SYSTEM, FREE_SPACE_LT20, OLDERTHAN25,TRIM(MODEL_TYPE), FC_FREE_PERCENT, NL_FREE_PERCENT, SSD_FREE_PERCENT FROM capacity_web_report where region=\'AMERICAS\'';

        oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/InstallBaseAmericas.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		datarray.append('[\"'+str(rec[0])+'\",'+string.strip(str(rec[1]))+',\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",'+str(rec[7])+','+str(rec[8])+','+str(rec[9])+','+str(rec[10])+','+str(rec[11])+','+str(rec[12])+',\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",'+str(rec[19])+','+str(rec[20])+','+str(rec[21])+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();

	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
	sqlstmt='select COMPANY, INSERVSERIAL, HP_SERIAL_NUMBER, INSTALLDATE, trim(MODEL), OSVERSION, DATADATE, FREE_PERCENT, TOTAL_SPACE, TOTAL_FREE_SPACE,TOTAL_CAGES,TOTAL_DISKS,TOTAL_VIRTUAL_SIZE, COUNTRY, ACTIVE, ';
	sqlstmt+=' CUSTOMER_SYSTEM, FREE_SPACE_LT20, OLDERTHAN25,TRIM(MODEL_TYPE), FC_FREE_PERCENT, NL_FREE_PERCENT, SSD_FREE_PERCENT FROM capacity_web_report where region=\'EMEA\'';
	oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/InstallBaseEMEA.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		datarray.append('[\"'+str(rec[0])+'\",'+string.strip(str(rec[1]))+',\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",'+str(rec[7])+','+str(rec[8])+','+str(rec[9])+','+str(rec[10])+','+str(rec[11])+','+str(rec[12])+',\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",'+str(rec[19])+','+str(rec[20])+','+str(rec[21])+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();
	
	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
	sqlstmt='select COMPANY, INSERVSERIAL, HP_SERIAL_NUMBER, INSTALLDATE, trim(MODEL), OSVERSION, DATADATE, FREE_PERCENT, TOTAL_SPACE, TOTAL_FREE_SPACE,TOTAL_CAGES,TOTAL_DISKS,TOTAL_VIRTUAL_SIZE, COUNTRY, ACTIVE, ';
        sqlstmt+=' CUSTOMER_SYSTEM, FREE_SPACE_LT20, OLDERTHAN25,TRIM(MODEL_TYPE), FC_FREE_PERCENT, NL_FREE_PERCENT, SSD_FREE_PERCENT FROM capacity_web_report where region=\'APJ\'';

	oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/InstallBaseAPJ.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		datarray.append('[\"'+str(rec[0])+'\",'+string.strip(str(rec[1]))+',\"'+str(rec[2])+'\",\"'+str(rec[3])+'\",\"'+str(rec[4])+'\",\"'+str(rec[5])+'\",\"'+str(rec[6])+'\",'+str(rec[7])+','+str(rec[8])+','+str(rec[9])+','+str(rec[10])+','+str(rec[11])+','+str(rec[12])+',\"'+str(rec[13])+'\",\"'+str(rec[14])+'\",\"'+str(rec[15])+'\",\"'+str(rec[16])+'\",\"'+str(rec[17])+'\",\"'+str(rec[18])+'\",'+str(rec[19])+','+str(rec[20])+','+str(rec[21])+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();
	
	
def main():
	report();

if __name__ == '__main__':
	main();

