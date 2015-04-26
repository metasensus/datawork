#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;

def report():
	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';

	sqlstmt='SELECT  A.COUNTRY_NAME, TO_CHAR(TO_DATE(A.DATAMONTH,\'YYYYMM\'),\'Mon YYYY\') MONTHYR, CAPACITY_IN_TIB,SYSTEM_COUNT FROM MONTHLY_CAPACITY_COUNTRYNAME A,';
	sqlstmt+='(SELECT COUNTRY_NAME,MAX(DATAMONTH) MAXMONTH FROM MONTHLY_CAPACITY_COUNTRYNAME GROUP BY COUNTRY_NAME) B ';
	sqlstmt+=' WHERE A.COUNTRY_NAME=B.COUNTRY_NAME AND A.DATAMONTH=B.MAXMONTH ORDER BY COUNTRY_NAME';
	oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/country_trend.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		country=rec[0];
		currmonth=rec[1];
		capacity=rec[2];
		systems=rec[3];
		sqlstmt='SELECT DATAMONTH,CAPACITY_IN_TIB FROM MONTHLY_CAPACITY_COUNTRYNAME WHERE COUNTRY_NAME=\''+country+'\' ORDER BY DATAMONTH';
		dat=oracon.execSql(oraconn,sqlstmt);
		monthArr=[];
		for datrec in dat:
			monthArr.append(str(datrec[1]));
		monthStr='['+string.join(monthArr,',')+']';
		sqlstmt='SELECT DATAMONTH,SYSTEM_COUNT FROM MONTHLY_CAPACITY_COUNTRYNAME WHERE COUNTRY_NAME=\''+country+'\' ORDER BY DATAMONTH';
		dat=oracon.execSql(oraconn,sqlstmt);
		monthArr=[];
		for datrec in dat:
			monthArr.append(str(datrec[1]));
		sysStr='['+string.join(monthArr,',')+']';
		datarray.append('[\"\",\"'+string.strip(country)+'\",\"'+string.strip(str(currmonth))+'\",'+str(capacity)+','+monthStr+','+monthStr+','+sysStr+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();

def main():
    	#while (1):
    	report();

if __name__ == '__main__':
	main();

