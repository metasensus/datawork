#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;

def report():
	constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';

	sqlstmt='select to_char(rap_date,\'Mon YYYY\') RAP_Month,rap_desc,count(distinct prod_id) number_of_unique_systems '; 
	sqlstmt+='from rap_extract where to_char(rap_date,\'YYYYMM\') in (select max(to_char(rap_date,\'YYYYMM\')) from rap_extract) and rap_desc is not null  ';
	sqlstmt+='group by to_char(rap_date,\'Mon YYYY\'),rap_category,rap_desc order by 3 desc';
	oraconn=oracon.openconnect(constr);
	resultrec=oracon.execSql(oraconn,sqlstmt);
	fl=open('report/rap_trend.txt','w');

	datstr='{"aaData": [\n'

	datarray=[];
	for rec in resultrec:
		rapmonth=rec[0];
		rapdesc=rec[1];
		systems=rec[2];
		sqlstmt='SELECT to_char(rap_date,\'YYYYMM\'),count(distinct prod_id) FROM rap_extract WHERE rap_desc=\''+rapdesc+'\' GROUP BY to_char(rap_date,\'YYYYMM\') ORDER BY to_char(rap_date,\'YYYYMM\')';
		dat=oracon.execSql(oraconn,sqlstmt);
		monthArr=[];
		for datrec in dat:
			monthArr.append(str(datrec[1]));
		monthStr='['+string.join(monthArr,',')+']';	
		datarray.append('[\"'+string.strip(rapdesc)+'\",\"'+string.strip(str(rapmonth))+'\",'+str(systems)+','+monthStr+']');
	datstr+=string.join(datarray,',\n')+'\n]\n}';
	fl.write(datstr);
	fl.close();
	oraconn.close();

def main():
    	#while (1):
    	report();

if __name__ == '__main__':
	main();

