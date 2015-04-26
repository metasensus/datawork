#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;
import datetime as dt;

constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
oraconn=oracon.openconnect(constr);
        	

def alert_detail(message_code, severity, model, inform,reccount):
	sqlstmt='SELECT DISTINCT TO_CHAR(DATADATE,\'DD-Mon-YYYY\'), NVL(NUM_SYSTEMS,0),datadate FROM ALERT_DAILY_AGGREGATE WHERE ';
	sqlstmt+=' TRIM(NVL(MESSAGE_CODE,\' \'))= \''+message_code+'\' AND TRIM(NVL(SEVERITY,\' \'))=\''+severity+'\' AND TRIM(NVL(MODEL,\' \'))=\''+model+'\' AND TRIM(NVL(INFORM_VERSION,\' \'))=\''+inform+'\' and datadate between trunc(sysdate)-180 and trunc(sysdate) order by datadate';
	#  ' and datadate between trunc(sysdate)-180 and trunc(sysdate) 
	starttime=dt.datetime.now();
	alertrec=oracon.execSql(oraconn,sqlstmt);
	alertstr='{"key":"'+str(reccount)+'\",\"systemDetails\": {';
	alertdata=[];
	dates=[];
	numSystems=[];
	custFinal=[];
	rapFinal=[];
	detFinal=[];
	recArray=[];
	dataRec=[];
	rowcount=0;
	for arec in alertrec:
		datdate='"'+arec[0]+'":{';
		nrecords='"nRecords":"'+str(arec[1])+'",'
		custArr=[];
		rapArr=[];
		detailArr=[];
		dates.append('"'+arec[0]+'"');
		numSystems.append('"'+str(arec[1])+'"');
			
		sqlstmt='SELECT distinct CUSTOMER_NAME,NUM_SYSTEMS FROM ALERT_CUSTOMER_SYSTEMS WHERE TRIM(MESSAGE_CODE)=\''+message_code+'\' AND TO_CHAR(DATADATE,\'DD-Mon-YYYY\')=\''+arec[0]+'\' and model=\''+model+'\' and TRIM(inform_version)=\''+inform+'\'';
		custrec=oracon.execSql(oraconn,sqlstmt);
			
		for cust in custrec:
			customer=string.replace(string.replace(string.replace(string.replace(cust[0],'"',' '),'\t',' '),'\n',' '),'\\',' ');
			custArr.append('{"customer":"'+customer+'","noOfSystems":"'+str(cust[1])+'"}');
		custrec.close();
			
		custStr='"nSystems":['+string.join(custArr,',')+']';
		custArr=[];				
		sqlstmt='SELECT distinct CUSTOMER_NAME,NUM_SYSTEMS FROM RAP_CUSTOMER WHERE TRIM(MESSAGE_CODE)=trim(\''+message_code+'\') AND TO_CHAR(DATADATE,\'DD-Mon-YYYY\')=trim(\''+arec[0]+'\') and TRIM(model)=trim(\''+model+'\') and TRIM(inform_version)=trim(\''+inform+'\')';
		#print sqlstmt
		raprec=oracon.execSql(oraconn,sqlstmt);
		for cust in raprec:
			customer=string.replace(string.replace(string.replace(string.replace(cust[0],'"',' '),'\t',' '),'\n',' '),'\\',' ');
			rapArr.append('{"customer":"'+customer+'","noOfSystems":"'+str(cust[1])+'"}');		       
		raprec.close();
		rapStr='"nRaps": ['+string.join(rapArr,',')+']';
		
			#rapFinal.append(rapStr);
		rapArr=[];	
		sqlstmt='SELECT distinct CUSTOMER_NAME,INSERVSERIAL,VERSION_ON_ERROR,NUM_RECORDS FROM ALERT_DAILY_OS_ONDATE WHERE TRIM(MESSAGE_CODE)=\''+message_code+'\' AND TO_CHAR(DATADATE,\'DD-Mon-YYYY\')=\''+arec[0]+'\' and model=\''+model+'\' and TRIM(current_inform_version)=\''+inform+'\'';
		allrec=oracon.execSql(oraconn,sqlstmt);
			
		for alldat in allrec:
			customer=string.replace(string.replace(string.replace(string.replace(alldat[0],'"',' '),'\t',' '),'\n',' '),'\\',' ');
			detailArr.append('{"customer":"'+customer+'","serialNumber":"'+str(alldat[1])+'","os":"'+alldat[2]+'","noOfOccurrence":"'+str(alldat[3])+'"}');
		allrec.close();
		allStr='"nDetails": ['+string.join(detailArr,',')+']';
			
		dataRec.append(datdate+nrecords+custStr+','+rapStr+','+allStr+'}')
		endtime=dt.datetime.now();
		rowcount+=1;
	
	
	alertstr+=string.join(dataRec,',\n')+'}}';
	dataRec=[];
	#print alertstr;
	alertrec.close();
	return alertstr;	
	
def alert():
	topres=[]
	constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
	sqlstmt='SELECT ALERT_MESSAGE, MESSAGE_CODE, SEVERITY, MODEL, INFORM_VERSION, NUM_SYSTEMS, NUM_RECORDS, INC_180_DAY, INC_90_DAY, INC_60_DAY,RANK_LEVEL FROM VW_ALERT';
        resultrec=oracon.execSql(oraconn,sqlstmt);
	rownum=0;
	
	fltrend=open('/report/alerts_trends_new','w');
	logfl=open('/root/proc/alert.log','w');
        fl=open('/report/alerts_new','w');
	fl.write('[\n');
	datarray=[];
	alertdrill=[];
	numrecs =0
	reccount=0;
	onedone=0;
	accumArr=[];
	recArr=[];
	for rec in resultrec:
		rownum+=1;
		reccount=rec[10];
		numrecs+=1;
		message=rec[0];
		message_code=rec[1];
		severity=rec[2];
		model=rec[3];
		inform=rec[4];
		systems=rec[5];
		records=rec[6];
		inc_180_day=rec[7];
		inc_90_day=rec[8];
		inc_60_day=rec[9];
		
		datarray.append('\t["",\"'+str(inc_180_day)+'\",\"'+str(inc_90_day)+'\",\"'+str(inc_60_day)+'\",\"'+severity+'\",\"'+message+'\",\"'+message_code+'\",\"'+model+'\",\"'+inform+'\",\"'+str(systems)+'\",\"'+str(records)+'\",\"'+str(reccount)+'\"]');
		
		alertstr=alert_detail(message_code, severity, model, inform,reccount)
		alertdrill.append(alertstr);
		if numrecs > 100:
			if onedone == 1:
				fltrend.write(','+string.join(alertdrill,','));
				fltrend.close();
				fltrend=open('/report/alerts_trends_new','a');
			else:
				fltrend.write('[\n\t'+string.join(alertdrill,','));
				fltrend.close();
				fltrend=open('/report/alerts_trends_new','a');
				onedone=1;
			logfl.write('Key pair done..... '+str(reccount)+'\n');
			print 'Key pair done..... '+str(reccount);
			alertdrill=[];
			alertstr='';
			fltrend.flush();
			numrecs=0;
			logfl.flush();
			
	fl.write(string.join(datarray,',')+']');
	fl.close();
	if len(alertdrill)>0:
		fltrend.write(string.join(alertdrill,',')+']');
	else:
		fltrend.write(']');
	fltrend.close();
	fl.close();
	logfl.close();
	resultrec.close();
def main():
    	#while (1):
	alert();

if __name__ == '__main__':
	main();

