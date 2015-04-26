#!/usr/bin/env python
import string;
import oracleconnect as oracon;
import os;
import time;
import statslog;
import sys;


def checkmakedir(dname,fname):
    try:
        d=os.path.dirname(dname);
        if not os.path.exists(d):
            os.makedirs(d);
    except:
        fl=statslog.logcreate(fname);
        statslog.logwrite(fl,"Error reported (create dir): "+str(sys.exc_info()[1]));
    return;

def drilldown():
    constr='produser/produser@callhomedw.3pardata.com:1521/callhomedw';
    oraconn=oracon.openconnect(constr);
    sqlstmt='select count(distinct inservserial) from capacity_web_report';
    numrec=oracon.execSql(oraconn,sqlstmt);
    for nrec in numrec:
        numInserv=nrec[0];
    
    countInserv=1;
    sqlstmt='select distinct inservserial from capacity_web_report order by inservserial';
    insrec=oracon.execSql(oraconn,sqlstmt);
    
    for inrec in insrec:
        
        sqlstmt='select  ROUND((TOTAL_SPACE-TOTAL_FREE_SPACE)/TOTAL_SPACE*100,2) USED_SPACE_PERS, ROUND(TOTAL_FREE_SPACE/TOTAL_SPACE*100,2) FREE_SPACE_PERS FROM capacity_web_report where inservserial=\''+inrec[0]+'\'';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        checkmakedir('/root/proc/report/data/'+string.strip(inrec[0])+'/','drilldown_create');
        fl=open('report/data/'+string.strip(inrec[0])+'/capacityUtilData.txt','w');
        for res in resultrec:
            dataStr='{"chart":{\n\t\t"palette": "4",\n\t\t"caption":"Capacity Utilization",\n\t\t"xAxisName":"Used Space",'
            dataStr+='\n\t\t"numberSuffix":"%"\n\t},\n\t"data":[\n\t\t{\n\t\t\t"label":"Used Space",\n\t\t\t"color": "B1D1DC",\n\t\t\t"value":"';
            dataStr+=str(res[0])+'"\n\t\t},\n\t\t{\n\t\t\t"label":"Free Space",\n\t\t\t"color": "C8A1D1",\n\t\t\t"value":"'+str(res[1]);
            dataStr+='"\n\t\t}\n\t]\n}';
        fl.write(dataStr);
        fl.close();
        sqlstmt='SELECT MONTHYR FROM INSERV_CAPACITY_TREND where inservserial='+inrec[0]+' ORDER BY YEARMONTH';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        fl=open('report/data/'+string.strip(inrec[0])+'/capacityUtilTrendData.txt','w');
        dataStr='{\n\t"chart": {\n\t\t"bgcolor": "FFFFFF",\n\t\t"palette": "4",\n\t\t"outcnvbasefontcolor": "666666",\n\t\t"caption": "Capacity Utilization Trend",';
        dataStr+='\n\t\t"xaxisname": "Month",\n\t\t"yaxisname": "Capacity",';
        dataStr+='\n\t\t"numberprefix": "",\n\t\t"showlabels": "1",\n\t\t"showvalues": "0",\n\t\t"plotfillalpha": "70","numvdivlines": "10",';
        dataStr+='\n\t\t"showalternatevgridcolor": "1",\n\t\t"alternatevgridcolor": "e1f5ff",\n\t\t"divlinecolor": "999999",\n\t\t"basefontcolor": "666666",';
        dataStr+='\n\t\t"canvasborderthickness": "1",\n\t\t"showplotborder": "0",\n\t\t"plotborderthickness": "0",\n\t\t"zgapplot": "0",';
        dataStr+='\n\t\t"zdepth": "120",\n\t\t"exetime": "1.2",\n\t\t"dynamicshading": "1",\n\t\t"yzwalldepth": "5",\n\t\t"zxwalldepth": "5",';
        dataStr+='\n\t\t"xywalldepth": "5",\n\t\t"canvasbgcolor": "FBFBFB",\n\t\t"startangx": "0",\n\t\t"startangy": "0",\n\t\t"endangx": "5",';
        dataStr+='\n\t\t"endangy": "-25",\n\t\t"divlineeffect": "bevel"\n\t},\n\t"categories": [\n\t\t{\n\t\t\t"category": [';
        
        cat=[]
        for res in resultrec:
            cat.append(res[0]);
        dataStr+='\n\t\t\t\t{\n\t\t\t\t\t"label":"'+string.join(cat,'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"label": "')+'"\n\t\t\t\t}';
        
        dataStr+='\n\t\t\t]\n\t\t}\n\t],\n\t"dataset": [\n\t\t{\n\t\t\t"seriesname": "Used Capacity in TiB",\n\t\t\t"color": "B1D1DC",';        
        dataStr+='\n\t\t\t"plotbordercolor": "B1D1DC",\n\t\t\t"renderas": "line",\n\t\t\t"data": [';
        
        sqlstmt='SELECT USEDTIB FROM INSERV_CAPACITY_TREND where inservserial='+inrec[0]+' ORDER BY YEARMONTH';
        resultrec=oracon.execSql(oraconn,sqlstmt);
        used=[]
        for res in resultrec:
            used.append(str(res[0]));
        
        dataStr+='\n\t\t\t\t{\n\t\t\t\t\t"value":"'+string.join(used,'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "')+'"\n\t\t\t\t}';
        dataStr+='\n\t\t\t]\n\t\t},\n\t\t{\n\t\t\t"seriesname": "Total Capacity in TiB",\n\t\t\t"color": "C8A1D1",';        
        dataStr+='\n\t\t\t"plotbordercolor": "C8A1D1",\n\t\t\t"renderas": "line",\n\t\t\t"data": [';
        
        sqlstmt='SELECT TOTALTIB FROM INSERV_CAPACITY_TREND where inservserial='+inrec[0]+' ORDER BY YEARMONTH';
        resultrec=oracon.execSql(oraconn,sqlstmt);
        total=[]
        for res in resultrec:
            total.append(str(res[0]));
        
        dataStr+='\n\t\t\t\t{\n\t\t\t\t\t"value":"'+string.join(total,'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "')+'"\n\t\t\t\t}';
        dataStr+='\n\t\t\t]\n\t\t}\n\t],\n\t"styles": {\n\t\t"definition": [\n\t\t\t{\n\t\t\t\t"name": "captionFont",\n\t\t\t\t';
        dataStr+='"type": "font",\n\t\t\t\t"size": "15"\n\t\t\t}\n\t\t],\n\t\t"application": [\n\t\t\t{\n\t\t\t\t"toobject": "caption",';
        dataStr+='\n\t\t\t\t"styles": "captionfont"\n\t\t\t}\n\t\t]\n\t}\n}';
        
        
        
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/companyData.txt','w');
        
        sqlstmt='SELECT COMPANY,ADDRESS1||\',\'||CASE WHEN UPPER(TRIM(CITY))=\'UNKNOWN\' THEN LOCATION ELSE CITY END||';
        sqlstmt+='CASE WHEN UPPER(NVL2(STATE,\',\'||STATE||\',\',\',\')) LIKE \'%UNKNOWN%\' THEN \',\' ELSE NVL2(STATE,\',\'||STATE||\',\',\',\')';
        sqlstmt+='END  ||CT.COUNTRY_NAME ADDRESS,TRIM(MODEL),NVL(CASE WHEN INSTR (OS_REL, \';\') > 0 ';
        sqlstmt+=' THEN SUBSTR (OS_REL, 1, INSTR (OS_REL, \';\', 1) - 1) ELSE OS_REL  END,\'Unknown\') OS_REL ';
        sqlstmt+='FROM (SELECT DISTINCT CUSTOMER_NAME COMPANY, ';
        sqlstmt+='TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE (LOCATION, \'"\', \'\'),\'\\\',\'/\'),CHR(9),\'\'),\'&\',\' and \')';
        sqlstmt+=',CHR(10),\'\'),CHR(13),\'\')) LOCATION,TRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE (ADDRESS1, \'"\', \'\'),\'\\\'';
        sqlstmt+=',\'/\'),CHR(9),\'\'),\'&\',\' and \'),CHR(10),\'\'),CHR(13),\'\')) ADDRESS1,TRIM(REPLACE(REPLACE(REPLACE(REPLACE (CITY,';
        sqlstmt+='\'"\', \'\'),\'\\\',\'/\'),CHR(9),\'\'),\'&\',\' and \')) CITY,TRIM(REPLACE(REPLACE(REPLACE(REPLACE (STATE, \'"\',';
        sqlstmt+='\'\'),\'\\\',\'/\'),CHR(9),\'\'),\'&\',\' and \')) STATE,COUNTRY_CODE, REPLACE(REPLACE(UPPER(MODEL),\'HP_3PAR\',\'\'),';
        sqlstmt+='\'INSERV\',\'\') MODEL, OS_REL FROM DATASTORE.ALL_INSERV_MASTER WHERE INSERVSERIAL=\''+inrec[0]+'\') MAST';
        sqlstmt+=',DATASTORE.COUNTRY_DESC CT WHERE MAST.COUNTRY_CODE=CT.COUNTRY_CODE ';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        dataStr='{"aaData": [\n\t';
        
        for res in resultrec:
            dataStr+='["'+res[0]+'","'+res[1]+'","'+res[2]+'","'+res[3]+'"]';
        dataStr+='\n\t]\n}';    
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/diskUtilData.txt','w');
        
        sqlstmt='SELECT (TOTAL_FC_SPACE-TOTAL_FC_FREE) TOTAL_FC_USED_GIB,';
        sqlstmt+='(TOTAL_NL_SPACE- TOTAL_NL_FREE) TOTAL_NL_USED_GIB,(TOTAL_SSD_SPACE- TOTAL_SSD_FREE) TOTAL_SSD_USED_GIB,';
        sqlstmt+='TOTAL_FC_FREE, TOTAL_NL_FREE, TOTAL_SSD_FREE  FROM INSERV_CAPACITY_REPORT WHERE INSERVSERIAL=\''+inrec[0]+'\'';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        dataStr='{\n\t"chart": {\n\t\t"palette": "4",\n\t\t"caption": "Disk type utilization",\n\t\t"xaxisname": "Utilization",\n\t\t';
        dataStr+='"yaxisname": "Disk type",\n\t\t"numberprefix": "",\n\t\t"showvalues": "0"\n\t},\n\t"categories":';
        dataStr+='[\n\t\t{\n\t\t\t"category": [\n\t\t\t\t{\n\t\t\t\t\t"label": "FC"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"label": "NL"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"label": "SSD"\n\t\t\t\t}';
        dataStr+='\n\t\t\t]\n\t\t}\n\t],\n\t"dataset": [\n\t\t{\n\t\t\t"seriesname": "Used in GiB",\n\t\t\t"color": "B1D1DC",\n\t\t\t"data": [';
        
        useddat='';
        freedat='';
        for res in resultrec:
            useddat='\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[0])+'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[1])+'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[2])+'"\n\t\t\t\t}';
            freedat='\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[3])+'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[4])+'"\n\t\t\t\t},\n\t\t\t\t{\n\t\t\t\t\t"value": "'+str(res[5])+'"\n\t\t\t\t}';
        
        dataStr+=useddat+'\n\t\t\t]\n\t\t},\n\t\t{\n\t\t\t"seriesname": "Free in GiB",\n\t\t\t"color": "C8A1D1",\n\t\t\t"data": ['+freedat;
        dataStr+='\n\t\t\t]\n\t\t}\n\t]\n}';
        
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/usageByDriveType.txt','w');
        
        sqlstmt='select to_char(datadate,\'mm/dd/yy\'),disktype,used_pers from(select datadate,manufacturer';
        sqlstmt+='||\'-\'||disk_model_type||\'(\'||disktype||\')\' disktype,';
        sqlstmt+='round(used_in_gib/total_in_gib*100,2) used_pers from usagetrend_with_desc where inservserial='+inrec[0]+') a ,';
        sqlstmt+='(select manufacturer||\'-\'||disk_model_type';
        sqlstmt+='||\'(\'||disktype||\')\' dtype,max(datadate) mdt from usagetrend_with_desc where inservserial='+inrec[0]; 
        sqlstmt+=' group by inservserial,manufacturer||\'-\'||disk_model_type||\'(\'||disktype||\')\') b';
        sqlstmt+=' where a.disktype=b.dtype and a.datadate=b.mdt';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        dataStr='{\n\t"aaData": [';
        dat='';
        datArray=[];
        recArray=[]
        for res in resultrec:
            dat='\n\t\t["'+res[1]+'","'+res[0]+'","'+str(res[2])+'",[';
            sqlstmt='select round(used_in_gib/total_in_gib*100,2) used_pers from usagetrend_with_desc where inservserial='+inrec[0]
            sqlstmt+=' and manufacturer||\'-\'||disk_model_type||\'(\'||disktype||\')\'=\''+res[1]+'\' order by datadate';
            
            datrec=oracon.execSql(oraconn,sqlstmt);
            for drec in datrec:
                datArray.append(str(drec[0]));
            
            dat+=string.join(datArray,',')+']]';
            recArray.append(dat);
        
        dataStr+=string.join(recArray,',')+'\n\t]\n}';
        
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/License.txt','w');
        
        dataStr='{\n\t"aaData": [';
        dat='';
        datArray=[];
        sqlstmt='select LICENSE_TYPE, case license_status when \'NO\' then case when LICENSE_EXP_DATE is null then \'VALID\'  else \'TRIAL\' end WHEN \'YES\' THEN CASE WHEN to_date(LICENSE_EXP_DATE,\'Month dd, YYYY\') <= TRIM(SYSDATE) THEN \'EXPIRED\' ELSE \'TRIAL\' END END ,LICENSE_EXP_DATE from LICENSE_REPORT where inservserial='+inrec[0];
        dat='\n\t\t'
            
        datrec=oracon.execSql(oraconn,sqlstmt);
        for drec in datrec:
            if drec[2]:
                datArray.append('["'+drec[0]+'","'+drec[1]+'","'+drec[2]+'"]');
            else:
                datArray.append('["'+drec[0]+'","'+drec[1]+'"," "]');
        dat+=string.join(datArray,',\n\t\t');
        dataStr+=dat+'\n\t]\n}';
        
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/licenseAttemptsByType.txt','w');
        
        dataStr='{\n\t"chart": {\n\t\t"caption": "Usage attempts after license expiry",\n\t\t"subcaption": "(Software)",\n\t\t"palette": "4",\n\t\t"decimals": "0",';
        dataStr+='\n\t\t"enablesmartlabels": "1",\n\t\t"enablerotation": "1",\n\t\t"bgangle": "360",\n\t\t"showborder": "0",\n\t\t"startingangle": "70"\n\t},';
        dataStr+='\n\t"data": [';
        dat='';
        datArray=[];
        sqlstmt='select LICENSE_TYPE, numberoftimes from LICENSE_REPORT where trunc(to_date(license_exp_date,\'Month dd, YYYY\'))<= trunc(sysdate) and inservserial='+inrec[0];
        dat=''
            
        datrec=oracon.execSql(oraconn,sqlstmt);
        for drec in datrec:
            datArray.append('\n\t\t{\n\t\t\t"label":"'+drec[0]+'",\n\t\t\t"value":"'+string.strip(str(drec[1]))+'"\n\t\t}');
        dat+=string.join(datArray,',');
        dataStr+=dat+'\n\t]\n}';
        
        fl.write(dataStr);
        fl.close();
        
        fl=open('report/data/'+string.strip(inrec[0])+'/pdByCage.txt','w');
        sqlstmt='select cage,disks,emptyslots from inserv_cage_disks where inservserial='+inrec[0]+' order by cage';
        dataStr='{"aaData" : [\n';
        datArray=[];
        dat=''
            
        datrec=oracon.execSql(oraconn,sqlstmt);
        for drec in datrec:
            datArray.append('\t\t["Cage '+str(drec[0])+'",'+str(drec[1])+',40,'+str(drec[2])+',['+str(drec[1])+','+str(drec[1])+','+'40,30,20]]');
        dat+=string.join(datArray,',\n');
        dataStr+=dat+'\n\t]\n}';
        fl.write(dataStr);
        fl.close();
        
        countInserv+=1;
        #if (countInserv/numInserv)*100 ==10 or (countInserv/numInserv)*100 ==20  or (countInserv/numInserv)*100 ==30 or (countInserv/numInserv)*100 ==40 or (countInserv/numInserv)*100 ==50 or (countInserv/numInserv)*100 == 60 or (countInserv/numInserv)*100 == 70 or (countInserv/numInserv)*100 == 80 or (countInserv/numInserv)*100 == 90 or (countInserv/numInserv)*100 ==100 :
        print 'Finished '+str(countInserv)+' out of '+str(numInserv);
        
    oraconn.close();
    
def main():
	drilldown();

if __name__ == '__main__':
	main();
        