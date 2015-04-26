#!/usr/bin/env python

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
    sqlstmt='select count(distinct country_name) from country_penetration';
    numrec=oracon.execSql(oraconn,sqlstmt);
    for nrec in numrec:
        numInserv=nrec[0];
    
    countInserv=1;
    sqlstmt='select distinct country_name from country_penetration order by country_name';
    insrec=oracon.execSql(oraconn,sqlstmt);
    
    for inrec in insrec:
        sqlstmt='select model,num_systems from country_penetration where country_name = \''+inrec[0]+'\' order by num_systems';
        
        resultrec=oracon.execSql(oraconn,sqlstmt);
        checkmakedir('/root/proc/report/countries/'+string.strip(inrec[0])+'/','drilldown_create');
        fl=open('report/countries/'+string.strip(inrec[0])+'/ModelPenetrationData.txt','w');
        dataStr='{\n\t"chart":{\n\t\t"caption":"Model Penetration %",\n\t\t"formatnumberscale": "0"\n\t},\n\t"data":[';
        datArray=[];
        for res in resultrec:
            datArray.append('\n\t\t{\n\t\t\t"label":"'+res[0]+'",'+'\n\t\t\t"value":"'+str(res[1])+'"\n\t\t}');
        dataStr+=string.join(datArray,',')+'\n\t]\n}';
        fl.write(dataStr);
        fl.close();
        
        sqlstmt='select distinct YYYYMM,month_yr from SHOWPD_LOWCAP where country_name = \''+inrec[0]+'\' order by YYYYMM';
        resultrec=oracon.execSql(oraconn,sqlstmt);
        fl=open('report/countries/'+string.strip(inrec[0])+'/MonthlyTrendOfMachineUsageByModelData.txt','w');
        dataStr='{\n\t"chart": {\n\t\t"caption": "Trend of machines above 80 % used",\n\t\t"yaxisname": "Number of machines",';
        dataStr+='\n\t\t"showvalues": "0",\n\t\t"areaovercolumns": "1",\n\t\t"showpercentvalues": "0"\n\t},\n\t"categories": [\n\t\t{';
        dataStr+='\n\t\t\t"category": [';
        dataArray=[];
        
        for res in resultrec:
            labsplit=string.split(string.replace(res[1],' ',':'),':');
            lab=labsplit[0]+' '+labsplit[len(labsplit)-1];
            dataArray.append('\n\t\t\t\t{\n\t\t\t\t\t"label": "'+lab+'"\n\t\t\t\t}');
        sqlstmt='select distinct model from SHOWPD_LOWCAP where country_name = \''+inrec[0]+'\'';
        modsrec=oracon.execSql(oraconn,sqlstmt);
        insArray=[];
        for modrec in modsrec:
            sqlstmt='select distinct YYYYMM,month_yr from SHOWPD_LOWCAP where country_name = \''+inrec[0]+'\' order by YYYYMM';
            datrec=oracon.execSql(oraconn,sqlstmt);
            insDatArray=[];
            for dtrec in datrec:
                sqlstmt='select count(1) from SHOWPD_LOWCAP where country_name = \''+inrec[0]+'\' and model=\''+modrec[0]+'\' and YYYYMM=\''+dtrec[0]+'\'';
                ctrec=oracon.execSql(oraconn,sqlstmt);
                for ct in ctrec:
                    counts=ct[0];
                if counts>0:
                    sqlstmt='select num_systems from SHOWPD_LOWCAP where country_name = \''+inrec[0]+'\' and model=\''+modrec[0]+'\' and YYYYMM=\''+dtrec[0]+'\'';
                    insdatrec=oracon.execSql(oraconn,sqlstmt);
                    for insdrec in insdatrec:
                        insDatArray.append('\n\t\t\t\t{\n\t\t\t\t\t"value":"'+str(insdrec[0])+'"\n\t\t\t\t}');
                else:
                    insDatArray.append('\n\t\t\t\t{\n\t\t\t\t\t"value":"0"\n\t\t\t\t}');
            insArray.append('\n\t\t{\n\t\t\t"seriesname": "'+modrec[0]+'",\n\t\t\t\t"data":['+string.join(insDatArray,',')+'\n\t\t\t\t]\n\t\t\t}');
                
                    
            
        dataStr+=string.join(dataArray,',')+'\n\t\t\t]\n\t\t}\n\t],\n\t"dataset":['+string.join(insArray,',')+'\n\t]\n}';
        fl.write(dataStr);
        fl.close();
        
        countInserv+=1;
        print 'Number countries done:'+str(countInserv)+' out of '+str(numInserv);
        
    oraconn.close();
    
def main():
	drilldown();

if __name__ == '__main__':
	main();
        