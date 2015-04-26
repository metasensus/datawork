#!/usr/bin/env python273
import oracleconnect as oracon
import generateHtml  as html
import string
import shutil

constr='demo/demo@statsdb.3pardata.com:1521/statscentral.3pardata.com';
oraconn=oracon.openconnect(constr);

# Main Page
sqlstmt='SELECT distinct INSERVSERIAL,MODEL,substr(OSVERSION,1,5),COMPANY,NUMNODES,to_char(INSTALLDATE,\'mm/dd/YYYY\'),';
sqlstmt+='round(TOTAL_SPACE/1024,2) total_space_gb,round(TOTAL_FREE_SPACE/1024,2) total_free_space_gb,round(PERCENT_UTILIZED,2) percent_used,';
sqlstmt+='round(FC_TOTAL_SIZE_MB/1024,2) FC_TOTAL_SIZE_GB,';
sqlstmt+='case when FC_TOTAL_SIZE_MB > 0 then round(((FC_TOTAL_SIZE_MB-FC_FREE_SIZE_MB)/FC_TOTAL_SIZE_MB)*100,2) else 0 end FC_PERS_SIZE_GB,';
sqlstmt+='round(NL_TOTAL_SIZE_MB/1024,2) NL_TOTAL_SIZE_GB, ';
sqlstmt+='case when NL_TOTAL_SIZE_MB >0 then round(((NL_TOTAL_SIZE_MB-NL_FREE_SIZE_MB)/NL_TOTAL_SIZE_MB)*100,2) else 0 end NL_PERS_SIZE_GB,';
sqlstmt+='round(SSD_TOTAL_SIZE_MB/1024,2) SSD_TOTAL_SIZE_GB,';
sqlstmt+='case when SSD_TOTAL_SIZE_MB > 0 then round(((SSD_TOTAL_SIZE_MB-SSD_FREE_SIZE_MB)/SSD_TOTAL_SIZE_MB)*100,2) else 0 end SSD_PERS_SIZE_GB ';
sqlstmt+='FROM LEGACY.CAPACITY_REPORT where nvl(total_space,0)>0 Order by round(TOTAL_SPACE/1024,2) desc';


datacur=oracon.execSql(oraconn, sqlstmt);

#datfmt="%Y%m%d%H%M%S";
#dt=datetime.datetime.now();
#d = datetime.datetime.strptime(dt, format);
htfl='index.html';
htmlfl=open(htfl,'w');
dataString='';

#10234.html#12034;Dummy;Dummy;10945;1567|102345.html#120345;Dummy;Dummy;10945;1567


strList =[];
for row in datacur:
    newrow = [];
    i=0;
    while i < len(row):
        if i==0:
            newrow.append(str(row[i])+'.html'+'#'+str(row[i]));
        else:     
            newrow.append(str(row[i]));
        i+=1;
    subStrDat=string.join(newrow,';');
    strList.append(subStrDat);
datString=string.join(strList,'|');
headStr='Serial Number,Model,Inform OS,Company,Number of Nodes,Installation Date,Installed Capacity in GB,Free Space in GB,% Used,FC Space in GB,FC % Used,NL Space in GB,NL % Used,SSD Space in GB,SSD % Used';
ht=html.generateHtmlHeader('Capacity Report','Capacity Report For HP-3PAR Systems by Installed Capacity');
ht+=html.generateHtmlTableHeader(headStr,0);
ht+=html.generateHtmlDataTable(datString,0);
ht+='</body>\n</html>';
htmlfl.write(ht);
shutil.copyfile(htfl,'/var/www/html/demo/'+htfl);
htmlfl.close;
oraconn.close();    

