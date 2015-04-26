#!/usr/bin/env python273
import shutil
import oracleconnect as oracon
import generateHtml  as html
import string
import sys

# Inserv Configuration Page

constr='demo/demo@statsdb.3pardata.com:1521/statscentral.3pardata.com';
oraconn=oracon.openconnect(constr);

sqlstmt='Select distinct inservserial from legacy.capacity_report order by 1';

datacur=oracon.execSql(oraconn, sqlstmt);
try:
    for row in datacur:
        hfl=str(row[0])+'.html'
        htfl=open(hfl,'w');
        ht=html.generateHtmlHeader('Configuration','Configuration for '+str(row[0]));
        ht+='<br/>\n';
        ht+='<table><tr><td>';
        ht+='<img src="InservImage.jpg" alt="Inserv" height="400" width="350" />\n';
        ht+='</td><td><table><tr><td>'
        
        sqlstmt='select distinct round(total_space/1024/1024,2),round((total_space-total_free_space)/1024/1024,2),';
        sqlstmt+='case when total_space>0 then round(((total_space-total_free_space)/total_space)*100,2) else 0 end,total_disks,fc_num,nl_num,ssd_num,';
        sqlstmt+='round(fc_total_size_mb/1024,2),case when (fc_total_size_mb >0) then round(((fc_total_size_mb-fc_free_size_mb)/fc_total_size_mb)*100,2) else 0 end,';
        sqlstmt+='round(nl_total_size_mb/1024,2),case when (nl_total_size_mb >0) then round(((nl_total_size_mb-nl_free_size_mb)/nl_total_size_mb)*100,2) else 0 end,';
        sqlstmt+='round(ssd_total_size_mb/1024,2),case when (ssd_total_size_mb>0) then round(((ssd_total_size_mb-ssd_free_size_mb)/ssd_total_size_mb)*100,2) else 0 end';
        sqlstmt+=' from legacy.capacity_report where inservserial=\''+str(row[0])+'\'';
        totcur=oracon.execSql(oraconn,sqlstmt);
        for totrow in totcur:
            ht+='Total Capacity (in TB) :'+str(totrow[0]);
            ht+='</td></tr><tr><td>';
            ht+='Total Used Space (in TB) :'+str(totrow[1]);                                     
            ht+='</td></tr><tr><td>';
            ht+='% used :'+str(totrow[2]);
            ht+='</td></tr><tr><td>';
            ht+='# of Disks :'+str(totrow[3]);
            ht+='</td></tr><tr><td>';
            ht+='# of FC Disks :'+str(totrow[4]);
            ht+='</td></tr><tr><td>';
            ht+='# of NL Disks :'+str(totrow[5]);
            ht+='</td></tr><tr><td>';
            ht+='# of SSD Disks :'+str(totrow[6]);
            ht+='</td></tr><tr><td>';
            ht+='FC Size (in GB) :'+str(totrow[7]);
            ht+='</td></tr><tr><td>';
            ht+='% used :'+str(totrow[8]);
            ht+='</td></tr><tr><td>';
            ht+='NL Size (in GB) :'+str(totrow[9]);
            ht+='</td></tr><tr><td>';
            ht+='% used :'+str(totrow[10]);
            ht+='</td></tr><tr><td>';
            ht+='SSD Size (in GB) :'+str(totrow[11]);
            ht+='</td></tr><tr><td>';
            ht+='% used :'+str(totrow[12]);
        
        sqlstmt='select distinct round(avg_util_perday/1024,2),number_of_days_till_zero from legacy.customer_inserv_space_proj where inservserial=\''+str(row[0])+'\'';
        projcur=oracon.execSql(oraconn, sqlstmt);
        for projrow in projcur:
            ht+='</td></tr><tr><td>';
            ht+='Average utilization (in GB per day) :'+str(projrow[0]);
            ht+='</td></tr><tr><td>';
            ht+='Projected Full Capacity (in days) :'+str(projrow[1]);
        
        sqlstmt='select count(1) from legacy.vv a,(select max(datadate) mxdt from legacy.vv where inservserial=\''+str(row[0])+'\'';
        sqlstmt+=')b where inservserial=+\''+str(row[0])+'\' and datadate=mxdt';
        vvcur=oracon.execSql(oraconn, sqlstmt);
        for vvrow in vvcur:
            ht+='</td></tr><tr><td>';
            ht+='# of VVs :'+str(vvrow[0]);
        ht+='</td></tr></table><td><table><tr><td>';   
        
        sqlstmt='select pdtype,drivespeed,capacity,numdrives,round((total-free)/total * 100,2) percent_free from ';
        sqlstmt+='(select pdtype,drivespeed,capacity,count(1) numdrives,sum(total) total,sum(free) free  ';
        sqlstmt+='from legacy.pdtype_info where inservserial=\''+str(row[0])+'\' group by  pdtype,drivespeed,capacity)';
        pdtypecur =oracon.execSql(oraconn,sqlstmt);
        newrow=[];
        for pdrow in pdtypecur:
            newrow.append(pdrow[0]+';'+str(pdrow[1])+';'+str(pdrow[2])+';'+str(pdrow[3])+';'+str(pdrow[4]));
        if len(newrow) > 0:
            datString=string.join(newrow,'|')
            headStr='Drive Type,Drive Speed (in K rpm),Capacity in GB,# of Drives,% Used';
            ht+=html.generateHtmlTableHeader(headStr,0);
            ht+=html.generateHtmlDataTable(datString);
        else:
            ht+='</tr></table>';
        ht+='</td></tr><tr>';
        
        sqlstmt='select substr(cagepos,1,instr(cagepos,\':\',1,1)-1) cageid,pdtype,drivespeed,';
        sqlstmt+='capacity,count(1) numdrives from legacy.pdtype_info where inservserial=\''+str(row[0])+'\' group by pdtype,';
        sqlstmt+='substr(cagepos,1,instr(cagepos,\':\',1,1)-1),drivespeed,capacity order by 1'; 
        pdtypecur =oracon.execSql(oraconn,sqlstmt);
        newrow=[];
        for pdrow in pdtypecur:
            newrow.append(str(pdrow[0])+';'+pdrow[1]+';'+str(pdrow[2])+';'+str(pdrow[3])+';'+str(pdrow[4]));
        if len(newrow) > 0:
            datString=string.join(newrow,'|')
            headStr='Cage Id,Drive Type,Drive Speed (in K rpm),Capacity in GB,# of Drives';
            ht+=html.generateHtmlTableHeader(headStr,0);
            ht+=html.generateHtmlDataTable(datString);
        else:
            ht+='</tr></table>';
        
        ht+='</td></tr></table><tr><td>';
        sqlstmt='select distinct software from legacy.config_inserv_soft where inservserial=\''+str(row[0])+'\'';
        softcur=oracon.execSql(oraconn, sqlstmt);
        newrow=[];
        for sftrow in softcur:
            newrow.append(sftrow[0]);
        if len(newrow) > 0:
            datString = string.join(newrow,'|');
            headStr='Software License';
            ht+=html.generateHtmlTableHeader(headStr,0);
            ht+=html.generateHtmlDataTable(datString);
        else:
            ht+='</tr></table>';
        ht+='</td></tr></table><table><tr><td>';
        sqlstmt='select distinct Component,componentver from legacy.systemInformSoftware where inservserial=\''+str(row[0])+'\'';
        compcur=oracon.execSql(oraconn, sqlstmt);
        newrow=[];
        for cmprow in compcur:
            newrow.append(string.join(cmprow,';'));       
        if len(newrow) > 0:
            headStr='OS Component,Component Version';
            ht+=html.generateHtmlTableHeader(headStr,0);
            datString = string.join(newrow,'|');    
            ht+=html.generateHtmlDataTable(datString);
        else:
            ht+='</tr></table>';
        ht+='</td></tr></table></td></table></body>\n</html>';
        htfl.write(ht);
        htfl.close;
        shutil.move(hfl,'/var/www/html/demo/'+hfl);
except:
    print "Inserv Error :"+str(row[0])
    print "Unexpected error:", sys.exc_info()[0];
    raise;
datacur.close();    

