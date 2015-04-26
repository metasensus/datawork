#!/usr/bin/env python273
import string
import oracleconnect as oracon
import generateHtml as html
import shutil

def build_summary(inservserial,oraconn):
    sqlstmt='select distinct round(total_space/1024/1024,2),round((total_space-total_free_space)/1024/1024,2),';
    sqlstmt+='round(((total_space-total_free_space)/total_space)*100,2),total_disks,fc_num,nl_num,ssd_num,';
    sqlstmt+='round(fc_total_size_mb/1024,2),case when (fc_total_size_mb >0) then round(((fc_total_size_mb-fc_free_size_mb)/fc_total_size_mb)*100,2) else 0 end,';
    sqlstmt+='round(nl_total_size_mb/1024,2),case when (nl_total_size_mb >0) then round(((nl_total_size_mb-nl_free_size_mb)/nl_total_size_mb)*100,2) else 0 end,';
    sqlstmt+='round(ssd_total_size_mb/1024,2),case when (ssd_total_size_mb>0) then round(((ssd_total_size_mb-ssd_free_size_mb)/ssd_total_size_mb)*100,2) else 0 end';
    sqlstmt+=' from legacy.capacity_report where inservserial=\''+str(inservserial)+'\'';
    ht='<table class="gridtable"><tr><td>';
    
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
    
    sqlstmt='select count(distinct vvid) from legacy.vv_dist_max where inservserial='+str(inservserial);
    vvcur=oracon.execSql(oraconn,sqlstmt);
    for vrec in vvcur:
        ht+='Total number of VV\'s:'+str(vrec[0]);
    
    sqlstmt='select distinct round(avg_util_perday/1024,2),number_of_days_till_zero from legacy.customer_inserv_space_proj where inservserial='+str(inservserial);
    projcur=oracon.execSql(oraconn, sqlstmt);
    for projrow in projcur:
        ht+='</td></tr><tr><td>';
        ht+='Average utilization (in GB per day) :'+str(projrow[0]);
        ht+='</td></tr><tr><td>';
        ht+='Projected Full Capacity (in days) :'+str(projrow[1]);
    ht+='</td></tr></table>';
    return ht;

def build_soft(inservserial,oraconn):
    sqlstmt='select distinct licensedesc,nvl(licensestatus,\'Valid\') from legacy.config_inserv_soft where to_char(inservserial)=\''+str(inservserial)+'\'';
    softcur=oracon.execSql(oraconn, sqlstmt);
    newrow=[];
    ht='';
    for sftrow in softcur:
        newrow.append(str(sftrow[0])+';'+str(sftrow[1]));
    if len(newrow) > 0:
        datString = string.join(newrow,'|');
        headStr='Software,License';
        ht+=html.generateHtmlTableHeader(headStr,0);
        ht+=html.generateHtmlDataTable(datString,0);
    else:
        ht+='';
    return ht;

def build_vv(inservserial,oraconn):
    # number of vvs over time
    sqlstmt='select distinct to_char(datadate,\'mm/dd/YYYY\'),vv_num from legacy.vv_agg where to_char(inservserial)=\''+str(inservserial)+'\'';
    vvcur=oracon.execSql(oraconn,sqlstmt);
    datrec=[];
    for vvrec in vvcur:
        datrec.append(str(vvrec[0])+':'+str(vvrec[1]));
    if len(datrec) > 0:    
        datStr=string.join(datrec,',');
        xmdat=html.generateHtmlGridLine(datStr,'Trend of VVs over time','# of VVs','Date',0);
        ht='';
        ht+='<div id="vvlinediv" align="center">VV Trend</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Line.swf", "ChartId", "1000", "500");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("vvlinediv");\n';
        ht+='\t</script>\n';
        return ht;
    else:
        return '';

def build_pd(inservserial,oraconn):
    ht='<strong>Physical Disk Installed by Cage</strong>'
    sqlstmt='select substr(cagepos,1,instr(cagepos,\':\',1,1)-1) cageid,pdtype,drivespeed,';
    sqlstmt+='capacity,count(1) numdrives from legacy.pdtype_info where to_char(inservserial)=\''+str(inservserial)+'\' group by pdtype,';
    sqlstmt+='substr(cagepos,1,instr(cagepos,\':\',1,1)-1),drivespeed,capacity order by 1'; 
    pdtypecur =oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    for pdrow in pdtypecur:
        newrow.append(str(pdrow[0])+';'+pdrow[1]+';'+str(pdrow[2])+';'+str(pdrow[3])+';'+str(pdrow[4]));
    if len(newrow) > 0:
        datString=string.join(newrow,'|')
        headStr='Cage Id,Drive Type,Drive Speed (in K rpm),Capacity in GB,# of Drives';
        ht+=html.generateHtmlTableHeader(headStr,0);
        ht+=html.generateHtmlDataTable(datString,0);
    else:
        ht+='';
    return ht;

def getinfo(inservserial,oraconn):
    sqlstmt='select * from (select distinct model,osversion,nvl(company,\'Unknown\') company,';
    sqlstmt+='min(nvl(installdate,(sysdate-(365*4)))) over (partition by inservserial),';
    sqlstmt+='rank() over (order by company) rnk ';
    sqlstmt+='from legacy.capacity_report where to_char(inservserial)=\''+str(inservserial)+'\') where rnk=1';
    headStr=',';
    datString='';
    ht=''
    ht+=html.generateHtmlTableHeader(headStr,1);
    infocur=oracon.execSql(oraconn,sqlstmt);
    for inrec in infocur:
        datString='Company :<strong>'+str(inrec[2])+'</strong>;Model :<strong>'+str(inrec[0])+'</strong>;Inform OS Version :<strong>'+str(inrec[1])+'</strong>';
    if len(datString)>0:
        ht+=html.generateHtmlDataTable(datString,1);
    else:
        ht=''
    return ht;

def getAlert(inservserial,oraconn):
    sqlstmt='select alert_message from legacy.inservalert where to_char(inservserial)=\''+str(inservserial)+'\'';
    datString='';
    ht=''
    headStr='Alert';
    ht+=html.generateHtmlTableHeader(headStr,1);
    alertcur=oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    for arow in alertcur:
        newrow.append(str(arow[0]));
    if len(newrow) > 0:
        datString=string.join(newrow,'|')
        ht+=html.generateHtmlDataTable(datString,1);
    else:
        ht='';
    return ht;

def getMajorAlert(inservserial,oraconn):
    sqlstmt='select a.alerttime, a.alerttype,trim(A.ALERTMSG),a.alertrepeatcount from legacy.alertnew a,';
    sqlstmt+='(select inservserial,max(datadate) mxdt from legacy.alertnew group by inservserial) ';
    sqlstmt+='b where upper(a.alertsvrty)=\'MAJOR\' and a.datadate=b.mxdt and a.inservserial=b.inservserial and to_char(a.inservserial)=\''+str(inservserial)+'\'';
    datString='';
    alertcur=oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    for arow in alertcur:
        newrow.append(str(arow[0])+';'+str(arow[1])+';'+str(arow[2])+';'+str(arow[3]));
    if len(newrow) > 0:
        ht='<strong>Latest Major Alerts</strong>'
        headStr='Alert Time,Alert Type,Message,Repeats';
        ht+=html.generateHtmlTableHeader(headStr,0);
        datString=string.join(newrow,'|')
        ht+=html.generateHtmlDataTable(datString,0);
    else:
        ht='';
    return ht;

def getExecReportOsRatio(oraconn):
    sqlstmt='select os,round((count_by_os/total_count)*100,2) os_pers from ';
    sqlstmt+=' (select nvl(os,\'Unknown\') os,count(distinct inservserial) count_by_os from ';
    sqlstmt+=' (select case when os like \'2.1%\' or os =\'2.2.1\' or os =\'2.2.2\' or os=\'2.2.3\' then \'less than 2.2.4\' else os end os,inservserial from '; 
    sqlstmt+=' (select a.inservserial,substr(trim(nvl(substr(os_rel,1,5),substr(osver,1,5))),1,5) os from legacy.all_inserv_master a left join ';
    sqlstmt+=' (select distinct inservserial,case when component like \'%2%\' then trim(substr(component,length(\'kernel\')+1,length(component)))||\' \'||componentver '; 
    sqlstmt+=' when component like \'%3%\' then trim(substr(component,length(\'kernel\')+1,length(component)))||\' \'||componentver ';
    sqlstmt+=' else componentver end osver  ';
    sqlstmt+=' from legacy.ver_max where upper(ver_max.component) like \'KERNEL%\') b ';
    sqlstmt+=' on to_char(a.inservserial)=to_char(b.inservserial))) group by nvl(os,\'Unknown\')) sub, ';
    sqlstmt+=' (select count(distinct inservserial) total_count from legacy.all_inserv_master) tot ';
    datString='';
    ht='';
    oscur=oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    headrow=[];
    for orec in oscur:
        headrow.append(str(orec[0]));
        newrow.append(str(orec[1]));
    headStr=string.join(headrow,' , ');
    datStr=string.join(newrow,',');
    xmdat=html.generateXmlPie(datStr,headStr,'',0);
    ht+='<div id="piediv" align="center">Inform Os Across Install Base</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "400", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("piediv");\n';
    ht+='\t</script>\n';
    oscur.close();
    return ht;

def searcharray(array,searchval):
    for ar in array:
        if searchval == ar:
            return 1
    return 0;    

def getOsQtrlyTrend(oraconn):
    datString='';
    datrow=[];
    category=[];
    series=[];
    sqlstmt='';
    
    sqlstmt='select distinct os_version from legacy.display_trend_table where sortq >= 20091 order by os_version';
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select distinct qtr,os_version,inserv_num,sortq from legacy.display_trend_table where os_version=\''+catrec[0]+'\' and sortq >= 20091 order by sortq' ;
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,'Q '+ostrec[3]) == 0:
                category.append('Q '+ostrec[3]);
            datrow.append(ostrec[1]+':'+str(ostrec[2])+':'+str(ostrec[3])+ostrec[1]+'.html');     
    ht='';
    xmdat=html.generateMSLine(category,series,datrow,3000,'','1');
    ht+='<div id="msdiv" align="center">Inform Os Across Install Base</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSLine.swf", "ChartId", "800", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("msdiv");\n';
    ht+='\t</script>\n';
    return ht;

def getLargestInstalledSys(oraconn):
    sqlstmt='select company,inservserial,installdate,Round((total_space/1024)/1024,2),round((used_space/1024)/1024,2),rnk from (';
    sqlstmt+='select company,inservserial,installdate,total_space,(total_space-total_free_space) used_space,dense_rank() over(order by total_space desc) rnk  from '
    sqlstmt+='(select * from legacy.capacity_report where upper(company) not like \'%HEWLETT-PACKARD%\')) where rnk <=5';
    
    lrgCur=oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    ht='';
    for lrgrow in lrgCur:
        newrow.append(str(lrgrow[0])+';'+str(lrgrow[1])+';'+str(lrgrow[2])+';'+str(lrgrow[3])+';'+str(lrgrow[4])+';'+str(lrgrow[5]));
    if len(newrow) > 0:
        datString = string.join(newrow,'|');
        headStr='Company,Serial Number,Installation Date,Capacity (in Tb),Used Capacity (in Tb),Rank by size';
        ht+=html.generateHtmlTableHeader(headStr,0);
        ht+=html.generateHtmlDataTable(datString,0);
    else:
        ht+='';
    return ht;

def getSysByPecentUsed(oraconn):
    sqlstmt='select capacity_used_tier,round((num_inserv/total_ins)*100,2) percent_ins from';
    sqlstmt+='(select  capacity_used_tier,count(distinct inservserial) Num_inserv from (';   
    sqlstmt+='select inservserial, case when percent_utilized > 90 then \' 90% - 100%\'';  
    sqlstmt+=' when percent_utilized between 80 and 90 then \' 80% -  90%\' ';   
    sqlstmt+=' when percent_utilized between 70 and 80 then \' 70% -  80%\'';
    sqlstmt+=' when percent_utilized between 60 and 70 then \' 60% -  70%\'';
    sqlstmt+=' when percent_utilized between 50 and 60 then \' 50% -  60%\'';
    sqlstmt+=' when percent_utilized between 40 and 50 then \' 40% -  50%\'';
    sqlstmt+=' when percent_utilized between 30 and 40 then \' 30% -  40%\'';
    sqlstmt+=' when percent_utilized between 20 and 30 then \' 20% -  30%\'';
    sqlstmt+=' else \' 0% - 20%\' end capacity_used_tier';
    sqlstmt+=' from legacy.capacity_report  where total_space > 0) group by capacity_used_tier) a,';
    sqlstmt+='(select count(distinct inservserial) total_ins from legacy.capacity_report where total_space > 0) b ';
    datString='';
    ht='';
    
    newrow=[];
    headrow=[];
    
    capuscur=oracon.execSql(oraconn,sqlstmt);
    for caprec in capuscur:
        headrow.append(str(caprec[0]));
        newrow.append(str(caprec[1]));

    headStr=string.join(headrow,' , ');
    datStr=string.join(newrow,',');
    xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30);
    ht+='<div id="newpiediv" align="center">Inform Os Across Install Base</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Bar2D.swf", "ChartId", "500", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("newpiediv");\n';
    ht+='\t</script>\n';
    return ht;

def getCapacityInstalledByDiskType(oraconn):
    sqlstmt='SELECT \'FC\',round(sum(nvl(FC_TOTAL_SIZE_MB,0))/sum(total_space) * 100,2) FROM LEGACY.CAPACITY_REPORT UNION';
    sqlstmt+=' SELECT \'NL\',round(sum(nvl(NL_TOTAL_SIZE_MB,0))/sum(total_space) * 100,2) FROM LEGACY.CAPACITY_REPORT UNION';
    sqlstmt+=' SELECT \'SSD\',round(sum(nvl(SSD_TOTAL_SIZE_MB,0))/sum(total_space) * 100,2) FROM LEGACY.CAPACITY_REPORT';
   
    datString='';
    ht='';
    
    newrow=[];
    headrow=[];
    
    diskcur=oracon.execSql(oraconn,sqlstmt);
    for diskrec in diskcur:
        headrow.append(str(diskrec[0]));
        newrow.append(str(diskrec[1]));

    headStr=string.join(headrow,' , ');
    datStr=string.join(newrow,',');
    xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30);
    ht+='<div id="diskpiediv" align="center">Installed Capacity By Disk Type</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Bar2D.swf", "ChartId", "400", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("diskpiediv");\n';
    ht+='\t</script>\n';
    return ht;

def getSystemsByCountry(oraconn):
    sqlstmt='select country,sum(num_inserv) num_inserv from ';
    sqlstmt+=' (select country,num_inserv from (';
    sqlstmt+=' select nvl(country,\'Unknown\') country,count(distinct inservserial) num_inserv';
    sqlstmt+=' from legacy.capacity_report group by nvl(country,\'Unknown\')'; 
    sqlstmt+=' )) group by country order by 2 desc';
    
    datString='';
    ht='';
    
    newrow=[];
    headrow=[];
    
    diskcur=oracon.execSql(oraconn,sqlstmt);
    for diskrec in diskcur:
        headrow.append(str(diskrec[0]));
        newrow.append(str(diskrec[1]));

    headStr=string.join(headrow,' , ');
    datStr=string.join(newrow,',');
    xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30);
    ht+='<div id="Countrypiediv" align="center">Systems by Country</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Column2D.swf", "ChartId", "3500", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("Countrypiediv");\n';
    ht+='\t</script>\n';
    return ht;

def getNumSystemsNearCapacity(oraconn):
    sqlstmt='select count(distinct inservserial) from customer_inserv_space_proj where numdays<=30';
    
    
    datString='';
    ht=''
    headStr='Alert';
    ht+=html.generateHtmlTableHeader(headStr,1);
    projcur=oracon.execSql(oraconn,sqlstmt);
    newrow=[];
    for proj in projcur:
        newrow.append(str(proj[0]));
    if len(newrow) > 0:
        datString=string.join(newrow,'|')
        ht+=html.generateHtmlDataTable(datString,1);
    else:
        ht='';
    return ht;

def getTotInstalledCapacityWeekly(oraconn):
    sqlstmt='select to_char(datadate,\'YYYYIW\') week,round(((sum(total_space)/1024)/1024)/1024,2) total_space_pb ';  
    sqlstmt+=' from legacy.inservspacetrend where to_char(datadate,\'YYYYIW\') between 201217 ';
    sqlstmt+=' and to_char(sysdate-15,\'YYYYIW\')  group by to_char(datadate,\'YYYYIW\') order by 1 asc ';
    
    instcur=oracon.execSql(oraconn,sqlstmt);
    datrec=[];
    for inrec in instcur:
        datrec.append(str(inrec[0])+':'+str(inrec[1]));
    if len(datrec) > 0:    
        datStr=string.join(datrec,',');
        xmdat=html.generateHtmlGridLine(datStr,'','Capacity in Petabytes','Week Year',1200,1);
        ht='';
        ht+='<div id="instlinediv" align="center">Total Space in PB installed by Week</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Area2D.swf", "ChartId", "1300", "300");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("instlinediv");\n';
        ht+='\t</script>\n';
        return ht;
    else:
        return '';
    
 
def getSpaceByRaidType(oraconn):
    sqlstmt='';
    sqlstmt+='SELECT \'Raid \'||RAIDTYPE,PERCENTALLOC FROM LEGACY.PERCENTSPACEBYRAIDTYPE';

    raidcur=oracon.execSql(oraconn,sqlstmt);
    
    newrow=[];
    headrow=[];
    for rrec in raidcur:
        headrow.append(str(rrec[0]));
        newrow.append(str(rrec[1]));
   
    if len(newrow) >0:
        headStr=string.join(headrow,' , ');
        datStr=string.join(newrow,',');
        xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30,'FF0000,33FF00,AFD8F8,F6BD0F,8BBA00,FF8E46,008E8E,D64646,8E468E,588526,B3AA00,008ED6,9D080D');
        ht='';
        ht+='<div id="raiddiv" align="center">Space by Raid Type</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "400", "300");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("raiddiv");\n';
        ht+='\t</script>\n';
        return ht;
    else:
        return '';

def getInservSpaceUtil(oraconn,inservserial):
    ht='';
    sqlstmt='select count(1) from legacy.capacity_report where inservserial=\''+str(inservserial)+'\'';
    
    checkcur=oracon.execSql(oraconn,sqlstmt);
    
    for ckcur in checkcur:
        if ckcur[0]==0:
            return '';
    
    sqlstmt='select nvl(total_space,0) from legacy.capacity_report where inservserial=\''+str(inservserial)+'\'';
    
    checkcur=oracon.execSql(oraconn,sqlstmt);
    
    for ckcur in checkcur:
        if ckcur[0]==0:
            return '';
    
    sqlstmt='select round(((total_space-total_free_space)/total_space)*100,2) used_pers,round((total_free_space/total_space)*100,2) free_pers from';
    sqlstmt+='(Select distinct total_space,total_free_space from legacy.capacity_report where inservserial=\''+inservserial+'\')';
    
    capcur=oracon.execSql(oraconn,sqlstmt);
    
    for caprec in capcur:
        datStr=str(caprec[0])+','+str(caprec[1]);
        colStr='FF0000,33FF00';
        
        headStr='%Used Space,%Free Space';
        xmdat=html.generateXmlPie(datStr,headStr,'',1,0,50,colStr);
        ht+='<fieldset><legend>Capacity Utilization</legend>'
        ht+='<div id="piediv" align="center"></div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "300", "150");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("piediv");\n';
        ht+='\t</script>\n</fieldset>\n';
        ht+='</td>\n<td>\n';
    return ht;

def getutilByDiskType(oraconn,inservserial):
    ht='';
    sqlstmt='select count(1) from legacy.capacity_report where inservserial=\''+str(inservserial)+'\'';
    
    checkcur=oracon.execSql(oraconn,sqlstmt);
    
    for ckcur in checkcur:
        if ckcur[0]==0:
            return '';
    
    sqlstmt='select nvl(total_space,0) from legacy.capacity_report where inservserial=\''+str(inservserial)+'\'';
    
    checkcur=oracon.execSql(oraconn,sqlstmt);
    
    for ckcur in checkcur:
        if ckcur[0]==0:
            return '';
    
    sqlstmt='Select distinct round(fc_free_size_mb/1024,2) fc_free_gb,round(nl_free_size_mb/1024,2) nl_free_gb,';
    sqlstmt+='round(ssd_free_size_mb/1024,2) ssd_tot_gb,round((fc_total_size_mb-fc_free_size_mb)/1024,2) fc_used_gb,';
    sqlstmt+=' round((nl_total_size_mb-nl_free_size_mb)/1024,2) nl_use_gb,round((ssd_total_size_mb-ssd_free_size_mb)/1024,2) ssd_used_gb '
    sqlstmt+=' from legacy.capacity_report where inservserial=\''+inservserial+'\'';
    
    dskcur=oracon.execSql(oraconn,sqlstmt);
    for dskrec in dskcur:
        datStr='Used:'+str(dskrec[3])+',Used:'+str(dskrec[4])+',Used:'+str(dskrec[5])+',Free:'+str(dskrec[0])+',Free:'+str(dskrec[1])+',Free:'+str(dskrec[2]);
            
    catstr='FC,NL,SSD';
    tagStr='Used,Free'
    colStr='AFD8F8,8BBA00';
    
    
    xmdat=html.generateXmlStBar(datStr,tagStr,catstr,colStr,'');
    ht+='<fieldset><legend>Disk type utilization</legend>'
    ht+='<div id="stckdiv" align="center"></div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_StackedBar2D.swf", "ChartId", "800", "200");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("stckdiv");\n';
    ht+='\t</script>\n</fieldset>\n';
    return ht

def build_disktype_free_trend(inservserial,oraconn):
    # number of vvs over time
    datString='';
    datrow=[];
    category=[];
    series=[];
    sqlstmt='';
    
    sqlstmt ='select count(1) from legacy.pd_type_daily_freetrend where to_char(inservserial)=\''+str(inservserial)+'\'';
    checkcur=oracon.execSql(oraconn,sqlstmt);
    for ckcur in checkcur:
        if ckcur[0] == 0:
            return ''
    
    sqlstmt ='select max(round((free/1024)/1024,0))+10 from legacy.pd_type_daily_freetrend where to_char(inservserial)=\''+str(inservserial)+'\'';
    maxcur=oracon.execSql(oraconn,sqlstmt);
    for mcur in maxcur:
        maxval=mcur[0];
    
    sqlstmt='select distinct pdtype from legacy.pd_type_daily_freetrend where to_char(inservserial)=\''+str(inservserial)+'\' order by 1';
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select to_char(datadate,\'mm/dd/YYYY\') dt,pdtype,round((free/1024)/1024,2) from legacy.pd_type_daily_freetrend ';
        isqlstmt+=' where to_char(inservserial)=\''+str(inservserial)+'\' AND pdtype=\''+catrec[0]+'\' and ';
        isqlstmt+=' datadate in (select datadate from (select datadate from legacy.pd_type_daily_freetrend where to_char(inservserial)= \''+str(inservserial)+'\'';
        isqlstmt+=' order by datadate desc) where rownum < 31 ) order by 1' ;
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[0]) == 0:
                category.append(ostrec[0]);
            datrow.append(ostrec[1]+':'+str(ostrec[2]));     
    ht='';
    xmdat=html.generateMSLine(category,series,datrow,maxval,'','1');
    ht+='<fieldset><legend>Historical Free Capacity in TB</legend>'
    ht+='<div id="msdiv" align="center">Historical Free Capacity</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSLine.swf", "ChartId", "500", "200");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("msdiv");\n';
    ht+='\t</script>\n</fieldset>';
    return ht;

def getInservSpaceByRaidType(oraconn):
    sqlstmt='';
    sqlstmt+='SELECT \'Raid \'||RAIDTYPE,PERCENTALLOC FROM LEGACY.PERCENTSPACEBYRAIDTYPE';

    raidcur=oracon.execSql(oraconn,sqlstmt);
    
    newrow=[];
    headrow=[];
    for rrec in raidcur:
        headrow.append(str(rrec[0]));
        newrow.append(str(rrec[1]));
   
    if len(newrow) >0:
        headStr=string.join(headrow,' , ');
        datStr=string.join(newrow,',');
        xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30,'FF0000,33FF00,AFD8F8,F6BD0F,8BBA00,FF8E46,008E8E,D64646,8E468E,588526,B3AA00,008ED6,9D080D');
        ht='';
        ht+='<div id="raiddiv" align="center">Space by Raid Type</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "400", "300");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("raiddiv");\n';
        ht+='\t</script>\n';
        return ht;
    else:
        return '';

def getNumDisks(oraconn):
    sqlstmt='';
    sqlstmt+='select * from (select \'FC\' DiskType, round((fc_disks/total_disks*100),2) percent from ';
    sqlstmt+=' (select sum(total_disks) total_disks,sum(fc_num) fc_disks from legacy.capacity_report  where total_disks is not null) union ';
    sqlstmt+=' select \'NL\' DiskType,round((nl_disks/total_disks*100),2) percent from    ';
    sqlstmt+=' (select sum(total_disks) total_disks,sum(nl_num) nl_disks from legacy.capacity_report where total_disks is not null) ';
    sqlstmt+=' union  select \'SSD\' DiskType,round((ssd_disks/total_disks*100),2) percent from ';
    sqlstmt+=' (select sum(total_disks) total_disks,sum(ssd_num) ssd_disks from legacy.capacity_report where total_disks is not null))';
    
    datString='';
    ht='';
    
    newrow=[];
    headrow=[];
    
    diskcur=oracon.execSql(oraconn,sqlstmt);
    for diskrec in diskcur:
        headrow.append(str(diskrec[0]));
        newrow.append(str(diskrec[1]));

    headStr=string.join(headrow,' , ');
    datStr=string.join(newrow,',');
    xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30);
    ht+='<div id="disknumpiediv" align="center">Number of Disks by Disk Type</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Bar2D.swf", "ChartId", "550", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("disknumpiediv");\n';
    ht+='\t</script>\n';
    return ht;

def getDiskByModel(oraconn):
    sqlstmt='select  MODEL, FC_PERCENT, NL_PERCENT, SSD_PERCENT from legacy.diskdistbymodel order by model';
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    category=[];
    series=[];
    datrow=[];
    for catrec in oscatcur:
        category.append(catrec[0]);
        
    isqlstmt='select \'FC\' drivetype, FC_PERCENT from legacy.diskdistbymodel order by model' ;
        
    ostrendcur=oracon.execSql(oraconn,isqlstmt);
    for ostrec in ostrendcur:
        series.append(ostrec[0]);
        datrow.append(ostrec[0]+':'+str(ostrec[1]));
            
    isqlstmt=' select \'NL\' drivetype, NL_PERCENT from legacy.diskdistbymodel order by model';
        
    ostrendcur=oracon.execSql(oraconn,isqlstmt);
    for ostrec in ostrendcur:
        series.append(ostrec[0]);
        datrow.append(ostrec[0]+':'+str(ostrec[1]));
            
    isqlstmt='select \'SSD\' drivetype, SSD_PERCENT from legacy.diskdistbymodel order by model ';
        
    ostrendcur=oracon.execSql(oraconn,isqlstmt);
    for ostrec in ostrendcur:
        series.append(ostrec[0]);
        datrow.append(ostrec[0]+':'+str(ostrec[1]));
        
    ht='';
    #print category;
    #print series;
    #print datrow;
    
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="modeldiv" align="center">Disk Type by model</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSBar2D.swf", "ChartId", "1000", "300");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("modeldiv");\n';
    ht+='\t</script>\n';
    return ht;

def getPenetrationByModel(oraconn):
    sqlstmt='';
    sqlstmt+='select m,round(ct/tct*100,2) from (select substr(model,1,1) m,count(1) ct from legacy.capacity_report ';
    sqlstmt+=' where model!=\'Unknown\' group by substr(model,1,1)) a,(select count(1) tct from legacy.capacity_report where model!=\'Unknown\') b';

    mcur=oracon.execSql(oraconn,sqlstmt);
    
    newrow=[];
    headrow=[];
    for rrec in mcur:
        headrow.append(str(rrec[0])+' - Class');
        newrow.append(str(rrec[1]));
   
    if len(newrow) >0:
        headStr=string.join(headrow,' , ');
        datStr=string.join(newrow,',');
        xmdat=html.generateXmlPie(datStr,headStr,'',1,1,30,'FF0000,33FF00,AFD8F8,F6BD0F,8BBA00,FF8E46,008E8E,D64646,8E468E,588526,B3AA00,008ED6,9D080D');
        ht='';
        ht+='<div id="mdiv" align="center">Space by Raid Type</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "400", "300");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("mdiv");\n';
        ht+='\t</script>\n';
        return ht;
    else:
        return '';

def getSoftwareQtrAttachRate(oraconn):
    
    datString='';
    datrow=[];
    category=[];
    series=[];
    sqlstmt='';
    
    sqlstmt='select distinct qtr_year from legacy.Qtrly_soft_attach_trend ';
    sqlstmt+='  order by to_number(trim(substr(qtr_year,length(qtr_year)-4,5))||\'0\'||trim(substr(qtr_year,2,2)))';
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select qtr_year,software,install_pers,to_number(trim(substr(qtr_year,length(qtr_year)-4,5))||\'0\'||trim(substr(qtr_year,2,2))) qtr '
        isqlstmt+=' from legacy.Qtrly_soft_attach_trend where upper(software) NOT IN (\'GOLDEN LICENSE\',\'RECOVERY MANAGER FOR EXCHANGE \'||\'&\'||\' ORACLE\')';
        isqlstmt+=' and qtr_year=\''+catrec[0]+'\' order by ' ;
        isqlstmt+=' to_number(trim(substr(qtr_year,length(qtr_year)-4,5))||\'0\'||trim(substr(qtr_year,2,2)))';
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[1]) == 0:
                category.append(ostrec[1]);
            datrow.append(catrec[0]+':'+str(ostrec[2]));     
    ht='';
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="mscoldiv" align="center">Inform Os Across Install Base</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSColumn2D.swf", "ChartId", "1500", "600");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
    ht+='\tchart.render("mscoldiv");\n';
    ht+='\t</script>\n';
    return ht;
 
def getOsByQtr(oraconn):
    sqlstmt='select \'Q\'||\' \'||to_char(config_date,\'YYYYQ\') qtr,substr(b.model,1,1)||\'-CLASS\' model,a.attr_value,';
    sqlstmt+=' count(distinct a.inservserial) num_inserv,to_char(config_date,\'YYYYQ\') from legacy.INSERV_CONFIG_FIX a, legacy.all_inserv_master b ';
    sqlstmt+=' where trim(a.inservserial)=trim(b.inservserial) ';
    sqlstmt+=' group by \'Q\'||\' \'||to_char(config_date,\'YYYYQ\'),substr(b.model,1,1)||\'-CLASS\',a.attr_value,to_char(config_date,\'YYYYQ\') order by 1,3,2';
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    headrow=[];
    newrow=[];
    
    datstr='';
    headstr='';
    
    ht='';
    
    for osrec in oscatcur:
        osverfl =osrec[4]+osrec[2]+'.html';
        osfl=open(osverfl,'w');
        
        isqlstmt='select \'Q\'||\' \'||to_char(config_date,\'YYYYQ\') qtr,substr(b.model,1,1)||\'-CLASS\' model,a.attr_value,';
        isqlstmt+=' count(distinct a.inservserial) num_inserv from legacy.INSERV_CONFIG_FIX a, legacy.all_inserv_master b ';
        isqlstmt+='where trim(a.inservserial)=trim(b.inservserial) and to_char(config_date,\'YYYYQ\')=\''+osrec[4]+'\' and ATTR_VALUE=\''+osrec[2]+'\'';
        isqlstmt+=' group by \'Q\'||\' \'||to_char(config_date,\'YYYYQ\'),substr(b.model,1,1)||\'-CLASS\',a.attr_value order by 1,3';
        
        suboscur=oracon.execSql(oraconn,isqlstmt);
        
        for subrec in suboscur:
            headrow.append(str(subrec[1]));
            newrow.append(str(subrec[3]));
        
        headStr=string.join(headrow,' , ');
        datStr=string.join(newrow,',');
        caption='Q '+osrec[4]+' for Inform OS '+osrec[2];
        xmdat=html.generateXmlPie(datStr=datStr,tagStr=headStr,capt=caption,shownames=1,isbar=1,yaxismaxvalue=2000);
        ht+='<head>';
        ht+='<script language="JavaScript" src="../FusionChartsFree/JSClass/FusionCharts.js"></script>';
        ht+='</head>';
        ht+='<body>';
        ht+='<div id="osmodel" align="center">Quarter '+osrec[0]+' OS by Model</div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Bar2D.swf", "ChartId", "700", "300");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("osmodel");\n';
        ht+='\t</script>\n';
        ht+='</body>';
        ht+='</html>';
        osfl.write(ht)
        osfl.close();
        ht='';
        shutil.move(osverfl,'/var/www/html/exec/'+osverfl);
        headrow=[];
        newrow=[];
        datstr='';
        headstr='';
        
def getWWByRapCategory(oraconn):
    sqlstmt='select distinct rap_days||\' days \',rap_ord from legacy.supply_chain_region_rap where region=\'WW\' order by rap_ord';
    
    datString='';
    datrow=[];
    category=[];
    series=[];
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select rap_category,rap_count from legacy.supply_chain_region_rap where region=\'WW\' and rap_ord='+str(catrec[1])+' order by rap_category';
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[0]) == 0:
                category.append(ostrec[0]);
            datrow.append(catrec[0]+':'+str(ostrec[1]));     
    
    ht='';
    
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="mswwdiv" align="center">WW RAP by RAP category</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSColumn2D.swf", "ChartId", "700", "500");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';                  
    ht+='\tchart.render("mswwdiv");\n';
    ht+='\t</script>\n';
    return ht;
        
def getEMEAByRapCategory(oraconn):
    sqlstmt='select distinct rap_days||\' days \',rap_ord from legacy.supply_chain_region_rap where region=\'EMEA\' order by rap_ord';
    
    datString='';
    datrow=[];
    category=[];
    series=[];
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select rap_category,rap_count from legacy.supply_chain_region_rap where region=\'EMEA\' and rap_ord='+str(catrec[1])+' order by rap_category';
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[0]) == 0:
                category.append(ostrec[0]);
            datrow.append(catrec[0]+':'+str(ostrec[1]));     
    
    ht='';
    
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="msemdiv" align="center">WW RAP by RAP category</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSColumn2D.swf", "ChartId", "700", "500");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';                  
    ht+='\tchart.render("msemdiv");\n';
    ht+='\t</script>\n';
    return ht;

def getAMSByRapCategory(oraconn):
    sqlstmt='select distinct rap_days||\' days \',rap_ord from legacy.supply_chain_region_rap where region=\'AMS\' order by rap_ord';
    
    datString='';
    datrow=[];
    category=[];
    series=[];
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select rap_category,rap_count from legacy.supply_chain_region_rap where region=\'AMS\' and rap_ord='+str(catrec[1])+' order by rap_category';
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[0]) == 0:
                category.append(ostrec[0]);
            datrow.append(catrec[0]+':'+str(ostrec[1]));     
    
    ht='';
    
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="msamdiv" align="center">WW RAP by RAP category</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSColumn2D.swf", "ChartId", "700", "500");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';                  
    ht+='\tchart.render("msamdiv");\n';
    ht+='\t</script>\n';
    return ht;
        
def getAPJByRapCategory(oraconn):
    sqlstmt='select distinct rap_days||\' days \',rap_ord from legacy.supply_chain_region_rap where region=\'APJ\' order by rap_ord';
    
    datString='';
    datrow=[];
    category=[];
    series=[];
    
    oscatcur=oracon.execSql(oraconn,sqlstmt);
    
    for catrec in oscatcur:
        series.append('\''+catrec[0]+'\'');
        isqlstmt='select rap_category,rap_count from legacy.supply_chain_region_rap where region=\'APJ\' and rap_ord='+str(catrec[1])+' order by rap_category';
        ostrendcur=oracon.execSql(oraconn,isqlstmt);
        for ostrec in ostrendcur:
            if searcharray(category,ostrec[0]) == 0:
                category.append(ostrec[0]);
            datrow.append(catrec[0]+':'+str(ostrec[1]));     
    
    ht='';
    
    xmdat=html.generateMSLine(category,series,datrow,100,'','1');
    ht+='<div id="msapdiv" align="center">WW RAP by RAP category</div>\n';
    ht+='<script type="text/javascript">\n';
    ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_MSColumn2D.swf", "ChartId", "700", "500");\n';
    ht+='\tchart.setDataXML("'+xmdat+'");\n';                  
    ht+='\tchart.render("msapdiv");\n';
    ht+='\t</script>\n';
    return ht;
        
        
        
        
        
    
        
    