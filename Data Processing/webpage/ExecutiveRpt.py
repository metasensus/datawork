#!/usr/bin/env python273
import string
import oracleconnect as oracon
import generateHtml as html
import report_data as report
import shutil

hfl ="index.html";
constr='demo/demo@statsdb.3pardata.com:1521/statscentral.3pardata.com';
oraconn=oracon.openconnect(constr);


with open(hfl,"w") as execrpt:
    ht="";
    ht+=html.generateHtmlWithSPHead('Executive Summary','Executive Dashboard','A Call Home Group Website');
    ht+='<br/>';
    tabList='General;Capacity;Disk Type;Software;Supply Chain';
    ht+=html.buildtab(tabList);
    tabval ='<table><tr><td><fieldset><legend>OS % Installed</legend>';
    tabval+=report.getExecReportOsRatio(oraconn);
    tabval+='</fieldset>';
    tabval+='</td><td>';
    tabval+='<fieldset><legend>Inform OS Installed Across Quarters From 2009</legend>';
    tabval+=report.getOsQtrlyTrend(oraconn);
    tabval+='</fieldset>';
    tabval+='</td>';
    tabval+='<td>';
    tabval+='<fieldset><legend>% of Installs by Model</legend>';
    tabval+=report.getPenetrationByModel(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr></table><table>';
    tabval+='<tr><td>';
    tabval+='<fieldset><legend>Number Of Systems Installed By Country</legend>';
    tabval+=report.getSystemsByCountry(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr></table>';
    ht+=html.addtabContent('General',tabval);
    tabval='<table><tr><td>';
    tabval+='<fieldset><legend>Top Five Systems by Installed Capacity</legend>';
    tabval+=report.getLargestInstalledSys(oraconn);
    tabval+='</fieldset>';
    tabval+='</td>\n<td>\n';
    tabval+='<fieldset><legend>Distribution Of Capacity Utilization</legend>';
    tabval+=report.getSysByPecentUsed(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr>\n';
    tabval+='</table>\n';
    tabval+='<table>\n';
    tabval+='<tr><td>\n';
    tabval+='<fieldset><legend>World Wide 3PAR Capacity Installed in Peta Bytes</legend>';
    tabval+=report.getTotInstalledCapacityWeekly(oraconn);
    tabval+='<center>Report Contributed by Iannaccone, Ivan (ivan.iannaccone@hp.com)</center>';
    tabval+='</fieldset>';
    tabval+='</td></tr></table>';
    ht+=html.addtabContent('Capacity',tabval);
    
    tabval='<table><tr><td>';
    tabval+='<fieldset><legend>Capacity Installed By Disk Type</legend>';
    tabval+=report.getCapacityInstalledByDiskType(oraconn);
    tabval+='</fieldset>';
    tabval+='</td>';
    tabval+='<td>';
    tabval+='<fieldset><legend>% Space installed by Raid Type</legend>';
    tabval+=report.getSpaceByRaidType(oraconn);
    tabval+='</fieldset>';
    tabval+='</td>';
    tabval+='<td>';
    tabval+='<fieldset><legend>% Number Of Disks by Disk Type</legend>'
    tabval+=report.getNumDisks(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr>';
    tabval+='</table>\n';
    tabval+='<table>\n';
    tabval+='<tr><td>';
    tabval+='<fieldset><legend>% Number Of Disks by Disk Type & Models</legend>'
    tabval+=report.getDiskByModel(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr>';
    tabval+='</table>\n';
    
    ht+=html.addtabContent('Disk Type',tabval);
    
    tabval='<table><tr><td>';
    tabval+='<fieldset><legend>Software Attach Rate</legend>';
    tabval+=report.getSoftwareQtrAttachRate(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr>';
    tabval+='</table>\n';
    
    ht+=html.addtabContent('Software',tabval);
    tabval+='<fieldset>';
    tabval='<table><tr><td>';
    tabval+='<fieldset><legend>WW RAP Category Report</legend>';
    tabval+=report.getWWByRapCategory(oraconn);
    tabval+='</fieldset>';
    tabval+='</td><td>';
    tabval+='<fieldset><legend>AMS RAP Category Report</legend>';
    tabval+=report.getAMSByRapCategory(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr><tr><td>';
    tabval+='<fieldset><legend>APJ RAP Category Report</legend>';
    tabval+=report.getAPJByRapCategory(oraconn);
    tabval+='</fieldset>';
    tabval+='</td><td>';
    tabval+='<fieldset><legend>EMEA RAP Category Report</legend>';
    tabval+=report.getEMEAByRapCategory(oraconn);
    tabval+='</fieldset>';
    tabval+='</td></tr>';
    tabval+='</table>\n';
    tabval+='<center>Report Contributed by Charles, Andy (andy.charles@hp.com)</center>';
    tabval+='</fieldset>';
    
    ht+=html.addtabContent('Supply Chain',tabval);
    
    ht+=html.addtabscript(tabList);
    
    #ht+=html.generateAddFieldset();
    ht+='</body>\n</html>';
    execrpt.write(ht);    
    execrpt.close();
    
report.getOsByQtr(oraconn);
shutil.move(hfl,'/var/www/html/exec/'+hfl);