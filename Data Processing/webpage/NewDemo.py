#!/usr/bin/env python273
import shutil
import oracleconnect as oracon
import generateHtml  as html
import string
import sys
import report_data as report

# Inserv Configuration Page

constr='demo/demo@statsdb.3pardata.com:1521/statscentral.3pardata.com';
oraconn=oracon.openconnect(constr);

sqlstmt='Select distinct inservserial from legacy.capacity_report where nvl(total_space,0)>0 order by 1';

datacur=oracon.execSql(oraconn, sqlstmt);
try:
    for row in datacur:
        print 'Processing.....:'+str(row[0])
        hfl=str(row[0])+'.html'
        htfl=open(hfl,'w');
        ht=html.generateHtmlwithTab('System','Details for '+str(row[0]));
        ht+='<tr><td>';
        ht+=report.getinfo(row[0],oraconn);
        ht+='</tr></td>';
        ht+='<tr><td>';
        ht+='<img src="3par_arrays.jpg" alt="Inserv" height="400" width="350" />';
        ht+='</td><td>';
        sqlstmt='select round(((total_space-total_free_space)/total_space)*100,2) used_pers,round((total_free_space/total_space)*100,2) free_pers from';
        sqlstmt+='(Select distinct total_space,total_free_space from legacy.capacity_report where inservserial=\''+str(row[0])+'\')';
        capcur=oracon.execSql(oraconn,sqlstmt);
        for caprec in capcur:
            datStr=str(caprec[0])+','+str(caprec[1]);
        
        colStr='FF0000,33FF00';
        
        headStr='%Used Space,%Free Space';
        xmdat=html.generateXmlPie(datStr,headStr,'',1,0,50,colStr);
        ht+='<fieldset><legend>Capacity Utilization</legend>'
        ht+='<div id="piediv" align="center"></div>\n';
        ht+='<script type="text/javascript">\n';
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Pie2D.swf", "ChartId", "400", "150");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("piediv");\n';
        ht+='\t</script>\n</fieldset>\n';
        ht+='</td>\n<td>\n';
 
        sqlstmt='Select distinct round(fc_free_size_mb/1024,2) fc_free_gb,round(nl_free_size_mb/1024,2) nl_free_gb,';
        sqlstmt+='round(ssd_free_size_mb/1024,2) ssd_tot_gb,round((fc_total_size_mb-fc_free_size_mb)/1024,2) fc_used_gb,';
        sqlstmt+=' round((nl_total_size_mb-nl_free_size_mb)/1024,2) nl_use_gb,round((ssd_total_size_mb-ssd_free_size_mb)/1024,2) ssd_used_gb '
        sqlstmt+=' from legacy.capacity_report where inservserial=\''+str(row[0])+'\'';
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
        ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_StackedBar2D.swf", "ChartId", "600", "200");\n';
        ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
        ht+='\tchart.render("stckdiv");\n';
        ht+='\t</script>\n<fieldset/>\n';
        #ht+='</td>\n</tr>\n<tr><td></td><td></td><td>';
        ht+='</td>\n<td>';
        ht+=report.getAlert(row[0],oraconn);
        ht+='</td></tr></table>\n';
        
        ht+='\t<ul id="tablist">\n';
        ht+='\t<li id="li_summary" onclick="tab(\'summary\')"><a class="current" href="#summary"  >Summary</a></li>\n'
        ht+='\t<li id="li_licenses" onclick="tab(\'licenses\')"><a href="#licenses">Licenses</a></li>\n';
        ht+='\t<li id="li_vv" onclick="tab(\'vv\')"><a href="#vv">Virtual Volumes</a></li>\n';
        ht+='\t<li id="li_pd" onclick="tab(\'pd\')"><a href="#pd">Physical Disk</a></li>\n';
        ht+='\t<li id="li_alerts" onclick="tab(\'alerts\')"><a href="#alerts">Alerts</a></li>\n';
        ht+='\t<li id ="li_rc" onclick="tab(\'rc\')"><a href="#rc">Remote Copy</a></li>\n';
        ht+='\t</ul>\n';

        ht+='<div class="content" id="summary">\n';
        ht+='\t<table><tr><td>'+report.build_summary(row[0],oraconn)+'\n</td><td>';
        sqlstmt='select to_char(datadate,\'mm/dd/yyyy\') datadate,round(used/1024,2) used_in_gb from legacy.diskusagetrend where inservserial='+str(row[0])+' order by datadate';
        usagecr=oracon.execSql(oraconn,sqlstmt);
        datrec=[];
        for usrec in usagecr:
            datrec.append(str(usrec[0])+':'+str(usrec[1]));
        datStr=string.join(datrec,',');
        if len(datrec) >0:
            xmdat=html.generateHtmlGridLine(datStr,'Trend of disk usage in GB','Used Space','Date',5000);
            ht+='<div id="linediv" align="center">Disk Usage Trend</div>\n';
            ht+='</td>\n</tr>\n</table>\n';
            ht+='<script type="text/javascript">\n';
            ht+='\tvar chart = new FusionCharts("../FusionChartsFree/Charts/FCF_Line.swf", "ChartId", "1000", "500");\n';
            ht+='\tchart.setDataXML("'+xmdat+'");\n';		   
            ht+='\tchart.render("linediv");\n';
            ht+='\t</script>\n';
        ht+='</div>\n';
        
        ht+='<div class="content" id="licenses" style="display:none;">\n'
        ht+='\t'+report.build_soft(row[0],oraconn)+'\n';
        ht+='</div>\n';
        
        ht+='<div class="content" id="vv" style="display:none;">\n'
        ht+='\t'+report.build_vv(row[0],oraconn)+'\n';
        ht+='</div>\n';
        
        ht+='<div class="content" id="pd" style="display:none;">\n'
        ht+='\t'+report.build_pd(row[0],oraconn)+'\n';
        ht+='</div>\n';
        
        ht+='<div class="content" id="alerts" style="display:none;">\n'
        ht+='\t'+report.getMajorAlert(row[0],oraconn)+'\n';
        ht+='</div>\n';
        
        ht+='<div class="content" id="rc" style="display:none;">\n'
        ht+='</div>\n';
        
        ht+='\t<script type="text/javascript">\n';
        ht+='\t\tfunction tab(tab) {\n';
        ht+='\t\t\tdocument.getElementById(tab).style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'summary\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'licenses\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'vv\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'pd\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'alerts\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'rc\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'li_summary\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(\'li_licenses\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(\'li_vv\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(\'li_pd\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(\'li_alerts\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(\'li_rc\').setAttribute(\'class\', \'\');\n';
        ht+='\t\t\tdocument.getElementById(tab).style.display = \'block\';\n';
        ht+='\t\t\tdocument.getElementById(\'li_\'+tab).setAttribute(\'class\', \'active\');\n';
        ht+='\t\t}\n';
        ht+='\t</script>';
        ht+='</body>\n</html>';
        htfl.write(ht);
        htfl.close;
        shutil.move(hfl,'/var/www/html/demo/'+hfl);
        print 'Done.....:'+str(row[0])
except:
    print "Inserv Error :"+str(row[0])
    print "Unexpected error:", sys.exc_info()[0];
    raise;
datacur.close();    

