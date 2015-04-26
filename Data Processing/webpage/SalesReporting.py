#!/usr/bin/env python273

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

sqlstmt='Select distinct INSERVSERIAL,HP_PROD_ID, HW_CONTRACT, SW_CONTRACT, trunc(INSTALL_DATE), CUSTOMER_NAME, ';
sqlstmt+='MODEL, OS_REL, NODE_COUNT, SITE_NAME, case when STATUS=\'A\' then \'Active\' else \'\' end sys_status,';
sqlstmt+='COUNTRY_CODE, LOCATION, POSTAL_CODE, ADDRESS1, CITY, STATE,';
sqlstmt+=' INSERVNAME, MASTER_NODE from legacy.all_inserv_master order by 1';

datacur=oracon.execSql(oraconn, sqlstmt);
try:
   
    for row in datacur:
        print 'Processing.....:'+str(row[0])
        hfl=str(row[0])+'.html'
        htfl=open(hfl,'w');
        newrow=[];
        ht=html.generateHtmlwithTab('3Par System Support Dashboard','3Par System Support Dashboard for  '+str(row[0]),'#9D8851');
        ht+='<br/>\n';
        ht+='<table>\n<tr><td>\n<table>\n<tr><td>';
        ht+='<img src="3par_arrays.jpg" alt="Inserv" height="400" width="350" />\n';
        ht+='</td></tr>\n</table>\n</td><td>\n<table>\n<tr><td>\n'
        
        newrow.append('Serial Number: '+str(row[0]));
        newrow.append('Installation Date: '+str(row[4]));
        newrow.append('Customer Name: '+str(row[5]));
        newrow.append('HP Serial Number: '+str(row[1]));
       # newrow.append('Hardware Contract: '+string.join(str(row[2]).split(';'),'<br/>'));
       # newrow.append('Software Contract: '+string.join(str(row[3]).split(';'),'<br/>'));
        newrow.append('Model: '+str(row[6]));
        newrow.append('OS: '+string.join(str(row[7]).split(';'),' '));
        newrow.append('Number of Nodes: '+str(row[8]));
        newrow.append('Site Name: '+str(row[9]));
        newrow.append('System Status: '+str(row[10]));
        newrow.append('Country Code: '+str(row[11]));
        newrow.append('Location: '+str(row[12]));
        newrow.append('Postal Code: '+str(row[13]));
        newrow.append('City: '+str(row[15]));
        newrow.append('State: '+str(row[16]));
        newrow.append('Inserv Name: '+str(row[17]));
        
        dataString=string.join(newrow,'|');
        ht+=html.generateHtmlTableHeader('',0);
        ht+=html.generateHtmlDataTable(dataString,0);
        ht+='</td></tr>\n</table>\n</td><td>\n<table>\n<tr><td>\n<table><td>';
        ht+=report.getInservSpaceUtil(oraconn,row[0]);
        ht+='</td><td>';
        ht+=report.build_disktype_free_trend(row[0],oraconn);
        ht+='</td></table></td></tr>\n<tr><td>';
        ht+=report.getutilByDiskType(oraconn,row[0]);
        ht+='</td></tr></table>\n<td>';
        ht+=report.build_soft(row[0],oraconn);
        ht+='</td>\n</table>\n';
        
        ht+='<hr color=red>';
        tabList='Layout;Alerts;Drives;Performance';
        ht+=html.buildtab(tabList);
        #addtabContent(id,tabval);
        ht+=html.addtabscript(tabList);
        ht+='\n</body>\n</html>';
        
        
        
        
        htfl.write(ht);
        htfl.close;
        shutil.move(hfl,'/var/www/html/sales/'+hfl);
        print 'Done.....:'+str(row[0])
except:
    print "Inserv Error :"+str(row[0])
    print "Unexpected error:", sys.exc_info()[0]
    raise;
datacur.close();    

