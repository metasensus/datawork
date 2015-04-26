#!/usr/bin/env python
import string;
import oracleconnect as oracon;

constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
	
sqlstmt='select inservserial,nvl(address1,\'Unknown\') from datastore.all_inserv_master order by inservserial'; 
        
oraconn=oracon.openconnect(constr);
resultrec=oracon.execSql(oraconn,sqlstmt);
fl=open('/report/address.txt','w');

datstr='{\n';

datarray=[];
for rec in resultrec:
    print rec[0];    
    datarray.append('[\"'+str(rec[0])+'\",\"'+string.strip(rec[1])+'\"]');

datstr+=string.join(datarray,',\n')+'\n}';
                
datstr+='\n}';
fl.write(datstr);
fl.close();
oraconn.close();
