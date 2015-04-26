#!/usr/bin/env python

import oracleconnect as oracon;
import os;
import time;
import string;

def checkexist(dat,srcdat):
    for dt in srcdat:
        if dt==dat:
            return 0;
    return 1;

def dedup(data):
    newdata=[];
    srcdata=data;
    for dt in srcdata:
        exist=checkexist(dt,newdata);
        if exist==1:
            newdata.append(dt);
    return newdata;    

def sales_detail():
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    sqlstmt='select UNBALANCED, nvl(UNBALANCE_TYPE,\'N/A\'), DIRECTOR, MANAGER, SALES_REP, COMPANY, INSERVSERIAL, nvl(HP_SERIAL_NUMBER,\'UNKNOWN\'),';
    sqlstmt+='MODEL, OSVERSION, TOTAL_SPACE, TOTAL_FREE_SPACE, FREE_PERCENT, nvl(CAMPAIGN,\'None\'), nvl(REVENUE,0), REGION, CASE WHEN TRIM(COUNTRY)=\'.\' THEN \'UNKNOWN\' ELSE COUNTRY END COUNTRY'; 
    sqlstmt+=', CASE WHEN TRIM(STATE)=\'.\' THEN \'UNKNOWN\' ELSE STATE END STATE, CASE WHEN TRIM(CITY)=\'.\' THEN \'UNKNOWN\' ELSE CITY END CITY ';
    sqlstmt+='from CAPACITY_FOR_SALES_CAMPAIGN where total_space>0 and inservserial >0 and inservserial < 9900000 order by inservserial';
    oraconn=oracon.openconnect(constr);
    resultrec=oracon.execSql(oraconn,sqlstmt);
    
    datarray=[];
    datstr='{"aaData": [\n';    
    for rec in resultrec:
        unbalanced=rec[0];
        unbalance_type=rec[1];
        director=rec[2];
        manager=rec[3];
        sales_rep=rec[4];
        company=rec[5];
        inservserial=rec[6];
        hpserial=rec[7];
        model=rec[8];
        osver=rec[9];
        totspace=rec[10];
        totfreespace=rec[11];
        freepers=rec[12];
        campaign=rec[13];
        rev=rec[14];
        region=rec[15];
        ctry=rec[16];
        state=rec[17];
        city=string.replace(string.replace(string.replace(string.replace(string.replace(string.replace(rec[18],'/',''),'&',''),'?',''),'\'',''),'#',' '),'.',' ');
        
        datarray.append('["","'+unbalanced+'","'+unbalance_type+'","'+director+'","'+manager+'","'+sales_rep+'","'+company+'","'+str(inservserial)+'","'+hpserial+'","'+model+'","'+osver+'",'+str(totspace)+','+str(totfreespace)+','+str(freepers)+',"'+campaign+'",'+str(rev)+',"'+region+'","'+ctry+'","'+state+'","'+city+'"]\n');
    datstr+=string.join(datarray,',')+'\n]\n}'
    outfl=open('/report/salesCampaignDetails.txt','w');
    outfl.write(datstr);
    outfl.close()
    resultrec.close();
    oraconn.close();


def sales_summary():
    
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    sqlstmt='select \'Director\' role,name,\'Refresh\' upgrade_type,refresh_rev,num_systems,region,country,email from ';   
    sqlstmt+='(select director name,sum(refresh_revenue) refresh_rev,count(distinct inservserial) num_systems,region ,country,director_email email  from (select distinct director,refresh_revenue,inservserial,region ,country,director_email from CAPACITY_WEB_REP_WITH_REF_UPD) where refresh_revenue>0 group by director,region,country,director_email)';
    sqlstmt+=' union select \'Director\' role,name,\'Upgrade\' upgrade_type,refresh_rev,num_systems ,region,country ,email from  '; 
    sqlstmt+=' (select director name,sum(upgrade_pricing) refresh_rev,count(distinct inservserial) num_systems,region,country,director_email email from (select distinct director,upgrade_pricing,inservserial,region ,country,director_email  from  CAPACITY_WEB_REP_WITH_REF_UPD) where upgrade_pricing>0 group by director,region,country,director_email)';
    sqlstmt+=' union select \'Manager\' role,name,\'Refresh\' upgrade_type,refresh_rev,num_systems,region,country,email from ';   
    sqlstmt+='(select manager name,sum(refresh_revenue) refresh_rev,count(distinct inservserial) num_systems,region,country,manager_email email from (select distinct manager,refresh_revenue,inservserial,region,country,manager_email from  CAPACITY_WEB_REP_WITH_REF_UPD) where refresh_revenue>0 group by manager,region,country,manager_email)';
    sqlstmt+=' union select \'Manager\' role,name,\'Upgrade\' upgrade_type,refresh_rev,num_systems,region,country,email from  '; 
    sqlstmt+=' (select manager name,sum(upgrade_pricing) refresh_rev,count(distinct inservserial) num_systems,region,country,manager_email email from(select distinct manager,upgrade_pricing,inservserial,region,country,manager_email  from CAPACITY_WEB_REP_WITH_REF_UPD)  where upgrade_pricing>0 group by manager,region,country,manager_email )';
    sqlstmt+=' union select \'Sales Rep\' role,name,\'Refresh\' upgrade_type,refresh_rev,num_systems,region,country,email from ';   
    sqlstmt+='(select sales_rep name,sum(refresh_revenue) refresh_rev,count(distinct inservserial) num_systems,region,country ,sales_rep_email email  from CAPACITY_WEB_REP_WITH_REF_UPD where refresh_revenue>0 group by sales_rep,region,country,sales_rep_email)';
    sqlstmt+=' union select \'Sales Rep\' role,name,\'Upgrade\' upgrade_type,refresh_rev,num_systems,region,country,email from  '; 
    sqlstmt+=' (select sales_rep name,sum(upgrade_pricing) refresh_rev,count(distinct inservserial) num_systems,region,country,sales_rep_email email from CAPACITY_WEB_REP_WITH_REF_UPD where upgrade_pricing>0 group by sales_rep,region,country,sales_rep_email )';
    summarray=[];
    oraconn=oracon.openconnect(constr);
    resultrec=oracon.execSql(oraconn,sqlstmt);
    for rec in resultrec:
        role=rec[0];
        name=rec[1];
        upgtyp=rec[2];
        rev=rec[3];
        num_systems=rec[4];
        region=rec[5];
        country=rec[6];
        email=rec[7];
        
        summarray.append('["'+role+'","'+name+'","'+upgtyp+'",'+str(num_systems)+','+str(rev)+',"'+region+'","'+country+'","'+email+'"]');
    outfl=open('/report/salesCampaignSummary.txt','w');
    outfl.write('{"aaData": ['+string.join(summarray,',')+']}');
    outfl.close()
    resultrec.close();
    oraconn.close();
    

def sales_hier():
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    sqlstmt='select distinct upper(region) from CAPACITY_WEB_REP_WITH_REF_UPD';
    oraconn=oracon.openconnect(constr);
    resultrec=oracon.execSql(oraconn,sqlstmt);
    datarry=[];
    datstr='';
    regarr=[];
    regctry=[];
    ctrystate=[];
    statecity=[];
    directorcountry=[];
    directordetail=[];
    directormanager=[];
    managerdetail=[];
    managersales=[];
    salesdetail=[];
    countrymodel=[];
    users=[];
    for reg in resultrec:
        regarr.append('"'+reg[0]+'"');
        sqlstmt='select distinct trim(upper(country)) from CAPACITY_WEB_REP_WITH_REF_UPD where upper(region)=\''+reg[0]+'\'';
        ctrec=oracon.execSql(oraconn,sqlstmt);
        ctry=[];
        for ct in ctrec:
            ctry.append('"'+ct[0]+'"');
            
            sqlstmt='select distinct director_email,director from CAPACITY_WEB_REP_WITH_REF_UPD where upper(region)=\''+reg[0]+'\'';
            drec=oracon.execSql(oraconn,sqlstmt);
            director=[]
            
            for dr in drec:
                director.append('"'+dr[0]+'"');
                directordetail.append('"'+dr[0]+'":{"name":"'+dr[1]+'","role":"directors"}');
                sqlstmt='select distinct manager_email,manager from CAPACITY_WEB_REP_WITH_REF_UPD where director_email=\''+dr[0]+'\'';
                mgrrec=oracon.execSql(oraconn,sqlstmt);
                manager=[];
                for mrec in mgrrec:
                    manager.append('"'+mrec[0]+'"');
                    managerdetail.append('"'+mrec[0]+'":{"name":"'+mrec[1]+'","role":"managers"}');
                    sqlstmt='select distinct sales_rep_email,sales_rep from CAPACITY_WEB_REP_WITH_REF_UPD where manager_email=\''+mrec[0]+'\'';
                    slsrec=oracon.execSql(oraconn,sqlstmt);
                    sales=[];
                    for srec in slsrec:
                        sales.append('"'+srec[0]+'"');
                        salesdetail.append('"'+srec[0]+'":{"name":"'+srec[1]+'","role":"sales"}');
                    slsrec.close();    
                    managersales.append('"'+mrec[0]+'":['+string.join(sales,',')+']');
                directormanager.append('"'+dr[0]+'":['+string.join(manager,',')+']');
                mgrrec.close();    
            directorcountry.append('"'+reg[0]+'":['+string.join(director,',')+']');
            drec.close();
            
            sqlstmt='select distinct trim(model) from CAPACITY_WEB_REP_WITH_REF_UPD where trim(upper(country))=\''+ct[0]+'\'';
            modrec=oracon.execSql(oraconn,sqlstmt);
            model=[];
            for mdrec in modrec:
                mdstr='\"'+str(mdrec[0])+'\"';
                model.append(mdstr);
            countrymodel.append('\"'+ct[0]+'\":['+string.join(model,',')+']');
            
            sqlstmt='select distinct trim(upper(state)) from CAPACITY_WEB_REP_WITH_REF_UPD where trim(upper(country))=\''+ct[0]+'\'';
            state=[];
        
            strec=oracon.execSql(oraconn,sqlstmt);
            for st in strec:
                state.append('"'+st[0]+'"');
                sqlstmt='select distinct trim(upper(city)) from CAPACITY_WEB_REP_WITH_REF_UPD where trim(upper(replace(state,\'\'\'\',\'\')))=\''+string.replace(st[0],'\'','')+'\'';
                city=[];
                citrec=oracon.execSql(oraconn,sqlstmt);
                for cty in citrec:
                    city.append('"'+string.replace(string.replace(string.replace(string.replace(string.replace(string.replace(cty[0],'/',''),'&',''),'?',''),'\'',''),'#',' '),'.',' ')+'"');
                    
                statecity.append('"'+st[0]+'":['+string.join(city,',')+']\n');
            ctrystate.append('"'+ct[0]+'":['+string.join(state,',')+']\n');
        regctrstr='\"'+reg[0]+'\":['+string.join(ctry,',')+']\n';
        regctry.append(regctrstr);
        ctrec.close();
    resultrec.close();
    
    sqlstmt='select email,max(name) name from (select distinct director_email email,director name from CAPACITY_WEB_REP_WITH_REF_UPD ';
    sqlstmt+='union select distinct manager_email email,manager name from CAPACITY_WEB_REP_WITH_REF_UPD';
    sqlstmt+=' union select distinct sales_rep_email email,sales_rep name from CAPACITY_WEB_REP_WITH_REF_UPD) group by email';
    resultrec=oracon.execSql(oraconn,sqlstmt);
    
    for rec in resultrec:
        email=rec[0];
        name=rec[1];
        role=[];
        sqlstmt='select count(1) from CAPACITY_WEB_REP_WITH_REF_UPD where director_email=\''+email+'\'';
        dirrec=oracon.execSql(oraconn,sqlstmt);
        numrecs = 0;
        for di in dirrec:
            numrecs=di[0];
        if numrecs > 0:
            role.append('\"Director\"');
        dirrec.close();
        sqlstmt='select count(1) from CAPACITY_WEB_REP_WITH_REF_UPD where manager_email=\''+email+'\'';
        dirrec=oracon.execSql(oraconn,sqlstmt);
        numrecs = 0;
        for di in dirrec:
            numrecs=di[0];
        if numrecs > 0:
            role.append('\"Manager\"');    
        dirrec.close();
        sqlstmt='select count(1) from CAPACITY_WEB_REP_WITH_REF_UPD where sales_rep_email=\''+email+'\'';
        dirrec=oracon.execSql(oraconn,sqlstmt);
        numrecs = 0;
        for di in dirrec:
            numrecs=di[0];
        if numrecs > 0:
            role.append('\"Sales Rep\"');    
        users.append('"'+email+'":{'+'"name":"'+name+'","role":['+string.join(role,',')+']}');
    
    
    ctrystate=dedup(ctrystate);
    statecity=dedup(statecity);
    directorcountry=dedup(directorcountry);
    directormanager=dedup(directormanager);
    managersales=dedup(managersales);
    directordetail=dedup(directordetail);
    managerdetail=dedup(managerdetail);
    salesdetail=dedup(salesdetail);
    
    datstr='\"region\":['+string.join(regarr,',')+']\n';
    countrystr='"countries" : {'+string.join(regctry,',')+'}\n';
    statestr='"states" : {'+string.join(ctrystate,',')+'}\n';
    citystr='"cities" : {'+string.join(statecity,',')+'}\n';
    modelstr='"models" : {'+string.join(countrymodel,',')+'}\n';
    dirstr='"directors" : {'+string.join(directorcountry,',')+'}\n';
    mgrstr='"managers" : {'+string.join(directormanager,',')+'}\n';
    salestr='"salesReps" : {'+string.join(managersales,',')+'}\n';
    userstr='"users" : {'+string.join(users,',')+'}';
    
    alldata='{'+datstr+','+countrystr+','+statestr+','+citystr+','+modelstr+','+dirstr+','+mgrstr+','+salestr+','+userstr+'}';
    outfl=open('/report/salesCampaignFilters.txt','w');
    outfl.write(alldata);
    outfl.close();
    oraconn.close()   
    
def main():
    	#while (1):
    sales_hier();
    sales_summary();
    sales_detail();
    
if __name__ == '__main__':
    main();
