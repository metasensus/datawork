#!/usr/bin/env python
import string;
import oracleconnect as oracon;

def inform_penetration():
    
    informfl=open('/report/ospene','w');
    
    constr='produser/pr0duser@callhomedw.3pardata.com:1521/callhomedw';
    
    sqlstmt='SELECT DISTINCT INFORM_VERSION FROM VW_INFORM_TREND ORDER BY INFORM_VERSION';
    oraconn=oracon.openconnect(constr);   
    resultrec=oracon.execSql(oraconn,sqlstmt);
 
    inforArr=[];
    for rec in resultrec:
        sqlstmt='SELECT  QTR, YRQTR, NUM_SYSTEMS,PERCENT_OF_TOTAL, PERCENT_QROWTH_QBYQ, PERCENTGROWHTYY, UPGRADE_PERS, INSTALLS_PERS,UPGRADES,INSTALLS FROM VW_INFORM_TREND where INFORM_VERSION=\''+rec[0]+'\' order by YRQTR';
        infrec=oracon.execSql(oraconn,sqlstmt);
        inform=rec[0];
        qtrArr=[];
        sysArr=[];
        persArr=[];
        insArr=[];
        qygwtArr=[];
        yrgwtArr=[];
        persupgArr=[];
        persinsArr=[];
        upgArr=[];
        insArr=[];
        
        for inf in infrec:
            qtr='"'+str(inf[0])+'"';
            numsys='"'+str(inf[2])+'"';
            persoftot='"'+str(inf[3])+'"';
            installpers='"'+str(inf[7])+'"';
            persgrowthqq='"'+str(inf[4])+'"';
            persgrowthyy='"'+str(inf[5])+'"';
            upgpers='"'+str(inf[6])+'"';
            upg='"'+str(inf[8])+'"';
            installs='"'+str(inf[9])+'"';
            
            qtrArr.append(qtr);
            sysArr.append(numsys);
            persArr.append(persoftot);
            qygwtArr.append(persgrowthqq);
            yrgwtArr.append(persgrowthyy);
            persupgArr.append(upgpers);
            persinsArr.append(installpers);
            upgArr.append(upg);
            insArr.append(installs);
        infrec.close();        
        inforStr='{\"version\":\"'+inform+'\",\"quarters\":['+string.join(qtrArr,',')+'],"nSystems":['+string.join(sysArr,',')+'],"pctInstall":['+string.join(persArr,',')+']';
        inforStr+=',"pctGrowthQoQ":['+string.join(qygwtArr,',')+'],"pctGrowthYoY":['+string.join(yrgwtArr,',')+'],"pctUpgrade":['+string.join(persupgArr,',')
        inforStr+='],"pctNewInstall":['+string.join(persinsArr,',')+'],"numUpgrade":['+string.join(upgArr,',')+'],"numNewInstall":['+string.join(insArr,',')+']}\n'
        inforArr.append(inforStr);
    resultrec.close(); 
    informfl.write('['+string.join(inforArr,',')+']')
    informfl.close();    

def main():
    	#while (1):
	inform_penetration();

if __name__ == '__main__':
	main();    
        
    
    
    