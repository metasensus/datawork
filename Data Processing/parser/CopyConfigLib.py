#!/usr/bin/env python
import mysqlconnect as mconnect;
import oracleconnect as oracon;
import sys;

# Create tables that do not exist
def checkCreatetable(cur,tablenameList,oraconn,oracon):
    tableList=['processfunction','processlogic','stat_split_file_lookup','stats_split_files','statsprocesstransact','statsstructure','statsoutput'];
    tabset=set(tableList);
    tabnmset=set(tablenameList);
    diff = tabset - tabnmset;
    tabstr='';
    for difftab in diff:
        tabstr=retCreateStatement(difftab);
        cur.execute(tabstr);
    InsertMetaData(oraconn,oracon,cur);
    return;

# Create table script library    
def retCreateStatement(tabname):
    table=[];
    table['processfunction']= 'CREATE TABLE PROCESSFUNCTION(FUNCTIONID int,FUNCTIONNAME VARCHAR(50),FUNCTIONPARAMETERS VARCHAR(100),FUNCTIONDESC VARCHAR(1000));';
    table['processlogic']='CREATE TABLE PROCESSLOGIC(STATS_STRUCTURE_ID  int,PROCESS_SEQUENCE int,FUNCTIONID int,PARAMETER_VALUES varchar(200))';
    table['stat_split_file_lookup']='CREATE TABLE STAT_SPLIT_FILE_LOOKUP(DATATYPE VARCHAR(50),SPLIT_FILE_TYPE VARCHAR(50),SPLIT_FILE_SEARCH_TAG VARCHAR(100),SPLIT_FILE_SKIP_LINES int,SPLIT_FILE_END_TAG VARCHAR(50),SPLIT_FILE_LINE_SEPERATOR VARCHAR(50),STATSID int)';
    table['stats_split_files']='CREATE TABLE STATS_SPLIT_FILES(STATS_FILEID int,STATS_SPLITFILE_ID int,STATS_SPLITFILE_NAME VARCHAR(100),STATS_SPLITFILE_PATH VARCHAR(1000),';
    table['stats_split_files']+='STATSID int,MACHINEID int,FILE_CREATE_DATE DATE, FILE_PROCESS_DATE DATE,FILE_PROCESS_STATUS int,STATS_SPLIT_FILE_TYPE VARCHAR(50))';
    table['statsprocesstransact']='CREATE TABLE STATSPROCESSTRANSACT(STATS_FILEID int,MACHINEID int,DATATYPE VARCHAR(30),FILE_ENTRY_DATE DATE,FILE_PROCESS_DATE DATE,';
    table['statsprocesstransact']+='FILE_PROCESS_STATUS int,STATS_FILE_PATH VARCHAR(4000),STATS_FILE_NAME VARCHAR(100))';
    table['statsstructure']='CREATE TABLE STATSSTRUCTURE(STATSID int,STATS_STRUCTURE_ID int,STATSNAME VARCHAR(40),STATS_VERSION VARCHAR(50),STATS_FIRST_ROW_VERSION VARCHAR(10),';
    table['statsstructure']+='STATS_SINGLE_ROW VARCHAR(10),STATS_END_OF_ROW VARCHAR(100))';
    table['statsoutput']='CREATE TABLE STATSOUTPUT(STATS_SPLITFILE_ID int,STATS_OUTPUTFILE_NAME VARCHAR(1000),STATS_OUTPUTFILE_PATH VARCHAR(1000),STATS_FILE_CREATE_DATE DATE,STATS_FILE_LOAD_DATE DATE,STATS_FILE_LOAD_STATUS int,STATS_STRUCTURE_TYPE_ID int)';
    return table[tabname];

# Clean and add new metadata
def InsertMetaData(oraconn,oracon,cur):
    
    dataList=oracon.execSql(oraconn,'select FUNCTIONID,FUNCTIONNAME,FUNCTIONPARAMETERS,FUNCTIONDESC from PROCESSFUNCTION');
    cur.execute('truncate table PROCESSFUNCTION');
    for dat in dataList:
        metasql='Insert into PROCESSFUNCTION(FUNCTIONID,FUNCTIONNAME,FUNCTIONPARAMETERS,FUNCTIONDESC) values ('+str(dat[0])+',\''+str(dat[1])+'\',\''+str(dat[2])+'\',\''+str(dat[3])+'\')';
        cur.execute(metasql);
        cur.execute('commit;');
    
    
    dataList=oracon.execSql(oraconn,'select STATS_STRUCTURE_ID ,PROCESS_SEQUENCE ,FUNCTIONID from PROCESSLOGIC');
    cur.execute('truncate table PROCESSLOGIC');
    for dat in dataList:
        metasql='Insert into PROCESSLOGIC(STATS_STRUCTURE_ID ,PROCESS_SEQUENCE ,FUNCTIONID ) values ('+str(dat[0])+','+str(dat[1])+','+str(dat[2])+')';
        cur.execute(metasql);
        cur.execute('commit;');
    
    
    dataList=oracon.execSql(oraconn,'select DATATYPE,SPLIT_FILE_TYPE,SPLIT_FILE_SEARCH_TAG,SPLIT_FILE_SKIP_LINES,SPLIT_FILE_END_TAG,SPLIT_FILE_LINE_SEPERATOR,STATSID from STAT_SPLIT_FILE_LOOKUP');
    cur.execute('truncate table STAT_SPLIT_FILE_LOOKUP');
    for dat in dataList:
        metasql='Insert into STAT_SPLIT_FILE_LOOKUP(DATATYPE,SPLIT_FILE_TYPE,SPLIT_FILE_SEARCH_TAG,SPLIT_FILE_SKIP_LINES,SPLIT_FILE_END_TAG,SPLIT_FILE_LINE_SEPERATOR,STATSID) ';
        metasql+='values (\''+str(dat[0])+'\',\''+str(dat[1])+'\',\''+str(dat[2])+'\',\''+str(dat[3])+'\',\''+str(dat[4])+'\',\''+str(dat[5])+'\','+str(dat[6])+')';
        cur.execute(metasql);
        cur.execute('commit;');
    
    
    dataList=oracon.execSql(oraconn,'select STATSID,STATS_STRUCTURE_ID,STATSNAME,STATS_VERSION,STATS_FIRST_ROW_VERSION,STATS_SINGLE_ROW,STATS_END_OF_ROW from STATSSTRUCTURE');
    cur.execute('truncate table STATSSTRUCTURE');
    for dat in dataList:
        metasql='Insert into STATSSTRUCTURE(STATSID,STATS_STRUCTURE_ID,STATSNAME,STATS_VERSION,STATS_FIRST_ROW_VERSION,STATS_SINGLE_ROW,STATS_END_OF_ROW) ';
        metasql+='values ('+str(dat[0])+','+str(dat[1])+',\''+str(dat[2])+'\',\''+str(dat[3])+'\',\''+str(dat[4])+'\',\''+str(dat[5])+'\',\''+str(dat[6])+'\')';
        cur.execute(metasql);
        cur.execute('commit;');
    
    return;
    
  
        
# Connect to the database 
def connectDb():
    try:
        print '1. Connecting to mysql ....';    
        db=mconnect.connectMysql('','root','','localhost');
        print '2. Creating db...';
        mconnect.checkDb('ods',db);
        print '3. Creating procuser...';
        mconnect.checkUser('procuser',db);
        print '4. Connecting to Mysql as procuser'
        db = mconnect.connectMysql('ods','procuser','c@llhome','localhost');

        cur = db.cursor(); 
        print '5. Show tables'
        cur.execute('SHOW TABLES');
        datList=cur.fetchall();
    
        tablenameList={};
        if not datList:
            for dat in datList:
                tablenameList.append(dat[0]);
        print '6. Connecting to oracle.......'
        constr='ods/ods@callhomeods:1521/callhomeods';
        oraconn=oracon.openconnect(constr);
        print '7. Creating table........'
        checkCreatetable(cur,tablenameList,oraconn,oracon);
        db.close();
        oraconn.close();
        return;
    except:
        print "Error reported: "+str(sys.exc_info()[1]);
   



