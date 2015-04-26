#!/usr/bin/env python

import string;

#Library for processing files


# Split the lines based on a delimiter
# delimiter can be any string
def LineSplitter (linearray,delimiter):
    retList=[];
    for ln in linearray:
        retList.append(string.split(ln,delimiter));
    return retList;

# Remove characters that may not be needed
# ideal for removing ' ' from array

def cleanData (linearray,remStr):
    retList=[];
    ret=[];
    for ln in linearray:
        for lnstr in ln:
            if ln != remStr:
                ret.append(ln);
        retList.append(ret);
    return retList;

# Copy columns if the columns does not exist use in conjunction with LineSplitter,cleanData
# Array of lines LineArrayList
# Copy Column Location as a string seperated by , copyColumnLoc
# Number Of columns expected actualNumColumns
# Column position gives the position of the columns without copy columns currentColumnPos
def copyColumn (lineArrayList,copyColumnLoc,actualNumColumns,currentColumnPos):
    retList=[];
    copyList=[];
    for ln in linearrayList:
        if len(ln) == actualNumColumns:
            colLocList=string.split(copyColumnLoc,',');
            i=0;
            while i < len(ln):
                for col in colLocList:
                    if i==col:
                        copyList[i]=ln[i];
                i+=1;
            retList.append(ln);
        else:
            currentColumnPosList=string.split(currentColumnPos,',');
            lnnew=[];
            i=0;
            while i < len(copyList):
                lnnew[i]=copyList[i];
                i+=1;
            i=0;
            for l in ln:
                lnnew[currentColumnPosList[i]]=l;
                i+=1;
            retList.append(lnnew);
    return retList;

# Remove a complete string from a string array
# Array of lines lineArrayList
# String to be removed remStr
def strRemove(lineArrayList,remStr):
    retList=[];
    skipstr=[];
    for ln in lineArrayList:
        if string.strip(ln) == string.strip(remStr):
            skipstr.append(ln);
        else:
            retList.append(ln);
    return retList;

# Returns a delimited string  
def strAddDelimiter(lineArrayList,delimiter):
    strArray=[];
    for ln in lineArrayList:
        strArray.append(string.join(ln,delimiter));
    return string.join(strArray,'\n');

def strReplaceDelim(lineArray,currentDelim,delimiter):
    strArray=[];
    for ln in lineArray:
        strArray.append(string.replace(ln,currentDelim,delimiter));
    return strArray;

def stringDelimitremove(linearray):
    delimiter='\t';
    newstr=[];
    for dt in linearray:
        strList=string.split(dt,delimiter);
        newdt=[];
        for st in strList:
            if st:
                newdt.append(st);
        newt=string.join(newdt,delimiter);
        newstr.append(newt);
    return newstr;

def stringJoin(linearray):
    return string.join(linearray);

def LineSplitterJoiner (linearray):
    delimiter,newdelim=' ','\t';
    retList=[];
    for ln in linearray:
        retList.append(string.join(string.split(ln,delimiter),newdelim));
    return retList;

def getVersion(filename,filepath):
    openFl=open(filepath+filename);
    readLines=openFl.readlines();
    openFl.close();
    lnCtr=0;
    ver='';
    for readln in readLines:
        if lnCtr==0:
            if len(string.strip(readln))>0 :
                ver=readln[0]+readln[1]+readln[2]+readln[3]+readln[4];
        lnCtr+=1;    
    return ver;     

def readFile(filename,filepath):
    openFl=open(filepath+filename);
    readLines=openFl.readlines();
    openFl.close();
    newData=[];
    lnCtr=0;
    for readln in readLines:
        if lnCtr>0:
            newData.append(readln);
        lnCtr+=1;
    return newData;

def LineColonSplitterJoiner (linearray):
    delimiter,newdelim=':','\t';
    retList=[];
    for ln in linearray:
        retList.append(string.join(string.split(ln,delimiter),newdelim));
    return retList;

def LineRemove(linearray):
    searchstr='License';
    newArr=[];
    for ln in linearray:
        if string.find(ln,searchstr)< 0:
            newArr.append(string.strip(ln));
    return newArr;

def LineSplit(linearray):
    splitStr='Expired on';
    newArr=[];
    for ln in linearray:
        newArr.append(string.replace(ln,splitStr,'\t'));
    return newArr;

def LineJoin(linearray):
    return string.join(linearray,'\n');

def RemSpace(lineArray):
    retline=[];
    for verlist in lineArray:
        ctr=1;
        newstr='';
        changeddelim=0;
        while ctr <= len(verlist):
            if ctr>2:
                if len(string.strip(verlist[ctr-1]))==0 and len(string.strip(verlist[ctr-2]))==0:
                    if changeddelim == 0:
                        newstr+=':';
                        changeddelim=1;
                else:
                    newstr+=verlist[ctr-1];
            else:
                newstr+=verlist[ctr-1];
            ctr+=1;        
        newstr=string.replace(newstr,':','\t');
        retline.append(newstr);
    return retline;

def convertRowtoCol(lineArray):
    startStr='Id';
    endStr='Message';
    splitStr=':';
    datStr='';
    recStr=[];
    datArr=[];
    prev_col='';
    for ln in lineArray:
        if len(ln)>1:
            dat=string.split(ln,splitStr);
            if len(dat)==1:
                break;
            if string.strip(dat[0])==startStr:
                recStr=[];
            if string.strip(prev_col)=='Message Code':
                if string.strip(dat[0])=='Time':
                    recStr.append('None')
            if len(dat) > 2:
                i=0;
                combrec=[];
                for drec in dat:
                    if i>0:
                        combrec.append(drec);
                    i+=1;
                dtrec=string.join(combrec,':');
                dtrec=string.replace(dtrec,'\n','');
                recStr.append(dtrec);
            else:
                dtrec=string.replace(dat[1],'\n','');
                recStr.append(dtrec);
            if string.strip(dat[0])==endStr:
                datArr.append(string.join(recStr,'\t'));    
            prev_col=dat[0];
    datStr=string.join(datArr,'\n');
    
    return datStr;
 
def removenewLine(linearray):
    newArray=[];
    for ln in linearray:
        newArray.append(string.replace(ln,'\n',''));
    return newArray;

def removeTab(linearray):
    newArray=[];
    for ln in linearray:
        newArray.append(string.replace(string.strip(ln),'\t',''));
    return newArray;
    
def replaceSpaceWithTab(linearray):
    newArray=[];
    for ln in linearray:
        newArray.append(string.join(string.split(string.strip(ln),' '),'\t'));
    return newArray;

def skipFirstLine(linearray):
    newArray=[];
    ctr=0;
    skipLine=0;
    for ln in linearray:
        if string.find(ln,'Node')>=0:
            skipLine=1;
        if skipLine== 1:
            if ctr >0:
                newArray.append(ln);
        else:
            newArray.append(ln);
            
        ctr+=1;
    return newArray;

def mergeColumnHwmem(linearray):
    newArray=[];
    normsize=12;
    
    for ln in linearray:
        currln=string.split(ln,' ');
        dt='';
        
        if len(currln) == normsize:
            stcount=0;
            newln=[];
            dt='';
            skip=0;
            for cr in currln:
                if stcount ==6 or stcount == 7:
                    if stcount == 6: 
                        dt=cr;
                        skip=1;
                    if stcount==7:
                        dt+=cr;
                        skip=0;
                else:
                    dt=cr;
                if skip==0:
                    newln.append(dt);
                stcount+=1;
            newArray.append(string.join(newln,'\t'));
        else:
            newln=string.replace(ln,' ','\t');
            newArray.append(newln);
    return newArray;

def addTabfordoubleSpace(linearray):
    newArray=[]
    for ln in linearray:
        newArray.append(string.replace(ln,'  ','\t'));
    return newArray;
    
def removeMultiTab(linearray):
    newArray=[]
    for ln in linearray:
        lnsplit=string.split(ln,'\t');
        arr=[];
        for ls in lnsplit:
            if len(string.strip(ls))>0:
                arr.append(string.strip(ls));
        newArray.append(string.join(arr,'\t'));    
    return newArray;

def removeColDelimiter(linearray):
    newArray=[]
    for ln in linearray:
        newline=string.replace(ln,'\t',' ')
        newline=newline.replace(' ','\t',6)
        newArray.append(newline)
    return newArray;