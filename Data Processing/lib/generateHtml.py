#!/usr/bin/env python273
import string

def generateHtmlHeader(title,header):
    ht='<html>\n<head><title>'+title+'</title>';
    ht+='<link rel="stylesheet" type="text/css" href="inserv_reports.css">\n';
    ht+='<style type="text/css">\n';
    ht+='\ttable.gridtable {\n';
    ht+='\t\tfont-family: verdana,arial,sans-serif;\n';
    ht+='\t\theight:inherit;\n';
    ht+='\t\toverflow:scroll;\n';
    ht+='\t\tfont-size:11px;\n';
    ht+='\t\tcolor:#333333;\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tborder-collapse: collapse;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable th {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\ttpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: #dedede;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable td {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: #ffffff;\n';
    ht+='\t}\n';
    ht+='</style>\n';
    ht+='</head><body>\n';
    ht+='<h1><center>'+header+'</center></h1>';
    ht+='<img src="HP_New_Logo_2D.png" alt="HP" height="42" width="42" />';
    return ht;

def generateHtmlWithSPHead(title,header,subhead=''):
    ht='<html>\n<head><title>'+title+'</title>';
    ht+='<link rel="stylesheet" type="text/css" href="inserv_reports.css">\n';
    ht+='<style type="text/css">\n';
    ht+='\ttable.gridtable {\n';
    ht+='\t\tfont-family: verdana,arial,sans-serif;\n';
    ht+='\t\theight:inherit;\n';
    ht+='\t\toverflow:scroll;\n';
    ht+='\t\tfont-size:11px;\n';
    ht+='\t\tcolor:#333333;\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tborder-collapse: collapse;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable th {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\ttpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: Highlight;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable td {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: #ffffff;\n';
    ht+='\t}\n\n';
    ht+='\t#tablist\n';
    ht+='\t{\n';
    ht+='\t\tpadding: 3px 0;\n';
    ht+='\t\tmargin-left: 0;\n';
    ht+='\t\tmargin-bottom: 0;\n';
    ht+='\t\tmargin-top: 0.1em;\n';
    ht+='\t\tfont: bold 12px Verdana;\n';
    ht+='\t\tborder-bottom: 1px solid gray;\n';
    ht+='\t}\n\n';
    ht+='\t#tablist li\n';
    ht+='\t{\n';
    ht+='\t\tlist-style: none;\n';
    ht+='\t\tdisplay: inline;\n';
    ht+='\t\tmargin: 0;\n';
    ht+='\t}\n\n';
    ht+='\t#tablist li a\n';
    ht+='\t{\n';
    ht+='\t\ttext-decoration: none;\n';
    ht+='\t\tpadding: 3px 0.5em;\n';
    ht+='\t\tmargin-right: 3px;\n';
    ht+='\t\tborder: 1px solid #778;\n';
    ht+='\t\tborder-bottom: none;\n';
    ht+='\t\tbackground: white;\n';
    ht+='\t}\n\n';
    
    ht+='\t#tablist li a:link, #tablist li a:visited\n';
    ht+='\t{\n';
    ht+='\t\tcolor: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a:hover\n';
    ht+='\t{\n';
    ht+='\t\tcolor: black;\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t\tborder-color: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a.current\n';
    ht+='\t{\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t}\n\n';
        
    ht+='\tdiv.content\n'; 
    ht+='\t{\n';
    ht+='\t\tclear:both;\n';
    ht+='\t\tpadding:0 15px;\n';
    #ht+='\t\tline-height:1500px;\n';
    
    #ht+='\t\ttop: 20px;\n';
    #ht+='\t\tz-index: 5;\n';
    #ht+='\t\theight: 1000px;\n';
    ht+='\t\toverflow: hidden;\n';
    ht+='\t\tposition: relative;\n';
    ht+='\t\ttop: 20px;\n';
    ht+='\t\tz-index: 5;\n';
    ht+='\t}\n';
    ht+='\tlegend\n'; 
    ht+='\t{\n';
    ht+='\t\ttext-align:center;\n';
    ht+='\t\tcolor:Navy;\n';
    ht+='\t\tfont-size:medium;\n'
    ht+='\t\tfont-family:Times New Roman;\n';
    ht+='\t\ttext-shadow:grey;\n';
    ht+='\t\ttext-transform:capitalize;\n';
    ht+='\t}\n';
    ht+='\t#tablist li\n';
    ht+='\t{\n';
    ht+='\t\tlist-style: none;\n';
    ht+='\t\tdisplay: inline;\n';
    ht+='\t\tmargin: 0;\n';
    ht+='\t}\n\n';
    
    ht+='\t#tablist li a\n';
    ht+='\t{\n';
    ht+='\t\ttext-decoration: none;\n';
    ht+='\t\tpadding: 3px 0.5em;\n';
    ht+='\t\tmargin-right: 3px;\n';
    ht+='\t\tborder: 1px solid #778;\n';
    ht+='\t\tborder-bottom: none;\n';
    ht+='\t\tbackground: white;\n';
    ht+='\t}\n\n';
    
    ht+='\t#tablist li a:link, #tablist li a:visited\n';
    ht+='\t{\n';
    ht+='\t\tcolor: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a:hover\n';
    ht+='\t{\n';
    ht+='\t\tcolor: black;\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t\tborder-color: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a.current\n';
    ht+='\t{\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t}\n\n';
        
    ht+='\tdiv.content\n'; 
    ht+='\t{\n';
    ht+='\t\tclear:both;\n';
    ht+='\t\tpadding:0 15px;\n';
    #ht+='\t\tline-height:1500px;\n';
    
    #ht+='\t\ttop: 20px;\n';
    #ht+='\t\tz-index: 5;\n';
    #ht+='\t\theight: 1000px;\n';
    ht+='\t\toverflow: hidden;\n';
    ht+='\t\tposition: relative;\n';
    ht+='\t\ttop: 20px;\n';
    ht+='\t\tz-index: 5;\n';
    ht+='\t}\n';
    
    ht+='</style>\n';
    ht+='<script language="JavaScript" src="../FusionChartsFree/JSClass/FusionCharts.js"></script>\n';
    ht+='</head><body>\n';
    ht+='<img src="HP_New_Logo_2D.png" alt="HP" height="42" width="42" />';
    ht+='\t<div class=WordSection1>\n';
    ht+='\t\t<p class=MsoNormal align=center style=\'text-align:center\'><b><span style=\'font-size:24.0pt;line-height:115%\'>'+header+'</span></b></p>\n';
    ht+='\t\t<p class=MsoNormal align=center style=\'text-align:center\'><span style=\'font-size:24.0pt;line-height:115%\'>&nbsp;</span></p>\n';
    ht+='\t\t<p class=MsoNormal align=center style=\'text-align:right\'><b><span style=\'font-size:12.0pt;line-height:80%\'>'+subhead+' [ Contact: Deepak Nair (dnair@hp.com) / William (Bill) Tung (bill.tung@hp.com) ] </span></b></p>\n';
    #ht+='\t\t<p class=MsoNormal align=center style=\'text-align:right\'><b><span style=\'font-size:12.0pt;line-height:80%\'>Contact: Deepak Nair (dnair@hp.com)</span></b></p>\n';
    #ht+='\t\t<p class=MsoNormal align=center style=\'text-align:right\'><b><span style=\'font-size:12.0pt;line-height:80%\'> William (Bill) Tung (bill.tung@hp.com)</span></b></p>\n';
    ht+='\t</div>\n';
    #ht+='\t<fieldset></legend>'
    #ht+='<table>';
    #ht+='<tr></tr><tr></tr><tr></tr><tr></tr>'
    return ht;

def generateAddFieldset():
    ht='</fieldset>'
    return ht;
#9D8851

def generateHtmlwithTab(title,header,color='#FFFFFF'):
    ht='<html>\n<head><title>'+title+'</title>';
    ht+='<link rel="stylesheet" type="text/css" href="inserv_reports.css">\n';
    ht+='<style type="text/css">\n';
    ht+='\ttable.gridtable {\n';
    ht+='\t\tfont-family: verdana,arial,sans-serif;\n';
    ht+='\t\theight:inherit;\n';
    ht+='\t\toverflow:scroll;\n';
    ht+='\t\tfont-size:11px;\n';
    ht+='\t\tcolor:#333333;\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tborder-collapse: collapse;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable th {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\ttpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: #dedede;\n';
    ht+='\t}\n';
    ht+='\ttable.gridtable td {\n';
    ht+='\t\tborder-width: 1px;\n';
    ht+='\t\tpadding: 8px;\n';
    ht+='\t\tborder-style: solid;\n';
    ht+='\t\tborder-color: #666666;\n';
    ht+='\t\tbackground-color: #ffffff;\n';
    ht+='\t}\n\n';
    ht+='\t#tablist\n';
    ht+='\t{\n';
    ht+='\t\tpadding: 3px 0;\n';
    ht+='\t\tmargin-left: 0;\n';
    ht+='\t\tmargin-bottom: 0;\n';
    ht+='\t\tmargin-top: 0.1em;\n';
    ht+='\t\tfont: bold 12px Verdana;\n';
    ht+='\t\tborder-bottom: 1px solid gray;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li\n';
    ht+='\t{\n';
    ht+='\t\tlist-style: none;\n';
    ht+='\t\tdisplay: inline;\n';
    ht+='\t\tmargin: 0;\n';
    ht+='\t}\n\n';
    
    ht+='\t#tablist li a\n';
    ht+='\t{\n';
    ht+='\t\ttext-decoration: none;\n';
    ht+='\t\tpadding: 3px 0.5em;\n';
    ht+='\t\tmargin-right: 3px;\n';
    ht+='\t\tborder: 1px solid #778;\n';
    ht+='\t\tborder-bottom: none;\n';
    ht+='\t\tbackground: white;\n';
    ht+='\t}\n\n';
    
    ht+='\t#tablist li a:link, #tablist li a:visited\n';
    ht+='\t{\n';
    ht+='\t\tcolor: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a:hover\n';
    ht+='\t{\n';
    ht+='\t\tcolor: black;\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t\tborder-color: navy;\n';
    ht+='\t}\n\n';
        
    ht+='\t#tablist li a.current\n';
    ht+='\t{\n';
    ht+='\t\tbackground: lightyellow;\n';
    ht+='\t}\n\n';
        
    ht+='\tdiv.content\n'; 
    ht+='\t{\n';
    ht+='\t\tclear:both;\n';
    ht+='\t\tpadding:0 15px;\n';
    #ht+='\t\tline-height:1500px;\n';
    
    #ht+='\t\ttop: 20px;\n';
    #ht+='\t\tz-index: 5;\n';
    #ht+='\t\theight: 1000px;\n';
    ht+='\t\toverflow: hidden;\n';
    ht+='\t\tposition: relative;\n';
    ht+='\t\ttop: 20px;\n';
    ht+='\t\tz-index: 5;\n';
    ht+='\t}\n';
    ht+='</style>\n';
    ht+='<script language="JavaScript" src="../FusionChartsFree/JSClass/FusionCharts.js"></script>';
    ht+='</head><body bgcolor="'+color+'">\n';
    ht+='<h1><center>'+header+'</center></h1>';
    ht+='<img src="HP_New_Logo_2D.png" alt="HP" height="42" width="42" />';
    ht+='<table>';
    return ht;

def generateHtmlTableHeader(headerStr,isgrid):
    if isgrid == 0:
        headerPrint='<table class="gridtable"><tr><th><strong>'+string.replace(headerStr,',','<strong></th><th>')+'</th></tr>';
    else:
        headerPrint='<table style="border-collapse:collapse;border:1px solid #808080;background-color:lightyellow;"><tr><th><strong>'+string.replace(headerStr,',','<strong></th><th>')+'</th></tr>';
    return headerPrint;


#<?xml version="1.0"?>
#-<graph decimalPrecision="0" showNames="1"> <set value="20" name="USA"/> <set value="7" name="France"/> <set value="12" name="India"/> <set value="11" name="England"/> <set value="8" name="Italy"/> <set value="19" name="Canada"/> <set value="15" name="Germany"/> </graph>

def generateXmlPie(datStr,tagStr,capt,shownames,isbar=0,yaxismaxvalue=50,color='FF0000,33FF00,AFD8F8,F6BD0F,8BBA00,FF8E46,008E8E,D64646,8E468E,588526,B3AA00,008ED6,9D080D'):
    colorlist=color.split(',');
    
    xmdat='<?xml version=\'1.0\'?>';
    if isbar == 0:
        xmdat+='<graph decimalPrecision=\'2\' showNames=\''+str(shownames)+'\' caption=\''+capt+'\'>';
    else:
        xmdat+='<graph decimalPrecision=\'2\' yaxismaxvalue=\''+str(yaxismaxvalue)+'\' showNames=\''+str(shownames)+'\' caption=\''+capt+'\'>';
    datSplit=datStr.split(',');
    tagSplit=tagStr.split(',');
    i=0;
    j=0;
    for dt in datSplit:
        if j>len(colorlist)-1:
            j=0;
        xmdat+='<set value=\''+str(dt)+'\' name=\''+tagSplit[i]+'\' color =\''+colorlist[j]+'\'/>';
        i+=1;
        j+=1;
    xmdat+='</graph>'
    return xmdat;


#-<dataset anchorBgColor="1D8BD1" anchorBorderColor="1D8BD1" color="1D8BD1" seriesName="Offline Marketing">
#<set value="1327"/> <set value="1826"/> <set value="1699"/> <set value="1511"/>
#<set value="1904"/> <set value="1957"/> <set value="1296"/> </dataset> -<dataset anchorBgColor="F1683C" anchorBorderColor="F1683C" color="F1683C" seriesName="Search"> <set value="2042"/>

def generateMSLine(category,series,datrow,yaxisMaxValue= 5000,caption='',vertical = '0',Verticalposition='Q 20103' ):
    color='1D8BD1,F1683C,2AD62A,CC00CC,FFFF00,F1683D,FBDC25,C8FE2E,FF5500,2BD62A,DCDC25,1E8BC1';
    
    colorlist=color.split(',');
    xmdat='<?xml version=\'1.0\'?>';
    xmdat+='<graph rotateNames=\'1\' yaxismaxvalue=\''+str(yaxisMaxValue)+'\' yaxisminvalue=\'0\' numVdivlines=\'0\' numdivlines=\'3\' showvalues=\'0\' decimalPrecision=\'0\' formatNumberScale=\'0\' hovercapborder=\'F47E00\' hovercapbg=\'FFECAA\' caption=\''+caption+'\'>';
    xmdat+='<categories>';
    for catrec in category:
        if catrec==Verticalposition:
            xmdat+='<vLine color=\'FF5904\' thickness= \'3\' labelPosition=\'1\' label=\'HP\' />';
        xmdat+='<category name=\''+catrec+'\'/>';
    xmdat+='</categories>';
    i=0;
    j=0;
    curseries='';
    preseries='';
    for dat in datrow:
        dt=dat.split(':')
        curseries=dt[0];
        if curseries != preseries:
            if i > 0:
                xmdat+='</dataset>';
            i=i+1;
            j=j+1;
            if j >= len(colorlist) - 1:
                j=0;
            xmdat+='<dataset anchorBgColor=\''+colorlist[j]+'\' anchorBorderColor=\''+colorlist[j]+'\' color=\''+colorlist[j]+'\' seriesName=\''+curseries+'\'>';
            preseries=curseries;
        if len(dt) == 3:
            xmdat+='<set value=\''+dt[1]+'\' link=\''+dt[2]+'\'/>';
        else:
            xmdat+='<set value=\''+dt[1]+'\'/>';
    xmdat+='</dataset>'
    xmdat+='</graph>';
    return xmdat;



#<?xml version="1.0"?>
#-<graph showValues="0" numberPrefix="$" numDivLines="3" decimalPrecision="0" subCaption="( 2004 to 2006 )"
#caption="Cumulative Sales" yAxisName="Sales" xAxisName="Products"> -<categories>
#<category name="Product A"/>
#<category name="Product B"/> <category name="Product C"/> <category name="Product D"/> <category name="Product E"/>
#</categories>
#-<dataset showValues="0" color="AFD8F8" seriesName="2004"> <set value="25601.34"/> <set value="20148.82"/>
#<set value="17372.76"/> <set value="35407.15"/> <set value="38105.68"/> </dataset> -<dataset showValues="0" color="F6BD0F" seriesName="2005">
#<set value="57401.85"/> <set value="41941.19"/> <set value="45263.37"/> <set value="117320.16"/> <set value="114845.27"/>
#</dataset> -<dataset showValues="0" color="8BBA00" seriesName="2006"> <set value="45000.65"/> <set value="44835.76"/>
#<set value="18722.18"/> <set value="77557.31"/> <set value="92633.68"/> </dataset> </graph>
def generateXmlStBar(datStr,tagStr,catStr,colStr,capt):
    xmdat='<?xml version=\'1.0\'?>';
    xmdat+='<graph decimalPrecision=\'2\' showNames=\'1\' caption=\''+capt+'\'>';
    xmdat+='<categories>';
    catSplit=catStr.split(',');
    for ct in catSplit:
        xmdat+='<category name=\''+ct+'\'/>';
    xmdat+='</categories>';
    tagSplit=tagStr.split(',');
    datSplit=datStr.split(',');
    currentTag ='';
    prevTag='';
    colorSplit=colStr.split(',');
    i=0;
    for dt in datSplit:
        colSplit=dt.split(':');
        currentTag=colSplit[0];
        if prevTag != currentTag:
            if i > 0:
                xmdat+='</dataset>';    
            prevTag=currentTag;
            xmdat+='<dataset showValues=\'0\' color=\''+colorSplit[i]+'\' seriesName=\''+currentTag+'\'>';
            i+=1;
        xmdat+='<set value=\''+colSplit[1]+'\'/>';
    xmdat+='</dataset>';
    xmdat+='</graph>';
    return xmdat;


#<?xml version="1.0"?>
#-<graph alternateHGridAlpha="5" divLineAlpha="20" divLineColor="ff5904" AlternateHGridColor="ff5904" showAlternateHGridColor="1" showValues="0" showNames="1" numberPrefix="$" formatNumberScale="0" decimalPrecision="0" yAxisName="Sales" yAxisMinValue="15000" xAxisName="Month" subcaption="For the year 2004" caption="Monthly Sales Summary">
#<set hoverText="January" value="17400" name="Jan"/> <set hoverText="February" value="19800" name="Feb"/> <set hoverText="March" value="21800" name="Mar"/> <set hoverText="April" value="23800" name="Apr"/> <set hoverText="May" value="29600" name="May"/> <set hoverText="June" value="27600" name="Jun"/> <set hoverText="July" value="31800" name="Jul"/> <set hoverText="August" value="39700" name="Aug"/> <set hoverText="September" value="37800" name="Sep"/> <set hoverText="October" value="21900" name="Oct"/> <set hoverText="November" value="32900" name="Nov"/> <set hoverText="December" value="39800" name="Dec"/> </graph>
def generateHtmlGridLine(datString,capt,yaxisname,xaxisname,yaxisminval,showval=0,showname=1):
    dataSplit=datString.split(',');    
    xmdat='<?xml version=\'1.0\'?>';
    xmdat+='<graph alternateHGridAlpha=\'5\' divLineAlpha=\'20\' divLineColor=\'ff5904\' AlternateHGridColor=\'ff5904\'';
    xmdat+=' showAlternateHGridColor=\'1\' showValues=\''+str(showval)+'\' showNames=\''+str(showname)+'\' formatNumberScale=\'0\' decimalPrecision=\'0\' yAxisName=\''+yaxisname;
    xmdat+='\' yAxisMinValue=\''+str(yaxisminval)+'\' xAxisName=\''+xaxisname+'\' caption= \''+capt+'\'>';
    for dt in dataSplit:
        dataVal=dt.split(':');
        xmdat+='<set hovertext=\''+dataVal[0]+'\' value= \''+str(dataVal[1])+'\'/>';
    xmdat+='</graph>';
    return xmdat;
    
#10234.html#12034;Dummy;Dummy;10945;1567|102345.html#120345;Dummy;Dummy;10945;1567
def generateHtmlDataTable(dataString,solidGrid):
    newrowList=[];
    rowSplit=dataString.split('|');
    j=0;
    for row in rowSplit:
        newcolList=[];
        columnsSplit=row.split(';');
        i=0
        for coln in columnsSplit:
            colSplit =coln.split('#');
            if len(colSplit) > 1:
                if colSplit[0] !='':
                    newcolList.append('<a href="'+colSplit[0]+'">'+colSplit[1]+'</a>');
            else:
                newcolList.append(coln);
            i+=1;
        j+=1;
        newrowList.append(string.join(newcolList,';'));
    datStr=string.join(newrowList,'|');
    if solidGrid == 1:
        repStr='<td style="border:10px solid lightyellow">';
    else:
        repStr='<td>';
        
    dataRows=string.replace(datStr,';','</td>'+repStr);
    dataPrint='<tr>'+repStr+string.replace(dataRows,'|','</td></tr><tr>'+repStr+'\n')+'</td></tr>\n</table>';
    return dataPrint

def buildtab(tabList):
    tabs=tabList.split(';');
    ht='\t<ul id="tablist">\n';
    i=1;
    for tab in tabs:
        tabl=tab.split(' ');
        if len(tabl)>1:
            tabstr=string.join(tabl);
        else:
            tabstr=tab;
            
        #if i==1:
        #    ht+='\t<li id="li_'+str(tabstr)+'" onclick="tab(\''+str(tabstr)+'\')"><a class="current" href="#'+str(tabstr)+'"  >'+str(tab)+'</a></li>\n';    
        #else:
        ht+='\t<li id="li_'+str(tabstr)+'" onclick="tab(\''+str(tabstr)+'\')"><a href="#'+str(tabstr)+'"  >'+str(tab)+'</a></li>\n';
    ht+='\t</ul>\n';
    return ht;
    
def addtabContent(id,tabval):
    ht='';
    ht+='<div class="content" id="'+str(id)+'">\n';
    ht+='\t'+str(tabval)
    ht+='</div>\n';
    return ht;
    
    
def addtabscript(tabList):    
    ht='\t<script type="text/javascript">\n';
    ht+='\t\tfunction tab(tab) {\n';
    ht+='\t\t\tdocument.getElementById(tab).style.display = \'none\';\n';
    tabs=tabList.split(';');
    
    for tab in tabs:
        tabl=tab.split(' ');
        if len(tabl)>1:
            tabstr=string.join(tabl);
        else:
            tabstr=tab;
            
        ht+='\t\t\tdocument.getElementById(\''+str(tab)+'\').style.display = \'none\';\n';
        ht+='\t\t\tdocument.getElementById(\'li_'+str(tab)+'\').setAttribute(\'class\', \'\');\n';
    
    ht+='\t\t\tdocument.getElementById(tab).style.display = \'block\';\n';
    ht+='\t\t\tdocument.getElementById(\'li_\'+tab).setAttribute(\'class\', \'active\');\n';
    ht+='\t\t}\n';
    ht+='\t</script>';
    ht+='\t<script>tab(\''+tabs[0]+'\');</script>';
    return ht;