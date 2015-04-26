#!/usr/bin/env python

import time;
import oracleconnect as oracon;
import os;
import string;
import sys;
   
    
    
    
def read_data(json_file,json_location,html_template,html_name,json_url,title):
    try:
 
        #resultset=setconnection(hostname,dbtype,sourcedb,dbuser,query_string);
        fl=open(html_name,'w');
        optimuslogger=open('log/html.log','a');
	optimuslogger.flush();
	datarray=[];
        datstr='\n<html>';
        datstr+='\n<head>';
        datstr+='\n<meta http-equiv="X-UA-Compatible" content="IE=edge,chrome=1">';
        
        #report title on HTML title bar
        datstr+='\n<title>'+title+'</title>';
        datstr+='\n\t<link href="css/bootstrap.css" rel="stylesheet" type="text/css" />';
        datstr+='\n\t<link href="css/bootstrap.min.css" rel="stylesheet">';
        datstr+='\n\t<link href="css/dashboard.css" rel="stylesheet">';
        
        datstr+='\n\t<link href="css/bootstrap-theme.min.css" rel="stylesheet">';
        datstr+='\n\t<script src="http://ajax.googleapis.com/ajax/libs/jquery/1.11.1/jquery.min.js"></script>';
        datstr+='\n\t<script src="http://maxcdn.bootstrapcdn.com/bootstrap/3.2.0/js/bootstrap.min.js"></script>';
        datstr+='\n\t<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"> </script>';
        
        datstr+='\n\t\t<style type="text/css">';
        
        datstr+='\n\t\t\t.axis path,';
        datstr+='\n\t\t\t.axis line';
        datstr+='\n\t\t\t {';
        datstr+='\n\t\t\tfill: none;';
        datstr+='\n\t\t\tstroke: #000;';
        datstr+='\n\t\t\tshape-rendering: crispEdges;';
        datstr+='\n\t\t\t}';
        
      

        #table css
        datstr+='\n\t\t\ttable';
        datstr+='\n\t\t\t{';
        datstr+='\n\t\t\t border: 1px solid #666;';   
        datstr+='\n\t\t\t width: 37%;';
	datstr+='\n\t\t\t top: 107%;';
	datstr+='\n\t\t\t left: 59%;'
        datstr+='\n\t\t\t position: absolute;';
        datstr+='\n\t\t\t}';

        datstr+='\n\t\t\t th';
        datstr+='\n\t\t\t {';
        datstr+='\n\t\t\t background: #f8f8f8; ';
        datstr+='\n\t\t\tfont-weight: bold;';
        datstr+='\n\t\t\tpadding: 2px;';
        datstr+='\n\t\t\t}';
      
        datstr+='\n\t\t\t#tableContainer-2 {';
        datstr+='\n\t\t\tvertical-align: middle;'
        datstr+='\n\t\t\tdisplay: table-cell;';
        datstr+='\n\t\t\theight: 100%;'
        datstr+='\n\t\t\t}'
        
        datstr+='\n\t\t\tbody{';
        datstr+='\n\t\t\t\ttext-align: center;';
        datstr+='\n\t\t\t}'
        
        datstr+='\n\t\t\tsvg{';
        datstr+='\n\t\t\t\tfont: 10px sans-serif;';
        datstr+='\n\t\t\t}';

        datstr+='\n\t\t</style>'
        
        #HTML body
        datstr+='\n\t\t<body>'
        datstr+='\n\t\t\t<script src="http://d3js.org/d3.v3.min.js"></script>';
        datstr+='\n\t\t\t<script src="http://labratrevenge.com/d3-tip/javascripts/d3.tip.v0.6.3.js"></script>';
        datstr+='\n\t\t\t<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"> </script>'


        datstr+='\n\t\t\t<div class="page-header navbar-fixed-top" id="header">';
        datstr+='\n\t\t\t\t<h1></h1>';
        
        datstr+='\n\t\t\t</div>';
        

        datstr+='\n\t\t\t<div class="container-fluid">';
        #script for d3.js bar
        datstr+='\n\t\t\t<script>';
                
        datstr+='\n\t\t\t\tvar url = "'+json_url+json_file+'";'
        datstr+='\n\t\t\t\tvar x1,y1,z1;'
               
        #function to read from the json
        datstr+='\n\t\t\t\td3.json(url, function(flights)'
        datstr+='\n\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\t\td3.json(url, function(error, data)'
        datstr+='\n\t\t\t\t\t\t{'
	
        datstr+='\n\t\t\t\t\t\t\t$.each(data.xaxis, function(i, f)';
        datstr+='\n\t\t\t\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\t\t\t\t\tx1=f.name;';
	datstr+='\n\t\t\t\t\t\t\t\t});'

        datstr+='\n\t\t\t\t\t\t\t$.each(data.yaxis, function(i, f)';
        datstr+='\n\t\t\t\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\t\t\t\t\ty1=f.name;';
	datstr+='\n\t\t\t\t\t\t\t\t});'
        
        datstr+='\n\t\t\t\t\t\t\t$.each(data.zaxis, function(i, f)';
        datstr+='\n\t\t\t\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\t\t\t\t\tz1=f.name;';
	datstr+='\n\t\t\t\t\t\t\t\t});'
        
        datstr+='\n\t\t\t\tvar m = 10,'
        datstr+='\n\t\t\t\t r = 130,'
        datstr+='\n\t\t\t\tz = d3.scale.category20c();'
        
        datstr+='\n\t\t\t\tvar pie = d3.layout.pie()'
        datstr+='\n\t\t\t\t\t.value(function(d) { return +d[y1]; })'
        datstr+='\n\t\t\t\t\t.sort(function(a, b) { return b[y1] - a[y1]; });'
        
        datstr+='\n\t\t\t\tvar arc = d3.svg.arc()'
        datstr+='\n\t\t\t\t\t.innerRadius(r / 2)'
        datstr+='\n\t\t\t\t\t.outerRadius(r);'
        
        datstr+='\n\t\t\t\tvar airports = d3.nest()'
        datstr+='\n\t\t\t\t\t.key(function(d) { return d[x1]; })'
        datstr+='\n\t\t\t\t\t.entries(flights.data);'
        
        datstr+='\n\t\t\t\tvar svg = d3.select("body").selectAll("div")'
        datstr+='\n\t\t\t\t\t.data(airports)'
        datstr+='\n\t\t\t\t\t.enter().append("div")'
        datstr+='\n\t\t\t\t\t.style("display", "inline-block")'
        datstr+='\n\t\t\t\t\t.style("width", (r + m) * 2 + "px")'
        datstr+='\n\t\t\t\t\t.style("height", (r + m) * 2 + "px")'
        
        datstr+='\n\t\t\t\t\t.append("svg:svg")'
        datstr+='\n\t\t\t\t\t\t.attr("width", 400)'
        datstr+='\n\t\t\t\t\t\t.attr("height", 900)'
        datstr+='\n\t\t\t\t\t.append("svg:g")'
        datstr+='\n\t\t\t\t\t\t.attr("transform", "translate(" + (r + m) + ",200)");'
        
        datstr+='\n\t\t\t\t\tsvg.append("svg:text")'
        datstr+='\n\t\t\t\t\t\t.attr("dy", ".35em")'
        datstr+='\n\t\t\t\t\t\t.attr("text-anchor", "middle")'
        datstr+='\n\t\t\t\t\t\t.text(function(d) { return d.key; });'
        
        datstr+='\n\t\t\t\t\t var g = svg.selectAll("g")'
        datstr+='\n\t\t\t\t\t\t.data(function(d) { return pie(d.values); })'
        datstr+='\n\t\t\t\t\t\t.enter().append("svg:g");'
        
        datstr+='\n\t\t\t\t\t g.append("svg:path")'
        datstr+='\n\t\t\t\t\t\t.attr("d", arc)'
        datstr+='\n\t\t\t\t\t\t.style("fill", function(d) { return z(d.data[z1]); })'
        datstr+='\n\t\t\t\t\t\t.append("svg:title")'
        datstr+='\n\t\t\t\t\t\t.text(function(d) { return d.data[z1] + ": " + d.data[y1]; });'
        
        datstr+='\n\t\t\t\t\tg.filter(function(d) { return d.endAngle - d.startAngle > .2; }).append("svg:text")'
        datstr+='\n\t\t\t\t\t\t.attr("dy", ".35em")'
        datstr+='\n\t\t\t\t\t\t.attr("text-anchor", "middle")'
        datstr+='\n\t\t\t\t\t\t.attr("transform", function(d) { return "translate(" + arc.centroid(d) + ")rotate(" + angle(d) + ")"; })'
        datstr+='\n\t\t\t\t\t\t.text(function(d) { return d.data[z1]; });'
        
        datstr+='\n\t\t\t\t\tfunction angle(d) {'
        datstr+='\n\t\t\t\t\t\tvar a = (d.startAngle + d.endAngle) * 90 / Math.PI - 90;'
        datstr+='\n\t\t\t\t\t\treturn a > 90 ? a - 180 : a;'
        datstr+='\n\t\t\t\t\t}'
        
        datstr+='\n\t\t\t\t})'
        datstr+='\n\t\t\t})' 
         
        datstr+='\n\t\t\td3.json(url, function(demo) {' 
        datstr+='\n\t\t\t\t$.each(demo.title, function(i, f)'
        datstr+='\n\t\t\t\t{'
        datstr+='\n\t\t\t\t\thead="<h1>"+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+ f.caption + "</h1>"'
        datstr+='\n\t\t\t\t\t$(head).appendTo("#header h1");'
        datstr+='\n\t\t\t\t});'
        datstr+='\n\t\t\t});'

        datstr+='\n\t\t\t</script>'
        datstr+='\n\t\t\t</div>'
   
        
        datstr+='\n</head>'

        datstr+='\n\t\t\t\t<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">'
        
        
	datstr+='\n\t\t\t\t\t\t<div class="navbar-header">'
	datstr+='\n\t\t\t\t\t\t<a class="navbar-brand" href="#">Alert Analysis</a>'
	datstr+='\n\t\t\t\t\t\t</div>'
	   
	
	datstr+='\n\t\t\t\t\t<ul class="nav navbar-nav navbar-right">'
	datstr+='\n\t\t\t\t\t\t<li><a href="../index.html">Dashboard</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Settings</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Profile</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Help</a></li>'
	datstr+='\n\t\t\t\t\t</ul>'
	datstr+='\n\t\t\t\t\t</div>'

        
       
        datstr+='\n\t\t\t\t</body>'
        datstr+='\n\t</html>'

        fl.write(datstr);
        fl.close();
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        optimuslogger.write(timestr+'\t'+title+'HTML has been generated'+'\n');
        optimuslogger.close;
    except:
        errlog=open('log/html_error.log','a');
        function='read data-Alert';
        #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
    
def get_data():
        try:
            connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521';
            oraconn = oracon.openconnect(connstr);
    
            sqlstmt="SELECT OUTPUT_FILE_NAME,FILE_LOCATION,HTML_TEMPLATE,HTML_FILE_NAME,JSON_URL_HTML,REPORT_TITLE FROM GRAPH_REPORT_DATA where HTML_TEMPLATE='informhtml.py'";
            htmlResultset = oracon.execSql(oraconn,sqlstmt);
        
            for rec in htmlResultset:
                json_file= rec[0];
                json_location=rec[1];
                html_template=rec[2];
                html_name=rec[3];
                json_url=rec[4];
                title= rec[5];
            
                print "InformOS HTML";
                            
                
            
                read_data(json_file,json_location,html_template,html_name,json_url,title);
            htmlResultset.close();
            oraconn.close();
            
            
        except:
        
            errlog=open('log/html_error.log','a');
            function='get data-Alert';
            #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
            timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
            errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
            errlog.close();      
    

get_data();
   
