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
        
        #bar css 
        datstr+='\n\t\t\t.bar   { fill: orange; }';
        datstr+='\n\t\t\t.bar:hover {fill: blue ;}';
        datstr+='\n\t\t\t.x.axis path{display: none; }';
        
        #tool tip css 
        datstr+='\n\t\t\t.d3-tip';
        datstr+='\n\t\t\t{';
        datstr+='\n\t\t\tline-height: 1;';
        datstr+='\n\t\t\tfont-weight: bold;';
        datstr+='\n\t\t\tpadding: 12px;';
        datstr+='\n\t\t\tbackground: rgba(0, 0, 0, 0.8);';
        datstr+='\n\t\t\tcolor: #fff;';
        datstr+='\n\t\t\tborder-radius: 2px;';
        datstr+='\n\t\t\t }';

        #tip icon css
        datstr+='\n\t\t\t/* Creates a small triangle extender for the tooltip */';
        datstr+='\n\t\t\t.d3-tip:after';
        datstr+='\n\t\t\t{';
        datstr+='\n\t\t\tbox-sizing: border-box;';
        datstr+='\n\t\t\tdisplay: inline;';
        datstr+='\n\t\t\tfont-size: 10px;';
        datstr+='\n\t\t\twidth: 100%;';
        datstr+='\n\t\t\tline-height: 1;';
        datstr+='\n\t\t\tcolor: rgba(0, 0, 0, 0.8);';
        datstr+="""\n\t\t\tcontent: "\\25BC";""";
        datstr+='\n\t\t\tposition: absolute;';
        datstr+='\n\t\t\ttext-align: center;';
        datstr+='\n\t\t\t}';

        datstr+='\n\t\t\t/* Style northward tooltips differently */';
        datstr+='\n\t\t\t.d3-tip.n:after';
        datstr+='\n\t\t\t{';
        datstr+='\n\t\t\tmargin: -1px 0 0 0;';
        datstr+='\n\t\t\ttop: 100%;';
        datstr+='\n\t\t\tleft: 0;';
        datstr+='\n\t\t\t}';

        #table css
        datstr+='\n\t\t\ttable';
        datstr+='\n\t\t\t{';
        datstr+='\n\t\t\t border: 1px solid #666;';   
        datstr+='\n\t\t\t width: 37%;';
	datstr+='\n\t\t\t top: 47%;';
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
 

        datstr+='\n\t\t</style>'
        
        #HTML body
        datstr+='\n\t\t<body>'
        datstr+='\n\t\t\t<script src="http://d3js.org/d3.v3.min.js"></script>';
        datstr+='\n\t\t\t<script src="http://labratrevenge.com/d3-tip/javascripts/d3.tip.v0.6.3.js"></script>';
        datstr+='\n\t\t\t<script type="text/javascript" src="http://ajax.googleapis.com/ajax/libs/jquery/1.6.2/jquery.min.js"> </script>'


        datstr+='\n\t\t\t<div class="page-header" id="header">';
        datstr+='\n\t\t\t\t<h1></h1>';
        
        datstr+='\n\t\t\t</div>';
        #right side button menu bar
        datstr+='\n\t\t\t<div class="btn-group-vertical pull-right">';
        datstr+='\n\t\t\t\t<button type="button" class="btn btn-default">';
	datstr+='\n\t\t\t\t<a href="../ssd/7000r.html">SSD</a>';
	datstr+='\n\t\t\t\t</button> ';
 
	datstr+='\n\t\t\t\t<button type="button" class="btn btn-default">';
	datstr+='\n\t\t\t\t<a href="../nl/7000r.html">NL</a>';
	datstr+='\n\t\t\t\t</button>'
	
	datstr+='\n\t\t\t\t<button type="button" class="btn btn-default">'
	datstr+='\n\t\t\t\t<a href="../fc/7000r.html">FC</a>'
	datstr+='\n\t\t\t\t</button>'
        datstr+='\n\t\t\t</div>';
      
        #script for d3.js bar
        datstr+='\n\t\t\t<script>';
        datstr+='\n\t\t\t\t var margin = {top: 150, right: 0, bottom: 40, left: 400},';
        datstr+='\n\t\t\t\twidth = 960 - margin.left - margin.right,';
        datstr+='\n\t\t\t\theight = 700 - margin.top - margin.bottom;';

        datstr+='\n\t\t\t\tvar formatPercent = d3.format(".0");';
        datstr+='\n\t\t\t\tvar x = d3.scale.ordinal()';
        datstr+='\n\t\t\t\t\t.rangeRoundBands([0, width], .1,0.5);  //0.5 is the distance from y axis of bars, more numbers means more distance;'
        datstr+='\n\t\t\t\tvar y = d3.scale.linear()'
        datstr+='\n\t\t\t\t\t.range([height, 0]);';
        datstr+='\n\t\t\t\tvar xAxis = d3.svg.axis()';
        datstr+='\n\t\t\t\t\t.scale(x)';
        datstr+='\n\t\t\t\t\t.orient("bottom");'
        
        datstr+='\n\t\t\t\tvar yAxis = d3.svg.axis()'
        datstr+='\n\t\t\t\t\t.scale(y)'
        datstr+='\n\t\t\t\t\t.orient("left")'
        datstr+='\n\t\t\t\t\t.tickFormat(formatPercent);'
        
        datstr+='\n\t\t\t\tvar tip = d3.tip()'
        datstr+="""\n\t\t\t\t\t.attr('class', 'd3-tip')"""
        datstr+='\n\t\t\t\t\t.offset([-10, 0])'
        datstr+='\n\t\t\t\t\t.html(function(d) {'
        datstr+="""\n\t\t\t\t\treturn "<strong>Count:</strong> <span style='color:red'>" + d[y1] + "</span>";})"""
        datstr+='\n\t\t\t\tvar svg = d3.select("body").append("svg")'
        datstr+='\n\t\t\t\t\t.attr("width", width + margin.left + margin.right)'
        datstr+='\n\t\t\t\t\t.attr("height", height + margin.top + margin.bottom)'
        datstr+='\n\t\t\t\t\t.append("g")'
        datstr+='\n\t\t\t\t\t.attr("transform", "translate(" + margin.left + "," + margin.top + ")");'
        datstr+='\n\t\t\t\tsvg.call(tip);'
        
        datstr+='\n\t\t\t\tvar url = "'+json_url+json_file+'";'
        datstr+='\n\t\t\t\tvar x1,y1;'
        datstr+='\n\t\t\t\tvar z,head;'
        datstr+='\n\t\t\t\tz="percent";'
        
        #function to read from the json
        datstr+='\n\t\t\t\td3.json(url, function(error, data)'
        datstr+='\n\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\t\t$.each(data.xaxis, function(i, f)'
        datstr+='\n\t\t\t\t\t{'
	datstr+='\n\t\t\t\t\tx1=f.name;';
	datstr+='\n\t\t\t\t\t});'

        datstr+='\n\t\t\t\t\t $.each(data.yaxis, function(i, f)';
        datstr+='\n\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\ty1=f.name;'
        datstr+='\n\t\t\t\t\t});' 
    
        datstr+='\n\t\t\t\t\t$.each(data.data, function(i, d)'
        datstr+='\n\t\t\t\t\t{'
        datstr+='\n\t\t\t\t\td[y1]=+d[y1];'
        datstr+='\n\t\t\t\t\t});'
     
   

        datstr+='\n\t\t\t\tx.domain(data.data.map(function(d) { return d[x1]; }));  '
        datstr+='\n\t\t\t\ty.domain([0, d3.max(data.data, function(d) { return d[y1]; })]);'

        datstr+='\n\t\t\t\tsvg.append("g")'
        datstr+='\n\t\t\t\t\t.attr("class", "x axis")'
        datstr+='\n\t\t\t\t\t.attr("transform", "translate(0," + height + ")")'
        datstr+='\n\t\t\t\t\t.call(xAxis)'
	datstr+='\n\t\t\t\t\t.append("text")'
	datstr+='\n\t\t\t\t\t.attr("fill","Blue")'
	datstr+='\n\t\t\t\t\t.attr("x", 230)'
	datstr+='\n\t\t\t\t\t.attr("y",35)'
        datstr+='\n\t\t\t\t\t.attr("dx", "0.2em")'
	datstr+='\n\t\t\t\t\t.style("text-anchor", "middle")'
	datstr+='\n\t\t\t\t\t.text(x1);'
  
        datstr+='\n\t\t\t\tsvg.append("g")'
        datstr+='\n\t\t\t\t\t.attr("class", "y axis")'
        datstr+='\n\t\t\t\t\t.call(yAxis)'
        datstr+='\n\t\t\t\t\t.append("text")'
	datstr+='\n\t\t\t\t\t.attr("fill","Blue")'
        datstr+='\n\t\t\t\t\t.attr("transform", "rotate(-90)")'
        datstr+='\n\t\t\t\t\t.attr("y", 6)'
        datstr+='\n\t\t\t\t\t.attr("dy", ".3em")'
        datstr+='\n\t\t\t\t\t.style("text-anchor", "middle")'
	datstr+='\n\t\t\t\t\t.text(y1);'

        datstr+='\n\t\t\t\tsvg.selectAll(".bar")'
        datstr+='\n\t\t\t\t\t.data(data.data)'
        datstr+='\n\t\t\t\t\t.enter().append("rect")'
        datstr+='\n\t\t\t\t\t.attr("class", "bar")'
        datstr+='\n\t\t\t\t\t.attr("fill", "teal")'
        datstr+='\n\t\t\t\t\t.attr("x", function(d) { return x(d[x1]); })'
        datstr+='\n\t\t\t\t\t.attr("width", x.rangeBand())'
        datstr+='\n\t\t\t\t\t.attr("y", function(d) { return y(d[y1]); })'
        datstr+='\n\t\t\t\t\t.attr("height", function(d) { return height - y(d[y1]); })'
        datstr+="""\n\t\t\t\t\t.on('mouseover', tip.show)"""
        datstr+="""\n\t\t\t\t\t.on('mouseout', tip.hide)"""
        datstr+='\n\t\t\t\t\t});'
        
        datstr+='\n\t\t\t</script>'
        
        #JS function for reading data for the table
        datstr+='\n\t\t\t<script>'
        datstr+='\n\t\t\t\t$.getJSON(url, function(data) {'
        datstr+='\n\t\t\t\t\tfor (var i in data.data)'
	datstr+='\n\t\t\t\t{'
	datstr+='\n\t\t\t\t\tvar tblRow = "<tr>" + "<td>" + data.data[i][x1] + "</td>" +"<td>" + data.data[i][y1] + "</td>" + "<td>"+data.data[i][z]+"</td>"+ "</tr>"'
	datstr+='\n\t\t\t\t\t$(tblRow).appendTo("#userdata tbody");'
	datstr+='\n\t\t\t\t\t}'
        
        datstr+='\n\t\t\t\t$.each(data.title, function(i, f)'
        datstr+='\n\t\t\t\t{'
        datstr+='\n\t\t\t\t\thead="<h1>"+"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;"+ f.caption + "</h1>"'
        datstr+='\n\t\t\t\t\t$(head).appendTo("#header h1");'
        datstr+='\n\t\t\t\t});'
        datstr+='\n\t\t\t});'

        datstr+='\n\t\t\t</script>'
        
        #funtion for the left side menu for the toogle(drop down)
        datstr+='\n\t\t\t<script>'
        datstr+='\n\t\t\t\t$(document).ready(function () {'
        datstr+="""\n\t\t\t\t\t$('label.tree-toggler').click(function () {"""
	datstr+="""\n\t\t\t\t$(this).parent().children('ul.tree').toggle(300);"""
	datstr+='\n\t\t\t\t\t});'
        datstr+='\n\t\t\t\t});'
        datstr+='\n\t\t\t</script>'
        
        
        datstr+='\n</head>'

        datstr+='\n\t\t\t\t<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">'
        
        datstr+='\n\t\t\t\t\t<div class="container-fluid">'
	datstr+='\n\t\t\t\t\t\t<div class="navbar-header">'
	datstr+='\n\t\t\t\t\t\t<a class="navbar-brand" href="#">SSD Service time distributions</a>'
	datstr+='\n\t\t\t\t\t\t</div>'
	   
	datstr+='\n\t\t\t\t\t<div class="navbar-collapse collapse">'
	datstr+='\n\t\t\t\t\t<ul class="nav navbar-nav navbar-right">'
	datstr+='\n\t\t\t\t\t\t<li><a href="../index.html">Dashboard</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Settings</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Profile</a></li>'
	datstr+='\n\t\t\t\t\t\t<li><a href="#">Help</a></li>'
	datstr+='\n\t\t\t\t\t</ul>'
	datstr+='\n\t\t\t\t\t</div>'

        datstr+='\n\t\t\t\t\t</div>'
        datstr+='\n\t\t\t\t</div>'
        
        datstr+='\n\t\t\t<div class="container-fluid">'
        datstr+='\n\t\t\t\t<div class="row">'
        datstr+='\n\t\t\t\t\t<div class="col-sm-3 col-md-2 sidebar">'
        datstr+='\n\t\t\t\t\t\t <ul class="nav nav-sidebar">'
        datstr+='\n\t\t\t\t\t\t\t<li class="active"><a href="#">Dashboard</a></li>'
        datstr+='\n\t\t\t\t\t\t</ul>'
        
        datstr+='\n\t\t\t\t\t\t<ul class="nav nav-sidebar">'
        datstr+='\n\t\t\t\t\t\t\t<ul class="nav nav-list">'
        datstr+='\n\t\t\t\t\t\t\t\t<li class="active"><label class="tree-toggler"><a href="#">SSD Service time Distributions: 7000</a></label>'
        datstr+='\n\t\t\t\t\t\t\t\t<ul class="nav nav-list tree">'
        datstr+='\n\t\t\t\t\t\t\t\t\t<li><a href="7000w.html">SSD Write</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t<li><a href="7000r.html">SSD Read</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t<li><a href="7000t.html">SSD Total</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t\t\t</li>'
        datstr+='\n\t\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t\t</ul>'
         
        datstr+='\n\t\t\t\t\t\t<ul class="nav nav-sidebar">'
        datstr+='\n\t\t\t\t\t\t\t<ul class="nav nav-list">'
        datstr+='\n\t\t\t\t\t\t\t\t<li class="active"><label class="tree-toggler"><a href="#">SSD Service time Distributions: 10000</a></label>'
        datstr+='\n\t\t\t\t\t\t\t\t\t<ul class="nav nav-list tree">'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="10000w.html">SSD Write</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="10000r.html">SSD Read</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="10000t.html">SSD Total</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t\t\t</li>'
        datstr+='\n\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t</ul>'
        
        datstr+='\n\t\t\t\t\t\t<ul class="nav nav-sidebar">'
        datstr+='\n\t\t\t\t\t\t\t<ul class="nav nav-list">'
        datstr+='\n\t\t\t\t\t\t\t\t<li class="active"><label class="tree-toggler"><a href="#">SSD Service time Distributions: F Class</a></label>'
        datstr+='\n\t\t\t\t\t\t\t\t\t<ul class="nav nav-list tree">'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="fclassw.html">SSD Write</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="fclassr.html">SSD Read</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="fclasst.html">SSD Total</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t\t\t</li>'
        datstr+='\n\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t</ul>'
        
        datstr+='\n\t\t\t\t\t\t<ul class="nav nav-sidebar">'
        datstr+='\n\t\t\t\t\t\t\t<ul class="nav nav-list">'
        datstr+='\n\t\t\t\t\t\t\t\t<li class="active"><label class="tree-toggler"><a href="#">SSD Service time Distributions: T Class</a></label>'
        datstr+='\n\t\t\t\t\t\t\t\t\t<ul class="nav nav-list tree">'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="tclassw.html">SSD Write</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="tclassr.html">SSD Read</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t\t<li><a href="tclasst.html">SSD Total</a></li>'
        datstr+='\n\t\t\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t\t\t</li>'
        datstr+='\n\t\t\t\t\t\t\t</ul>'
        datstr+='\n\t\t\t\t\t\t</ul>'
        
                
        datstr+='\n\t\t\t\t\t</div>'
        datstr+='\n\t\t\t\t</div>'
        datstr+='\n\t\t\t</div>'
        datstr+='\n\t\t\t<table id= "userdata" border="2" class="table-hover table-striped">'
        datstr+='\n\t\t\t\t<thead>'
        datstr+='\n\t\t\t\t\t<th>Svctime(ms)</th>'
        datstr+='\n\t\t\t\t\t<th>Occurrence</th>'
        datstr+='\n\t\t\t\t\t<th>Percent</th>'
        datstr+='\n\t\t\t\t</thead>'
        datstr+='\n\t\t\t\t<tbody>'
        datstr+='\n\t\t\t\t</tbody>'
        datstr+='\n\t\t\t</table>'

        fl.write(datstr);
        fl.close();
        timestr=time.strftime('%m/%d/%Y %H:%M:%S');
        optimuslogger.write(timestr+'\t'+title+'HTML has been generated'+'\n');
        optimuslogger.close;
    except:
        errlog=open('log/html_error.log','a');
        function='read data-SSD';
        #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
        timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
        errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
        errlog.close();
    
def get_data():
        try:
            connstr='ods/ods@callhomeods.3pardata.com/callhomeods:1521';
            oraconn = oracon.openconnect(connstr);
    
            sqlstmt="SELECT OUTPUT_FILE_NAME,FILE_LOCATION,HTML_TEMPLATE,HTML_FILE_NAME,JSON_URL_HTML,REPORT_TITLE FROM GRAPH_REPORT_DATA where HTML_TEMPLATE='ssdhtml.py'";
            htmlResultset = oracon.execSql(oraconn,sqlstmt);
        
            for rec in htmlResultset:
                json_file= rec[0];
                json_location=rec[1];
                html_template=rec[2];
                html_name=rec[3];
                json_url=rec[4];
                title= rec[5];
            
                print "SSD HTML";
                            
                
            
                read_data(json_file,json_location,html_template,html_name,json_url,title);
            htmlResultset.close();
            oraconn.close();
            
            #os.system('mv /var/www/html/ssd/'+mysqlorgdump+' /root/proc/mysqldump/'+mysqldump_name);
        except:
        
            errlog=open('log/html_error.log','a');
            function='get data-SSD';
            #optimuslogger.write('---------------------------------------------------------------------------------------------------------------------------------------------\n');
            timestr=time.strftime('%m/%d/%Y %H:%M:%S')+' '+function;
            errlog.write(timestr+'\t Error reported: '+str(sys.exc_info()[1])+ '\n');
            errlog.close();      
    

get_data();
   
