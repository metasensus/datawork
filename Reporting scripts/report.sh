echo 'Running capacity report......'
python capacity_report.py
echo 'Running country trend.....'
python country_trend.py 
echo 'Sales campaign report'
python sales_hierarchy_extract.py
python capacity_report_with_sales.py
echo 'Running license report .....'
python license_with_filter.py
echo 'Creating Drilldown at the inserv level....'
python drilldown.py
echo 'Creating drilldown at country level....'
python country_drilldown.py 
echo 'Creating FTO Trend....'
python fto_trend.py
echo 'inform os'
echo 'Alert extract.....'
python Alert.py

