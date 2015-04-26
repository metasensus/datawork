# copying from directories
echo "###################################################################"
echo "# Start of the data copy to hdfs                                  #"
echo "###################################################################" 
echo "Copying Script Files from directories......"
cp /ods84/eventlog/*.sh $HOME/
cp /ods84/alertdnew/*.sh $HOME
echo "Changing mode of the script files ......."
chmod +x $HOME/*.sh
echo "Executing directory creation in hdfs (Ignore the warning errors. This is because either the directory exists or there is a bug in hdfs 2.2.0 when one of the libs was creared in 32 biti)" 
echo "Starting Eventlog................"
echo " "
$HOME/eventlog_createhdfsdir.sh
echo "Starting Alert................"
echo " "
$HOME/alertdnew_createhdfsdir.sh
echo "Copying data for eventlog........"
$HOME/eventlog_copydata.sh
echo "Copying data for alert........"
$HOME/alertdnew_copydata.sh
