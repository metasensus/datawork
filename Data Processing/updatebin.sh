chmod +x kill_proc.sh
./kill_proc.sh copymeta
chmod +x killcopymeta.sh
./killcopymeta.sh
./kill_proc.sh parser
chmod +x killparser.sh
./killparser.sh
./kill_proc.sh dataCopy
chmod +x killdataCopy.sh
./killdataCopy.sh
mysql -u 'procuser' -p  ods < delete_splitFile.sql
python copymeta.py &
python parser.py &
python dataCopy.py &
