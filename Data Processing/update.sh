cp parser/statsfilelib.py exec/
cp updatebin.sh exec/
cp kill_proc.sh exec/
cp delete_splitFile.sql exec/
cp parser/dataCopy.py exec/
cd exec 
zip update statsfilelib.py updatebin.sh delete_splitFile.sql kill_proc.sh dataCopy.py
rm -rf statsfilelib.py
rm -rf updatebin.sh
rm -rf delete_splitFile.sql
rm -rf kill_proc.sh
rm -rf dataCopy.py
