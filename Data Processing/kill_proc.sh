rm -rf kill$1.sh

ps aux | grep $1 | gawk -F" " '{print $2}' |\
while read f
do 
  echo "echo "Killing process: $f" " >> kill$1.sh
  echo "kill -9 $f" >> kill$1.sh
done
chmod +x kill$1.sh
