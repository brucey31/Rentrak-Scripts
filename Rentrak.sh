#!/bin/bash

HOST='ftp.iboe.com'
USER='ukamt_uk'
PASSWD='GnR109TeO'


cd /home/Rentrak



ftp -n $HOST <<END_SCRIPT
quote USER $USER
quote PASS $PASSWD
cd OUT

get ukFLYTXTwrk.txt
get ukFLYFILMwrk.txt

echo 'Files downloaded from FTP Server'

quit 
END_SCRIPT

sudo mv ukFLYTXTwrk.txt Cinemas_$(date +\%Y\%m\%d).csv
sudo mv ukFLYFILMwrk.txt Films_$(date +\%Y\%m\%d).csv
echo 'Name changes attempted'

sudo su hdfs <<EOF


hadoop fs -put Cinemas_$(date +\%Y\%m\%d).csv /data_import/RentrakCinemaListings/Raw/Cinemas
hadoop fs -put Films_$(date +\%Y\%m\%d).csv /data_import/RentrakCinemaListings/Raw/Films
EOF

sudo rm -r /home/Rentrak/Cinemas
sudo rm -r /home/Rentrak/Films




echo 'file put into hdfs'