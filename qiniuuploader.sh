#!/bin/bash

# Dump market data from mysql databases

timestamp=`date --date="1 day ago" +%Y%m%d`
echo Timestamp is :$timestamp
mysql_host="127.0.0.1"
mysql_pwd="bitcoin"
mysql_db="bcexmerged"

qiniu_ak="9j6jwFePHFbA5iD5cTdvnZE0s6tTac08cuK7z7na"
qiniu_sk="5E63DQz0XOxCGvOhmf0GsCaoP4vIwKiX3vXPfd6W"
qiniu_bucket="bitcoin"

table_list=()

for item in `mysql -h $mysql_host -u root -p$mysql_pwd $mysql_db -AN -e "show tables;" 2>/dev/null `
do
    if [[ $item =~ "snapshot_$timestamp" ]]
    then
        table_list+=($item)
    fi
done


if [ ! -d stored_data ] ; then
    mkdir stored_data
fi

cd stored_data

if [ ${#table_list[@]} -gt 0 ]
qshell account  $qiniu_ak  $qiniu_sk
then
    for table in  ${table_list[@]};
    do
    echo $table
    mysqldump -h $mysql_host -uroot -p$mysql_pwd $mysql_db $table > $table.sql
    tar cjf $table.tgz  $table.sql
    rm  $table.sql
    qshell fput $qiniu_bucket $table.tgz $table.tgz
    done
fi

cd -
rm -rf stored_data
