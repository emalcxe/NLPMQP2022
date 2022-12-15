#!/bin/bash
echo "Master@$(hostname)"
$SPARK_HOME/sbin/start-master.sh --webui-port 37433 > ./logs/Node$1.o 2> ./logs/Node$1.err
for (( ; ; ))
do
   sleep 1
done
