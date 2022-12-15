#!/bin/bash
echo "Worker$1@$(hostname)"
$SPARK_HOME/sbin/start-worker.sh spark://$HOSTNAME:7077 > ./logs/Node$1.o 2> ./logs/Node$1.err
for (( ; ; ))
do
   sleep 1
done
