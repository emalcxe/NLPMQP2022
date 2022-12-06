#!/bin/bash
echo "Worker$1@$(hostname)"
$SPARK_HOME/sbin/start-worker.sh spark://$HOSTNAME:7077 > $CLUSTER_HOME/logs/Node$1.o 2> $CLUSTER_HOME/logs/Node$1.err
for (( ; ; ))
do
   sleep 1
done
