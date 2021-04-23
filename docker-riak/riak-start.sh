#!/bin/bash
# script sets node name, starts riak, tails log until container stopped

set -x

IP=$(hostname -i)
sed -i 's/NODE_IP_TO_CHANGE/'"$IP"'/' /etc/riak/riak.conf

/usr/sbin/riak start

# Trap SIGTERM and SIGINT and tail the log file indefinitely
tail -n 1024 -f /var/log/riak/console.log &
PID=$!
trap "/usr/sbin/riak stop; kill $PID" SIGTERM SIGINT
wait $PID