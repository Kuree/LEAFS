#!/bin/bash
echo "Starting simulation..."
source ../../mqtt-env/bin/activate
killall python
sleep 5 # make sure the server load is back to normal
cd LEAFS/test
count=$(($1 -1))
for i in $(seq 0 $count);
do
     python start-streaming.py $2 $3 $4 &
done

