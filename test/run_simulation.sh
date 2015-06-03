#!/bin/bash
# collect the system information
echo "Activating python virtualenv"
source mqtt-env/bin/activate

if [ "$#" < 5 ]; then
    echo "Illegal number of parameters"
    exit
fi
if [ "$#" -eq 5 ]; then
    echo "Use default setting to benchmark"
    level="None"
elif [ "$#" -eq 6 ]; then
    echo "Using provided configuration"
    if [ "$6" == "-p" ]; then
        level="DATABASE"
        echo "Starting the SQL agent"
        python sqlAgent.py &
        sleep 1
    fi
fi

MACHINE_COUNT=$1
PROCESS_COUNT=$2
THREAD_COUNT=$3
TOTAL_TIME=$4
INTERVAL=$5


for fre in 10 20
do 
    echo "Running simulation with machine count:  $MACHINE_COUNT. process count: $PROCESS_COUNT. thread count: $THREAD_COUNT"
    echo "frequency: $fre Benchmark level: $level"
    python connect_run.py $MACHINE_COUNT $PROCESS_COUNT $THREAD_COUNT $fre $level $TOTAL_TIME $INTERVAL
done

# exit
echo "kill all running python instance"
killall python
