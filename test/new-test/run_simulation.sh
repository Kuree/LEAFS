#!/bin/bash
MACHINE_COUNT=25
PROCESS_COUNT=10
THREAD_COUNT=4
echo "Activating python virtualenv"
source mqtt-env/bin/activate
# collect the system information

#while getopts ":a" opt; do
#  case $opt in
#    p)
#      echo "Use datastore" >&2
#      python sqlAgent.py &
#      LEVEL="DATABASE"
#      ;;
#    \?)
#      echo "Invalid option: -$OPTARG" >&2
#      exit 1
#      ;;
#  esac
#done

QUERY_ARG="-q"
if [ -z "$var" ];
    then
        echo "Use default setting to benchmark" 
        level="NONE"
else

    if [ $1=$QUERY_ARG ];
          then
            level="DATABASE"
            echo "Starting the SQL agent"
            python sqlAgent.py &
            sleep 1
    fi
fi

for fre in 10 20
do 
    echo "Running simulation with machine count:  $MACHINE_COUNT. process count: $PROCESS_COUNT. thread count: $THREAD_COUNT"
    echo "frequency: $fre Benchmark level: $level"
    python connect_run.py $MACHINE_COUNT $PROCESS_COUNT $THREAD_COUNT $fre $level
done

# exit
echo "kill all running python instance"
killall python
