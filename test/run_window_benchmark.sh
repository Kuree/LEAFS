#!/bin/bash

if [ "$#" -ne 5 ]; then
    echo "Illegal number of parameters"
    exit
fi

echo "Starting window agent"
python run_window_agent.py &

sleep 0.5

echo "Starting compute agent"
python run_compute_agent.py &

sleep 0.5

echo "Starting benchmark..."

python run_window_benchmark.py &
