import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from client import QueryClient
import time
import random
from core import ComputeCommand
from core import msgEncode

MONITOR_COUNT = 300
INTERVAL = 60

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Invalid argument")
        exit(1)
    total_id = int(sys.argv[1])
    q = QueryClient("benchmark")
    delay_dict = {}

    def on_message(topic, msg):
        receive_time = time.time()
        timestamp = encoder.deocde(msg)
        delay = receive_time - timestamp
        global delay_dict
        if topic in delay_dict:
            delay_dict[topic].append(delay)
        else:
            delay_dict[topic] = [delay]

    q.on_message = on_message
    random_id_list = random.sample(range(total_id), MONITOR_COUNT)
    for i in random_id_list:
        c = ComputeCommand()
        c.add_compute_command(ComputeCommand.AVERAGE, INTERVAL)
        q.add_stream("benchmark", "benchmark/" + str(i), c.to_obj())
    q.connect()
