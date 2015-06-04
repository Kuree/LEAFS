import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import paramiko
from paho.mqtt.client import Client
from paho.mqtt.publish import single
import time
import struct
import random
import threading
import timeit
from core import msgEncode
#import numpy as np
#import matplotlib.pyplot as plt
#import scipy.stats as stats


_ssh_list = []
_delay_time_dict = {}
_level = "NULL"
_dump_count = 0
_should_dump =  False

def connect(id, process_count, thread_count, ssh_list, interval):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    server_name = "brki164-lnx-" + str(id)
    ssh.connect(server_name)
    channel = ssh.invoke_shell()
    command = get_command_str(id, process_count, thread_count, interval)
    ssh.exec_command(command)
    ssh_list.append(ssh)

def get_command_str(id, process_count, thread_count, interval):
    base_id = id * process_count * thread_count * interval
    result = "cd LEAFS/test && source run_streaming.sh  {0:d} {1:d} {2:d} {3:d}".format(process_count, base_id, thread_count, interval)
    return result    

def disconnect(ssh_list):
    for ssh in ssh_list:
        ssh.exec_command("killall python")
        ssh.close()

def dump_data_points():
    global _MACHINE_COUNT, _PROCESS_COUNT, _THREAD_COUNT, _INTERVAL, _total_time, _dump_count, _delay_time_dict
    data_points = []
    for key in _delay_time_dict:
        for x in _delay_time_dict[key]:
            data_points.append(x)
    data_points.sort()

    filename = "../log/{0:d}-{1:d}-{2:d}-{3:d}-{4}-{5:d}".format(_MACHINE_COUNT, _PROCESS_COUNT, _THREAD_COUNT, _INTERVAL, _level, _dump_count)
    with open(filename, 'w') as f:
        for data_point in data_points:
            f.write(str(data_point) + '\n')
    for key in _delay_time_dict:
        _delay_time_dict[key] = []

    _dump_count += 1 

def on_message(mqttc,  obj, msg):
    received_time = timeit.default_timer()
    global _delay_time_dict, _dump_count
    if msg.topic == "benchmark/dump": 
        # dump the data points to a file to reduce the memory
        print("Start to dumping the data points")
        dump_data_points()
        return  # no need to store the data    
    payload = msg.payload
    timestamp, seq, value = msgEncode.decode(payload)
    id = int(msg.topic.split("/")[-1])
    delay = received_time - value
    if id in _delay_time_dict:
        _delay_time_dict[id].append(delay)
    else:    # first time doesn't count
        _delay_time_dict[id] = []

def calculate_delay_time(delay_dict):
    result, count = 0, 0
    for id in delay_dict:
        for delay in delay_dict[id]:
            result += delay
            count += 1
    return result / count, count


def choose_topic_to_subscribe(client, machine_count, process_count, thread_count, monitor_count, interval):
    samples = random.sample(range(process_count * thread_count * machine_count * interval), monitor_count)
    for i in samples:
        client.subscribe("benchmark/" + str(i))
    

if __name__ == "__main__":
    # arg list
    # arg1 machine count
    # arg2 process count
    # arg3 thread count
    # arg4 sample per second

    if len(sys.argv) != 8:
        print("Invalid argument")

    _PROCESS_COUNT = int(sys.argv[2])
    _THREAD_COUNT = int(sys.argv[3])
    _MACHINE_COUNT = int(sys.argv[1])
    _INTERVAL = int(sys.argv[4])
    _WAIT_TIME = 120
    _MONITOR_COUNT = 300
    _level = sys.argv[5]

    _count_interval = int(sys.argv[7])
    _total_time = int(sys.argv[6])

    client = Client()
    client.on_message =on_message
    client.connect("mqtt.bucknell.edu")
    client.loop_start()
    choose_topic_to_subscribe(client, _PROCESS_COUNT, _MACHINE_COUNT,  _THREAD_COUNT, _MONITOR_COUNT, _INTERVAL)
    client.subscribe("benchmark/dump")
    thread_list= []

    for i in range(_MACHINE_COUNT):
        t = threading.Thread(target = connect, args =(i + 1, _PROCESS_COUNT, _THREAD_COUNT, _ssh_list, _INTERVAL))
        t.start()
        thread_list.append(t)

    for t in thread_list:
        t.join()
    
    while len(_delay_time_dict) == 0:
        time.sleep(1)
    
    start_time = time.time()
    while time.time() < start_time + (_total_time + _INTERVAL * 1.1):
        time.sleep(_count_interval)
        # dump the data points to a file
        # so that it won't run out of memory
        single("benchmark/dump", payload = 0, hostname = "mqtt.bucknell.edu")
        

    
    
    disconnect(_ssh_list)

    # calculate the average delay time
    #delay, count = calculate_delay_time(_delay_time_dict)
    #print("The average delay time is: {0:f}. The message collected is {1:d}".format(delay, count))
    
    
    #hmean = np.mean(data_points)
    #hstd = np.std(data_points)
    #pdf = stats.norm.pdf(data_points, hmean, hstd)
    #plt.plot(data_points, pdf)
    #plt.show()


