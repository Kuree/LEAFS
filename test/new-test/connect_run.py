import paramiko
from paho.mqtt.client import Client
from paho.mqtt.publish import single
import time
import struct
import random
import threading
import sys
import timeit
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats


_ssh_list = []
_delay_time_dict = {}
_level = "NULL"


def connect(id, process_count, thread_count, ssh_list, interval, send_count):
    ssh = paramiko.SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    server_name = "brki164-lnx-" + str(id)
    ssh.connect(server_name)
    channel = ssh.invoke_shell()
    command = get_command_str(id, process_count, thread_count, interval, send_count)
    ssh.exec_command(command)
    ssh_list.append(ssh)

def get_command_str(id, process_count, thread_count, interval, send_count):
    base_id = id * process_count * thread_count * interval
    result = "source run_streaming.sh  {0:d} {1:d} {2:d} {3:d} {4:d}".format(process_count, base_id, thread_count, interval, send_count)
    return result    

def disconnect(ssh_list):
    for ssh in ssh_list:
        ssh.close()

def on_message(mqttc,  obj, msg):
    received_time = timeit.default_timer()
    global _delay_time_dict
    payload = msg.payload
    timestamp, seq, value = struct.unpack("dId", payload)
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

    if len(sys.argv) != 6:
        print("Invalid argument")

    _PROCESS_COUNT = int(sys.argv[2])
    _THREAD_COUNT = int(sys.argv[3])
    _MACHINE_COUNT = int(sys.argv[1])
    _INTERVAL = int(sys.argv[4])
    _WAIT_TIME = 120
    _MONITOR_COUNT = 500
    _SEND_COUNT = 3
    _level = sys.argv[5]

    client = Client()
    client.on_message =on_message
    client.connect("mqtt.bucknell.edu")
    client.loop_start()
    choose_topic_to_subscribe(client, _PROCESS_COUNT, _MACHINE_COUNT,  _THREAD_COUNT, _MONITOR_COUNT, _INTERVAL)

    thread_list= []

    for i in range(_MACHINE_COUNT):
        t = threading.Thread(target = connect, args =(i + 1, _PROCESS_COUNT, _THREAD_COUNT, _ssh_list, _INTERVAL, _SEND_COUNT))
        t.start()
        thread_list.append(t)

    for t in thread_list:
        t.join()
    
    while len(_delay_time_dict) == 0:
        time.sleep(1)
    
    time.sleep(_INTERVAL * _SEND_COUNT * 2) 
    disconnect(_ssh_list)

    # calculate the average delay time
    delay, count = calculate_delay_time(_delay_time_dict)
    print("The average delay time is: {0:f}. The message collected is {1:d}".format(delay, count))
    data_points = []
    for key in _delay_time_dict:
        for x in _delay_time_dict[key]:
            data_points.append(x)
    data_points.sort()

    hmean = np.mean(data_points)
    hstd = np.std(data_points)
    pdf = stats.norm.pdf(data_points, hmean, hstd)
    plt.plot(data_points, pdf)
    plt.show()
    size = _INTERVAL * _PROCESS_COUNT * _THREAD_COUNT * _PROCESS_COUNT
    filename = "log/{0:d}-{1:d}-{2:d}-{3:d}-{4:d}-{5}".format(_MACHINE_COUNT, _PROCESS_COUNT, _THREAD_COUNT, _INTERVAL, int(time.time()), _level)
    with open(filename, 'w') as f:
        for data_point in data_points:
            f.write(str(data_point) + '\n')
