import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from client import QueryClient
import time
import random
from core import ComputeCommand
from core import msgEncode
import threading

MONITOR_COUNT = 300
INTERVAL = 60

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



if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Invalid argument")
        exit(1)

    _MACHINE_COUNT = int(sys.argv[1])
    _PROCESS_COUNT = int(sys.argv[2])
    _THREAD_COUNT = int(sys.argv[3])
    _FREQUENCY = int(sys.argv[4])
    _TOTAL_TIME = int(sys.argv[5])


    total_id = _MACHINE_COUNT * _PROCESS_COUNT * _THREAD_COUNT * _FREQUENCY
    ssh_list = []
    thread_list = []
    for i in range(_MACHINE_COUNT):
        t = threading.Thread(targec=connect, args=(i+1, _Process_COUNT, _THREAD_COUNT, ssh_list, _FREQUENCY))
        t.start()
        thread_list.append(t)

    for t in thread_list:
        t.join()



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

    time.sleep(_TOTAL_TIME)

    # need to dump the data points here
