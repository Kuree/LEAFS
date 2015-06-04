import os, sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from paho.mqtt.publish import single
import random
import struct
import timeit
import threading
import time
from core import msgEncode

def send_message(client_id, interval):
    time.sleep(random.uniform(0, interval))  #sleep some random time
    i = 0
    while True:
        for j in range(interval):
            #start = timeit.default_timer()
            single("benchmark/" + str(client_id + j), payload=msgEncode.encode(( timeit.default_timer(), i, timeit.default_timer())), hostname = "134.82.56.50")
            i += 1
            time.sleep(1 / interval)# -  (timeit.default_timer()- start))


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(sys.argv)
        print("Invalid argument")
        exit(1)
    base_number = int(sys.argv[1])
    thread_count =  int(sys.argv[2])
    interval = int(sys.argv[3])
    thread_list = []
    for i in range(thread_count):
        t = threading.Thread(target = send_message, args =(base_number + i * interval, interval,))
        t.start()
        thread_list.append(t)

    for t in thread_list:
        t.join()
