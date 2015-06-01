from paho.mqtt.publish import single
import random
import struct
import timeit
import sys
import threading
import time

def send_message(client_id, interval, send_count):
    time.sleep(random.uniform(0, interval))  #sleep some random time
    for i in range(send_count):
        for j in range(interval):
            #start = timeit.default_timer()
            single("benchmark/" + str(client_id + j), payload=bytearray(struct.pack("dId", timeit.default_timer(), i, timeit.default_timer())), hostname = "mqtt.bucknell.edu")
            time.sleep(1 / interval)# -  (timeit.default_timer()- start))


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(sys.argv)
        print("Invalid argument")
        exit(1)
    base_number = int(sys.argv[1])
    thread_count =  int(sys.argv[2])
    interval = int(sys.argv[3])
    send_count = int(sys.argv[4])
    thread_list = []
    for i in range(thread_count):
        t = threading.Thread(target = send_message, args =(base_number + i * interval, interval, send_count,))
        t.start()
        thread_list.append(t)

    for t in thread_list:
        t.join()
