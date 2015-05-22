import paho.mqtt.publish as publish
from client.QueryClient import QueryClient
import time
import threading
from core import ComputeCommand
from core import msgEncode

sending_time = 0
count = 0


def push_data_forever(maximum):
    global sending_time
    count = 0
    send_start_time = time.time()
    while count < maximum:
        obj = (count, count, count)
        publish.single("test/test/stream", msgEncode.encode(obj), hostname="mqtt.bucknell.edu")
        count += 1
        time.sleep(0.5)
    send_end_time = time.time()
    sending_time = send_end_time - send_start_time



if __name__ == '__main__':
    maximum = 1000
    t = threading.Thread(target=push_data_forever, args=(maximum, ))
    q = QueryClient("benchmark_stream")

    count = 0
    re_end_time = 0
    re_start_time = 0
    compute = ComputeCommand()
    interval = 2
    compute.add_compute_command(ComputeCommand.AVERAGE, interval)
    q.add_stream("SQL", "test/test/stream", compute.to_obj())
    def on_message(topic, msg):
        global count, maximum, re_end_time, re_start_time
        if count == 0:
            re_start_time = time.time()
        count += 1
        if count < maximum:
            re_end_time = time.time()
    q.on_message = on_message
    q.connect()
    t.start()
    while re_end_time == 0:
        time.sleep(1)
    receive_time = re_end_time - re_start_time
    print("Sending time: {0}. Receiving time: [1}. Total delay: {2}. Delay time per message: {3}".format(
        sending_time, receive_time, receive_time - sending_time, (receive_time - sending_time) / maximum
    ))
