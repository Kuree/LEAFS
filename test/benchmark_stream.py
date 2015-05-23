import paho.mqtt.publish as publish
from client.QueryClient import QueryClient
import time
import threading
from core import ComputeCommand
from core import msgEncode

sending_time = 0
count = 0

class benchmark_stream:

    def __init__(self, name):
        self.sending_time = 0
        self.send_end_time = 0
        self.send_start_time = 0

        self.maximum = 100
        self.re_start_time = 0
        self.re_end_time = 0
        self.receive_time = 0

        self.count = 0
        self.interval = 50
        self.name = name
       

    def push_data_forever(self, maximum):
        count = 0
        self.send_start_time = time.time()
        while count < maximum + self.interval * 10:  # add extra in case there's buffering. timing only counts for the first maximum number of data points
            obj = (count, count, count)
            publish.single("test/test/stream" + str(self.name), msgEncode.encode(obj), hostname="mqtt.bucknell.edu")
            count += 1
            if count == maximum:
                self.send_end_time = time.time()
                self.sending_time = self.send_end_time - self.send_start_time

    def run(self):
        t = threading.Thread(target=self.push_data_forever, args=(self.maximum, ))
        q = QueryClient("benchmark_stream" + str(self.name))
        compute = ComputeCommand()
        compute.add_compute_command(ComputeCommand.AVERAGE, self.interval)
        q.add_stream("SQL", "test/test/stream" + str(self.name), compute.to_obj())
        q.on_message = self.on_message

        q.connect()
        time.sleep(0.2) # wait till the connection is ready
        t.start()
        self.re_start_time = time.time()

        while self.re_end_time == 0:
            time.sleep(1)
        self.receive_time = self.re_end_time - self.re_start_time
        print("Sending time: {0}. Receiving time: {1}. Total delay: {2}. Delay time per message: {3}".format(
            self.sending_time, self.receive_time, self.receive_time - self.sending_time, (self.receive_time - self.sending_time) / self.maximum
        ))

        return self.sending_time, self.receive_time

    def on_message(self, topic, msg):
        self.count += 1
        
        if self.count == self.maximum / self.interval:
            self.re_end_time = time.time()
