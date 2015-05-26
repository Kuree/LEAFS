import paho.mqtt.publish as publish
from paho.mqtt.client import Client
from client.QueryClient import QueryClient
import time
import threading
from core import ComputeCommand
from core import msgEncode
import csv, json

sending_time = 0
count = 0

class benchmark_stream:

    def __init__(self, name, interval, sensor_type):

        self.sensor_type = sensor_type


        self.interval = interval
        self.name = name

        # hold the file in memory
        self.data_points = []
        with open("water_level.csv", 'r') as f:
            reader = csv.reader(f)
            count = -1
            for row in reader:
                if len(row) == 0:
                    continue
                if count > 100000:
                    break

                if count >= 0:
                    self.data_points.append((time.time(), count, float(row[1])))
                    time.sleep(0.0001)
                count += 1

        self.min_rec_time = 0
        self.max_rec_time = 0

        self.min_send_time = 0
        self.max_send_time = 0

        self._benchmark_sub = Client()
        self._benchmark_sub.on_message = self._benchmark_send_message
        self._benchmark_sub.connect("mqtt.bucknell.edu")
        self._benchmark_sub.subscribe("benchmark/request")
        self._benchmark_sub.loop_start()

    def push_data(self):
        self._update_sending_time(time.time())
        self._update_receive_time(time.time())
        for data_point in self.data_points:
            publish.single("test/test/stream" + str(self.name), payload=msgEncode.encode(data_point), hostname="mqtt.bucknell.edu",)
        self._update_sending_time(time.time())

    def run(self):
        t = threading.Thread(target=self.push_data)
        q = QueryClient("benchmark_stream" + str(self.name))
        compute = ComputeCommand()
        compute.add_compute_command(ComputeCommand.AVERAGE, self.interval)
        q.add_stream("SQL", "test/test/stream" + str(self.name), compute.to_obj())
        q.on_message = self.on_message

        q.connect()
        time.sleep(0.2) # wait till the connection is ready
        t.start()
        

    def on_message(self, topic, msg):
        self._update_receive_time(time.time())

    def _benchmark_send_message(self, mqttc, obj, message):
        publish.single("benchmark/reply/benchmark", payload = json.dumps({"min" : self.min_rec_time, "max": self.max_rec_time}), hostname= "mqtt.bucknell.edu")
        publish.single("benchmark/reply/send", payload = json.dumps({"min" : self.min_send_time, "max": self.max_send_time}), hostname= "mqtt.bucknell.edu")
        
    def _update_receive_time(self, data_time):
        if self.min_rec_time == 0:
            self.min_rec_time = data_time
        else:
            self.max_rec_time = data_time

    def _update_sending_time(self, data_time):
        if self.min_send_time == 0:
            self.min_send_time = data_time
        else:
            self.max_send_time = data_time
