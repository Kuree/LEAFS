from test.benchmark_stream import benchmark_stream
import time
from agent import WindowAgent, ComputeAgent
from paho.mqtt.publish import single
from paho.mqtt.client import Client
import json


if __name__ == "__main__":
    #w = WindowAgent(is_benchmark = True)
    #c = ComputeAgent(is_benchmark = True)
    b = benchmark_stream("test", 0.005, "water")

    should_stop = False
    stop_control = {"window" : [0, 0, False] , "compute": [0, 0, False], "benchmark" : [0, 0, False], "send" : [0, 0, False]}

    def on_message(mqttc, obj, msg):
        global should_stop, stop_control
        topic = msg.topic
        raw_data = json.loads(msg.payload.decode())
        max_time = raw_data["max"]
        min_time = raw_data["min"]
        type = topic.split("/")[-1]
        if max_time == 0:
            return
        if stop_control[type][0] != max_time:
            stop_control[type][0] = max_time
        else:
            stop_control[type][-1] = True
            stop_control[type][1] = min_time

        result = True
        for key in stop_control:
            result = result and stop_control[key][-1]

        should_stop = result


    benchmark_sub = Client()
    benchmark_sub.connect("mqtt.bucknell.edu")
    benchmark_sub.subscribe("benchmark/reply/+")
    benchmark_sub.on_message = on_message
    benchmark_sub.loop_start()

    #w.connect()
    #c.connect()
    b.run()

    

    while not should_stop:
        single("benchmark/request", payload = 0, hostname="mqtt.bucknell.edu")
        time.sleep(5) # long sleep

    sending_time = stop_control["send"][0] - stop_control["send"][1]
    total_delay = stop_control["benchmark"][0] - stop_control["benchmark"][1] - sending_time
    compute_delay = stop_control["compute"][0] - stop_control["compute"][1] - sending_time
    window_delay = stop_control["window"][0] - stop_control["window"][1] - sending_time
    broker_dealy = total_delay - window_delay - compute_delay

    print("total delay: ", total_delay, "window delay: ", window_delay, "compute delay: ", compute_delay,
          "broker dealy: ", broker_dealy)



    

