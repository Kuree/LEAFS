from paho.mqtt.client import Client
import paho.mqtt.publish as publish
import threading, math
import json, time, logging
from LoggingHelper import log
from functools import reduce


class ComputeFunction:
    # NOTICE:
    # Currently the first time stamp of a chunk is used to represent the entire chunk
    # Need to find a better way to do it

    # NOTICE:
    # This entire computation is map-reduce pattern.
    # It is very reasonable to use streaming map-reduce in the future
    @staticmethod
    def avg(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b: a + b, [x[1] for x in tuples]) / len(tuples)
        return [[x[0][0], compute(x)] for x in chunks]

    @staticmethod
    def max(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a if a > b else b, [x[1] for x in tuples])
        return [[x[0][0], compute(x)] for x in chunks]

    @staticmethod
    def min(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a if a < b else b, [x[1] for x in tuples])
        return [[x[0][0], compute(x)] for x in chunks]

    @staticmethod
    def sum(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a + b, [x[1] for x in tuples])
        return [[x[0][0], compute(x)] for x in chunks]

    @staticmethod
    def dev(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: 
                avg = reduce(lambda a, b : a + b, [x[1] for x in tuples]) / len(tuples)
                return math.sqrt(reduce(lambda a, b : a + b, [math.pow(x[1] - avg, 2) for x in tuples]))  # compute the standard deviation
        return [[x[0][0], compute(x)] for x in chunks]

    @staticmethod
    def filter(data, min = None, max = None, interval = 1):
        # TODO: currently not supported. Need to find a good way to implement various argument length
        return [0]

class ComputeAgent:

    _COMPUTE_REQUEST_TOPIC_STRING = "+/Query/Compute/+/+"
    _QUERY_RESULT_TOPIC_STRING = "/Query/Result/"
    _HOSTNAME = "mqtt.bucknell.edu"
    COMPUTE_FUNCTION = {"avg" : ComputeFunction.avg, "max" : ComputeFunction.max,"min" :ComputeFunction.sum,"dev" : ComputeFunction.dev}

    def __init__(self, block_current_thread = False):
       self._compute_request_sub = Client()
       self._compute_request_sub.on_message = self._on_compute_message
       self._compute_request_sub.connect(ComputeAgent._HOSTNAME)
       self._compute_request_sub.subscribe(ComputeAgent._COMPUTE_REQUEST_TOPIC_STRING, 0)
       

       if block_current_thread:
           self._compute_request_sub.loop_forever()
       else:
           self._compute_request_sub.loop_start()

    def _on_compute_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        # TODO: need to switch to log
        assert len(topics) == 5, "A standard request should have 5 levels"
        db_tag = topics[0]

        message = json.loads(msg.payload.decode())
        data = message["data"]
        commands = message["compute"]

        if not ComputeAgent.should_return(commands):
            command = commands[0]
            command_name = command["name"]
            if command_name in ComputeAgent.COMPUTE_FUNCTION:
                # perform computation
                if command_name != "filter":
                    arg = command["arg"]
                    if len(arg) == 0:
                        message["data"] = ComputeAgent.COMPUTE_FUNCTION[command_name](data)
                    else:
                        interval = int(arg[0])
                        message["data"] = ComputeAgent.COMPUTE_FUNCTION[command_name](data, interval)

            commands.remove(command)
            message["compute"] = commands

            # MAY CHANGE IN THE FUTURE
            self.send_to_next(msg.topic, json.dumps(message))
        else:
            request_id = topics[-2] + "/" +  topics[-1]
            publish.single(db_tag + ComputeAgent._QUERY_RESULT_TOPIC_STRING + request_id, json.dumps(data), hostname = ComputeAgent._HOSTNAME)
            return

    def send_to_next(self, topic, message):
        publish.single(topic, message, hostname=ComputeAgent._HOSTNAME)

    @staticmethod
    def should_return(commands):
        for command in commands:
            if command["name"] in ComputeAgent.COMPUTE_FUNCTION:
                return False
        return True



    @staticmethod
    def split_into_chunk(data, interval):
        result = []
        chunk = []
        pre_timestamp = data[0][0] # the first time stamp in a chunk
        for data_point in data:
            timestamp = data_point[0]
            value = data_point[1]
            if timestamp < pre_timestamp + interval:
                chunk.append(data_point)
            else:
                result.append(chunk)
                chunk = [data_point]
                pre_timestamp = timestamp
        result.append(chunk)
        return result

if __name__ == "__main__":
    sys = ComputeAgent(True)