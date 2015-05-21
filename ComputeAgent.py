from paho.mqtt.client import Client
import paho.mqtt.publish as publish
import threading, math
import json, time, logging
from LoggingHelper import logger
from functools import reduce
from QueryObject import ComputeCommand
from msgEncode import msgEncode


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
            else: return reduce(lambda a, b: a + b, [x[2] for x in tuples]) / len(tuples)
        return [[x[0][0], x[0][1], compute(x)] for x in chunks]

    @staticmethod
    def max(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a if a > b else b, [x[2] for x in tuples])
        return [[x[0][0], x[0][1], compute(x)] for x in chunks]

    @staticmethod
    def min(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a if a < b else b, [x[2] for x in tuples])
        return [[x[0][0], x[0][1], compute(x)] for x in chunks]

    @staticmethod
    def sum(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: return reduce(lambda a, b : a + b, [x[2] for x in tuples])
        return [[x[0][0], x[0][1], compute(x)] for x in chunks]

    @staticmethod
    def dev(data, interval = 1):
        chunks = ComputeAgent.split_into_chunk(data, interval)
        def compute(tuples):
            if len(tuples) == 0: return 0
            else: 
                avg = reduce(lambda a, b : a + b, [x[2] for x in tuples]) / len(tuples)
                return math.sqrt(reduce(lambda a, b : a + b, [math.pow(x[2] - avg, 2) for x in tuples]))  # compute the standard deviation
        return [[x[0][0], x[0][1], compute(x)] for x in chunks]

class ComputeAgent:

    _COMPUTE_REQUEST_TOPIC_STRING = "+/Query/Compute/+/+"
    _QUERY_RESULT_TOPIC_STRING = "/Query/Result/"
    _HOSTNAME = "mqtt.bucknell.edu"
    COMPUTE_FUNCTION = {ComputeCommand.AVERAGE : ComputeFunction.avg, ComputeCommand.MAX : ComputeFunction.max, ComputeCommand.MIN :ComputeFunction.sum,ComputeCommand.DEV : ComputeFunction.dev}

    def __init__(self, block_current_thread = False):
       self._compute_request_sub = Client()
       self._compute_request_sub.on_message = self._on_compute_message
       self._compute_request_sub.connect(ComputeAgent._HOSTNAME)
       self._compute_request_sub.subscribe(ComputeAgent._COMPUTE_REQUEST_TOPIC_STRING, 0)

       self.block_current_thread = block_current_thread
       

    def _on_compute_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        if len(topics) != 5: logger.log(logging.WARN, "A standard request should have 5 levels")
        db_tag = topics[0]

        message = msgEncode.decode(msg.payload)
        data = message[0]
        commands = message[1]

        while not ComputeAgent.should_return(commands):
            # loop till the task is finished
            command = commands[0]
            command_name = command[0]
            if command_name in ComputeAgent.COMPUTE_FUNCTION:
                # perform computation
                arg = command[1]
                message[0] = ComputeAgent.COMPUTE_FUNCTION[command_name](data, arg)

            commands.remove(command)

        
        request_id = topics[-2] + "/" +  topics[-1]
        publish.single(db_tag + ComputeAgent._QUERY_RESULT_TOPIC_STRING + request_id, msgEncode.encode(message[0]), hostname = ComputeAgent._HOSTNAME)
        return


    @staticmethod
    def should_return(commands):
        for command in commands:
            if command[0] in ComputeAgent.COMPUTE_FUNCTION:
                return False
        return True


    def connect(self):
       if self.block_current_thread:
           self._compute_request_sub.loop_forever()
       else:
           self._compute_request_sub.loop_start()



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
    sys.connect()