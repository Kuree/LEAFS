from paho.mqtt.client import Client
from paho.mqtt.publish import single
import json
import time
try:
    from core import msgEncode
    from core import ComputeCommand
    from client import QueryClient
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from core import msgEncode
    from client import QueryClient
    from core import ComputeCommand

class MultiSeriesAgent:
    _AGENT_TOPIC = "Multi/+/+"
    _TIME_OUT_TOPIC = "MultiTimeout"

    def __init__(self):
        self.client = Client()
        self.client.on_message = self.on_message
        self.query_client = QueryClient(api_key=1234)
        self.query_client.connect()
        self.query_client.on_message = self.on_client_message

        self._result_dict = {}
        self._request_id_table = {}

    def connect(self):
        self.client.connect("mqtt.bucknell.edu")
        self.client.loop_start()
        self.client.subscribe(MultiSeriesAgent._AGENT_TOPIC)

    def on_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")

        # TODO: ADD TIME OUT

        if len(topics) != 3:
            return
        tag = topics[0]
        request_id = topics[1] + "/" + topics[-1]
        if tag == "Multi":
            data = json.loads(msg.payload.decode())
            self.handle_multi_request(request_id, data)

    def handle_multi_request(self, request_id, data):
        type = data["type"]
        sensor_data = data["data"]
        function = data["function"]

        self._result_dict[request_id] = {"type" : type, "function" : function, "series_count": len(sensor_data), "data_points": []}

        if type == "stream":
            for sensor in sensor_data:
                topic = sensor[0]
                new_id = self.fnv32a(request_id + topic + str(time.time))
                self.query_client.add_stream(topic, None, True, new_id)
                self._request_id_table[new_id] = request_id
        elif type == "historical":
            for sensor in sensor_data:
                topic = sensor[0]
                db_tag = sensor[1]
                start = data["start"]
                end = data["end"]
                new_id = self.fnv32a(request_id + topic + str(time.time))
                self.query_client.add_query(db_tag, topic, start, end, query_id=new_id)
                self.query_client.add_stream(topic, None, True, new_id)
                self._request_id_table[new_id] = request_id

    def on_client_message(self, topic, payload):
        topics = topic.split("/")
        id = topics[-1]
        request_id = self._request_id_table[id]
        data_points = msgEncode.decode(payload)
        data_points_list = self._result_dict[request_id]["data_points"]
        data_points_list.append(data_points)
        if len(data_points_list) >= self._result_dict[request_id]["series_count"]:
            self.send_data(request_id)

    def send_data(self, request_id):
        data_points = self._result_dict[request_id]["data_points"]
        function = self._result_dict[request_id]["function"]
        # on flatten the list
        result_points = [item for sublist in data_points for item in sublist]
        result_points.sort()
        length = len(result_points)

        # 1000 data points after aggregation
        interval = 0
        if length > 1000:
            interval = (result_points[-1][0] - result_points[0][0]) // 1000
        else:
            interval = length

        compute = MultiSeriesAgent.convert_to_command(function, interval)

        single("Compute/" + request_id, msgEncode.encode(result_points, compute=compute), hostname="mqtt.bucknell.edu")

        # clear the old data
        self._result_dict[request_id]["data_points"] = []


    def convert_to_command(function, interval):
        func_command = 0
        if function == "avg":
            function_command = ComputeCommand.AVERAGE
        elif function == "sum":
            function_command = ComputeCommand.SUM
        elif function == "max":
            function_command = ComputeCommand.MAX
        elif function == "min":
            function_command = ComputeCommand.MIN
        elif function == "dev":
            function_command = ComputeCommand.DEV
        elif function == "count":
            function_command = ComputeCommand.COUNT

        compute = ComputeCommand()
        compute.add_compute_command(func_command, interval)
        return compute.to_obj()


    def fnv32a(self, string):
        hval = 0x811c9dc5
        fnv_32_prime = 0x01000193
        uint32_max = 2 ** 32
        for s in string:
            hval = hval ^ ord(s)
            hval = (hval * fnv_32_prime) % uint32_max
        return str(hval)

if __name__ == "__main__":
    a = MultiSeriesAgent()
    a.connect()
    while True:
        time.sleep(10)