from paho.mqtt.client import Client
from paho.mqtt.publish import single
import json
import time
import random
try:
    from client import QueryClient
    from core import msgEncode
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from client import QueryClient
    from core import msgEncode

QUERY_PROCESS_TOPIC = "Process/+/+"

# TODO: change this to RDF linked data search
AVERAGE_LIST = ["average", "mean", "avg"]
MAX_LIST = ["max", "maximum"]
MIN_LIST = ["min", "minimum"]
FUNCTION_DICT = {}
for x in AVERAGE_LIST:
    FUNCTION_DICT[x] = "avg"
for x in MIN_LIST:
    FUNCTION_DICT[x] = "min"
for x in MAX_LIST:
    FUNCTION_DICT[x] = "max"


# TODO: change this to RDF linked data search (SPARQL)
TEMPERATURE_LIST = ["temperature", "t", "temp"]
PARAM_DICT = {}
for x in TEMPERATURE_LIST:
    PARAM_DICT[x] = "temp"

queryClient = QueryClient(api_key=1234)
queryClient.connect()

def on_message(mqttc, obj, msg):
    global queryClient
    topics = msg.topic.split("/")
    request_id = topics[-2] + "/" + topics[-1]
    request_data = json.loads(msg.payload.decode())
    print(request_data)
    #if "LOCATION" not in request_data:
    #    # TODO: return error message to result topic
    #    return
    param_raw = request_data["PARAM"] if "PARAM" in request_data else "temperature"
    time_raw = request_data["DATE"] if "DATE" in request_data else "now"
    func_raw = request_data["FUNC"] if "FUNC" in request_data else "average"

    param = PARAM_DICT[param_raw]
    func = FUNCTION_DICT[func_raw]

    single("ProcessResult/" + request_id, payload=0, hostname="mqtt.bucknell.edu")
    queryClient.add_stream("test/test/1", None, False, topics[-1])

    

if __name__ == "__main__":
    c = Client()
    c.on_message = on_message
    c.connect("mqtt.bucknell.edu")
    c.subscribe(QUERY_PROCESS_TOPIC)
    c.loop_start()

    # send testing message
    count = 0
    while True:
        time.sleep(0.1)
        # this is for test purpose
        # TODO: clean this up after testing
        single("test/test/1", msgEncode.encode((time.time(), count, random.uniform(0.0, 10.0))), hostname = "mqtt.bucknell.edu")
