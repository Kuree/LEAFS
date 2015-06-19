from paho.mqtt.client import Client
from paho.mqtt.publish import single
import json
import time
import random
import requests
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


def find_location_ip(ip):
    result = requests.get("http://freegeoip.net/json/" + ip).text
    json_result = json.loads(result)
    if "latitude" in json_result:
        lat = json_result["latitude"]
        lon = json_result["longitude"]
        location_name = json_result["city"]
    else:
        lat = 0
        lon = 0
        location_name = "Unknown"
    return lat, lon, location_name

def on_message(mqttc, obj, msg):
    global queryClient
    topics = msg.topic.split("/")
    request_id = topics[-2] + "/" + topics[-1]
    request_data = json.loads(msg.payload.decode())
    print(request_data)
    lat, lon = 0.0, 0.0
    has_match = False

    location_name = ""

    if "LOCATION" not in request_data:
        ip = request_data["ip"]
        lat, lon, location_name = find_location_ip(ip)
    else:
        result = requests.get("http://api.geonames.org/searchJSON?q=" + request_data["LOCATION"] + "&fuzzy=0.8&username=kz005").text
        json_result = json.loads(result)
        if json_result["totalResultsCount"] != 0:
            lat = json_result["geonames"][0]["lat"]
            lon = json_result["geonames"][0]["lng"]
            location_name = json_result["geonames"][0]["name"]
        else:
            lat, lon, location_name = find_location_ip(ip)
    
    param_raw = request_data["PARAM"] if "PARAM" in request_data else "temperature"
    time_raw = request_data["DATE"] if "DATE" in request_data else "now"
    func_raw = request_data["FUNC"] if "FUNC" in request_data else "average"

    param = PARAM_DICT[param_raw]
    func = FUNCTION_DICT[func_raw]

    if not has_match:
        print("no match for", request_data)

    process_result = {"has_match" : has_match, "keyword" : [location_name]}

    single("ProcessResult/" + request_id, payload=json.dumps(process_result), hostname="mqtt.bucknell.edu")
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
