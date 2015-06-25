from paho.mqtt.client import Client
from paho.mqtt.publish import single
import json
import time
import random
import requests
import sqlite3
import urllib
from geopy.distance import vincenty
from lxml import etree
try:
    from client import QueryClient
    from core import msgEncode
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from client import QueryClient
    from core import msgEncode


class QueryProcessAgent:

    QUERY_PROCESS_TOPIC = "Process/+/+"

    # TODO: change this to RDF linked data search
    AVERAGE_LIST = ["average", "mean", "avg"]
    MAX_LIST = ["max", "maximum"]
    MIN_LIST = ["min", "minimum"]
    FUNCTION_DICT = {}
    THRESHOLD =  20

    def __init__(self):
        for x in QueryProcessAgent.AVERAGE_LIST:
            QueryProcessAgent.FUNCTION_DICT[x] = "avg"
        for x in QueryProcessAgent.MIN_LIST:
            QueryProcessAgent.FUNCTION_DICT[x] = "min"
        for x in QueryProcessAgent.MAX_LIST:
            QueryProcessAgent.FUNCTION_DICT[x] = "max"


        self.con = sqlite3.connect("meta/sensor_meta.db", check_same_thread=False)
        # add compute distance function to sqlite
        self.con.create_function("IS_MATCH", 4, QueryProcessAgent.is_distance_match)
        self.con.commit()
        self.cur = self.con.cursor()



        self.client = Client()
        self.client.on_message = self.on_message

    def is_distance_match(lat1, lon1, lat2, lon2):
        return 1 if vincenty((lat1, lon1), (lat2, lon2)).km <= QueryProcessAgent.THRESHOLD else 0


    def connect(self):
        self.client.connect("mqtt.bucknell.edu")
        self.client.subscribe(QueryProcessAgent.QUERY_PROCESS_TOPIC)
        self.client.loop_start()

    def initialize_database(self):
        self.cur.execute('CREATE TABLE IF NOT EXISTS meta (type text, unit text)')
        self.cur.execute('CREATE TABLE IF NOT EXISTS sensor (sensor_tag text PRIMARY KEY, type REFERENCES meta(type), \
        topic text, unit REFERENCES meta(unit), lat REAL, lon REAL, db_tag text)')
        self.con.commit()
        cf_names = self.get_cf_names()
        for cf_name in cf_names:
            self.cur.execute('INSERT INTO meta VALUES (?,?)', cf_name)
        self.con.commit()

    def get_cf_names(self):
        tree = etree.parse('http://cfconventions.org/Data/cf-standard-names/28/src/cf-standard-name-table.xml')
        result = []
        for entry in tree.xpath('entry'):
            standard_name = entry.attrib['id']
            for c_unit in entry.xpath('canonical_units'):
                unit = c_unit.text
                result.append((standard_name, unit))
        return result

    def get_lat_lon(self, location):
        result = requests.get("http://api.geonames.org/searchJSON?q=" + location + "&fuzzy=0.8&username=kz005").text
        json_result = json.loads(result)
        if json_result["totalResultsCount"] != 0:
            lat = json_result["geonames"][0]["lat"]
            lon = json_result["geonames"][0]["lng"]
            location_name = json_result["geonames"][0]["name"]
        else:
            lat, lon, location_name = self.find_location_ip(ip)

        return lat, lon

    def find_location_ip(self, ip):
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

    def on_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        request_id = topics[-2] + "/" + topics[-1]
        request_data = json.loads(msg.payload.decode())
        print(request_data)
        lat, lon = 0.0, 0.0

        location_name = ""

        if "LOCATION" not in request_data:
            ip = request_data["ip"]
            lat, lon, location_name = self.find_location_ip(ip)
        else:
            lat, lon = self.get_lat_lon(request_data["LOCATION"])
    
        param = request_data["PARAM"] if "PARAM" in request_data else "temperature"
        start_time_raw = request_data["START_TIME"]
        end_time_raw = request_data["END_TIME"]

        has_historical_query = False
        has_streaming_query = False

        if start_time_raw == "PRESENT_REF":
            has_streaming_query = True
        else:
            has_historical_query = True
            has_streaming_query = end_time_raw == "PRESENT_REF"

        func_raw = request_data["FUNC"] if "FUNC" in request_data else "average"

        func = QueryProcessAgent.FUNCTION_DICT[func_raw]

        has_match = False
        param = param.replace(" ", "_")
        self.cur.execute("SELECT * FROM sensor WHERE type = ? AND IS_MATCH(lat, lon, ?, ?) = 1", (param, lat, lon,))

        search_results =  list(self.cur.fetchall())   
        
        if len(search_results) == 0:
            has_match = False
            print("no match for", request_data)
        else:
            has_match = True

        process_result = {"has_match" : has_match, "location" : [location_name.split(",")], "result": []}
        for sensor in search_results:
            process_result["result"].append((sensor[-3], sensor[-2], sensor[0]))
        single("ProcessResult/" + request_id, payload=json.dumps(process_result), hostname="mqtt.bucknell.edu")

        
        self.handle_request(request_id, search_results, start_time_raw, end_time_raw, has_streaming_query, has_historical_query, func)
        # self.queryClient.add_stream(search_results[0][2], None, False, topics[-1])

    def handle_request(self, request_id, sensors, start, end, has_streaming, has_historical, function):
        stream_request = []

        for sensor in sensors:
            topic = sensor[2]
            db_tag = sensor[-1]
            stream_request.append((topic, db_tag))
        request = { "data" : stream_request, "function" : function}
        
        if has_streaming:
            request["type"] = "stream"
            single("Multi/" + request_id, json.dumps(request), hostname="mqtt.bucknell.edu")

        if has_historical:
            start_time = int(start)
            end_time = int(time.time() * 1000) if end_time_raw == "PRESENT_REF" else int(end)
            request["type"] = "historical"
            request["start"] = start_time
            request["end"] = end_time
            single("Multi/" + request_id, json.dumps(request), hostname="mqtt.bucknell.edu")


if __name__ == "__main__":
    q = QueryProcessAgent()
    #q.initialize_database()
    q.connect()
    # send testing message
    count = 0
    while True:
        time.sleep(0.5)
        # this is for test purpose
        # TODO: clean this up after testing
        sample = random.uniform(0.0, 10.0)
        single("test/test/1", msgEncode.encode((time.time(), count, sample)), hostname = "mqtt.bucknell.edu")
        single("test/test/2", msgEncode.encode((time.time(), count, 10 - sample)), hostname = "mqtt.bucknell.edu")
        count += 1
