import paho.mqtt.publish as publish
import json

class logger:
    _HOSTNAME = "mqtt.bucknell.edu"

    @staticmethod
    def log(level, message):
        data = [level, json.dumps(message)]
        publish.single("Query/Log", json.dumps(data), hostname=logger._HOSTNAME)