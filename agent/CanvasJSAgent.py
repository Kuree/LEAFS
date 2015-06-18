from paho.mqtt.client import Client
from paho.mqtt.publish import single
import json
try:
    from core import msgEncode
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from core import msgEncode

# for testing
# TODO: need to move it to config file
ALLOWED_CLIENT = ["1234", "123456"]
AGENT_TAG = "CanvasJS"


if __name__ == "__main__":
    def on_message(mqttc, obj, msg):
        global AGENT_TAG
        topics = msg.topic.split("/")
        request_id = topics[-2] + "/" + topics[-1]
        data_points = msgEncode.decode(msg.payload)
        # transform into the format that's readable to the server
        # note it is not exactly the same as the format that's used in canvasJS
        # may consider to change the format to CanvasJS' format
        result = [(int(x[0] * 1000), x[-1]) for x in data_points]
        single(AGENT_TAG + "/" + request_id, json.dumps(result), hostname="mqtt.bucknell.edu")
    c = Client()
    c.connect("mqtt.bucknell.edu")
    for topic in ALLOWED_CLIENT:
        c.subscribe("Result/" + topic + "/+")
    c.on_message = on_message
    c.loop_forever();