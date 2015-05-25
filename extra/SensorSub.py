import paho.mqtt.client as mqtt
from SqlHelper import putData
import json

def on_connect(mqttc, obj, flags, rc):
    print("rc : " + str(rc))

def on_message(mqttc, obj, msg):
    #print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    data = json.loads(msg.payload.decode())
    putData(msg.topic, data["Timestamp"], float(data["Value"]))

def on_publish(mqttc, obj, mid):
    print("mid :" + str(mid))

def on_subscribe(mqttc, obj, mid, granted_qos):
    print("subscribed " + str(mid) + " " + str(granted_qos))

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

mqttc.connect("mqtt.bucknell.edu", 1883, 60)
mqttc.subscribe("PA/#", 0)

mqttc.loop_forever()
