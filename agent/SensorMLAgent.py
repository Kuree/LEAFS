from paho.mqtt.client import Client
try:
    from SensorML import SensorML
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from meta import SensorML

class SensorMLAgent:
    
    def __init__(self):
        self.client = Client()
        self.sensorml = SensorML()
        self.client.on_message = self.on_message

    def connect(self):
        self.client.connect("mqtt.bucknell.edu")
        self.client.subscribe("SensorML/+/+")
        self.client.loop_start()

    def on_message(self, mqttc, obj, msg):
        sensorML = msg.payload.decode()
        self.sensorml.add_sensorml(sensorML)

if __name__ == "__main__":
    a = SensorMLAgent()
    a.connect()