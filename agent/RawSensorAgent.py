from paho.mqtt.client import Client
import sqlite3
import json
import time

#try:
#    from meta import Sensor
#except:
#    import os, sys
#    sys.path.insert(1, os.path.join(sys.path[0], '..'))
#    from meta import Sensor

class RawSensorAgent:
    
    SENSOR_DISCOVERY_TOPIC = "RawSensor/+/+"
    USER_ID_LIST = ["1234"]

    def __init__(self):
        self.client = Client()
        self.client.on_message = self.on_message

        self.con = sqlite3.connect("meta/sensor_meta.db", check_same_thread=False)
        self.cur = self.con.cursor()


    def on_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        if topics[1] not in RawSensorAgent.USER_ID_LIST:
            return

        sensor_data = json.loads(msg.payload.decode())

        self.add_sensor_data(sensor_data)


    def initialize_database(self):
        self.cur.execute('CREATE TABLE IF NOT EXISTS meta (type text, unit text)')
        self.cur.execute('CREATE TABLE IF NOT EXISTS sensor (sensor_tag text PRIMARY KEY, type REFERENCES meta(type), \
        topic text, unit REFERENCES meta(unit), lat REAL, lon REAL, db_tag text)')
        self.con.commit()
        cf_names = self.get_cf_names()
        for cf_name in cf_names:
            self.cur.execute('INSERT INTO meta VALUES (?,?)', cf_name)
        self.con.commit()


    def add_sensor_data(self, sensor_data):
        sensor_tag = sensor_data["tag"]
        sensor_type = sensor_data["type"]
        sensor_unit = sensor_data["unit"]
        sensor_topic = sensor_data['topic']
        sensor_lat = sensor_data["lat"]
        sensor_lon = sensor_data["lon"]
        db_tag = sensor_data['db_tag']

        try:
            self.cur.execute('INSERT INTO sensor VALUES (?, ?, ?, ?, ?, ?, ?)', (sensor_tag, sensor_type, sensor_topic, sensor_unit, sensor_lat, sensor_lon, db_tag,))
            self.con.commit() 
        except:
            return

    def connect(self):
        self.client.connect("mqtt.bucknell.edu")
        self.client.subscribe(RawSensorAgent.SENSOR_DISCOVERY_TOPIC)
        self.client.loop_start()

if __name__ == "__main__":
    a = RawSensorAgent()
    a.connect()
    while True:
        time.sleep(10)