from paho.mqtt.client import Client
import time
import threading
import pymysql.cursors
import struct


if __name__ == "__main__":
    data_points = []
    should_push = False
    last_ts = time.time()
    client = Client()
    conn = pymysql.connect(host="db.eg.bucknell.edu", user="mqtt", passwd = "timahb4eiquo", db = "mqtt")
    cur = conn.cursor()
    def on_message(mqttc, obj, msg):
        global cur, conn, last_ts
        topic = int(msg.topic.split("/")[-1])
        ts, s_n, value = struct.unpack("dId", msg.payload)
        data_points.append((ts, s_n, value))
        if value - last_ts > 1:
            for data_point in data_points:
                sql = "INSERT INTO benchmark (timestamp, sequence_number, value) VALUES (%s, %s, %s)"
                cur.execute(sql, (ts, s_n, value, ))
            conn.commit()
            del data_points[:]
        last_ts = value
    client.on_message = on_message
    client.connect("mqtt.bucknell.edu")
    client.subscribe("benchmark/+")
    client.loop_forever()

