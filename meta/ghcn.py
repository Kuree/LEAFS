import urllib2
import json
from paho.mqtt.publish import single, multiple
import time
import sqlite3
#import ulmo
#import pandas

if __name__ == "__main__":

    req = urllib2.Request('ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt')
    response = urllib2.urlopen(req)
    data = response.read()
    db_tag = "GHCN"
    data_list = []

    con = sqlite3.connect("meta/sensor_meta.db", check_same_thread=False)
    cur = con.cursor()

    for data in filter(None, data.split("\n")):
        data = filter(None, data.split(" "))
        station_name = data[0]
        lat = float(data[1])
        lon = float(data[2])

        # right now only deal with max temperature
        # assume that it always exits
        # TODO: FIX NEED TO CHECK THE EXISTENCE
        sensor_tag = station_name + "_TEMP"

        ## check if the station has max temp

        #data = ulmo.ncdc.ghcn_daily.get_data(station_name, as_dataframe=True)

        #if data['TMAX'] is None:
        #    print("something goes wrong", station_name)
        sensor_data = {"tag" : sensor_tag, "lat" : lat, "lon" : lon, "topic" : sensor_tag, "type" : "air_temperature", "unit" : "C", "db_tag" : db_tag}
        data_list.append(sensor_data)

    for sensor_data in data_list:
        sensor_tag = sensor_data["tag"]
        sensor_type = sensor_data["type"]
        sensor_unit = sensor_data["unit"]
        sensor_topic = sensor_data['topic']
        sensor_lat = sensor_data["lat"]
        sensor_lon = sensor_data["lon"]
        db_tag = sensor_data['db_tag']

        try:
            cur.execute('INSERT INTO sensor VALUES (?, ?, ?, ?, ?, ?, ?)', (sensor_tag, sensor_type, sensor_topic, sensor_unit, sensor_lat, sensor_lon, db_tag,))
        except:
            continue
    
    con.commit() 


