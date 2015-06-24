from lxml import etree
from sensor import Sensor
import os
import sqlite3

class SensorML:
    ns = {"sml" : "http://www.opengis.net/sensorml/2.0" ,
        "swe" : "http://www.opengis.net/swe/2.0",
        "gml" :  "http://www.opengis.net/gml/3.2",
        "leafs" : "http://www.bucknell.edu/leafs"}

    XML_FOLDER = "sensorml"

    def __init__(self):
        self.con = sqlite3.connect("meta/sensor_meta.db")
        self.cur = self.con.cursor()
        self._sensor_list = []
        self._load_xml()

    def _load_xml(self):
        
        for file in os.listdir(SensorML.XML_FOLDER):
            if not file.endswith(".xml"):
                continue
            with open(SensorML.XML_FOLDER + "/" + file, "r") as f:
                self.add_sensorml(f)


    def add_sensorml(self, sensorml):
        tree = etree.parse(sensorml)
        # get measurement type and units
        # TODO: add streaming processing
        measure_type, unit = "", ""
        for output in tree.xpath('//sml:output', namespaces=SensorML.ns):
            try:
                element = output.xpath("//sml:DataInterface//sml:data//swe:DataStream", namespaces=SensorML.ns)[0]
                measure_type = element.xpath("//swe:Quantity", namespaces=SensorML.ns)[0].attrib["definition"].split("/")[-1]
                unit = element.xpath("//swe:uom", namespaces=SensorML.ns)[0].attrib["code"]
            except Exception as ex:
                print(ex)
                continue
                
        # get sensor station location
        sensor_station = tree.xpath("//sml:position", namespaces=SensorML.ns)[0]
        point = sensor_station.xpath("//gml:Point", namespaces=SensorML.ns)[0]
        lat, lon = point.xpath("gml:coordinates", namespaces=SensorML.ns)[0].text.split(" ")
        lat, lon = float(lat), float(lon)
            
        # get streaming topic
        topic = tree.xpath("//leafs:topic", namespaces=SensorML.ns)[0].text
        sensor_tag = tree.xpath("//leafs:sensor_tag", namespaces=SensorML.ns)[0].text
        db_tag = tree.xpath("//leafs:db_tag", namespaces=SensorML.ns)[0].text

        self.cur.execute("SELECT * FROM sensor WHERE sensor_tag = ?", (sensor_tag, ))
        matched_data= self.cur.fetchall()
        if len(matched_data) == 0:
            self.cur.execute("INSERT INTO sensor Values (?, ?, ?, ?, ?, ?, ?)", (sensor_tag, measure_type, topic, unit, lat, lon, db_tag))
            self.con.commit()
        
        self._sensor_list.append(Sensor(measure_type, unit, (lat, lon), topic))

if __name__ == "__main__":
    s = SensorML()
    print(s._sensor_list)
