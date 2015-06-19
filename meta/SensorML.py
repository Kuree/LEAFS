from lxml import etree
from sensor import Sensor


class SensorML:
    def __init__(self):
        self._sensor_list = []
        self._load_xml()

    def _load_xml(self):
        ns = {"sml" : "http://www.opengis.net/sensorml/2.0" ,
        "swe" : "http://www.opengis.net/swe/2.0",
        "gml" :  "http://www.opengis.net/gml/3.2",
        "leafs" : "http://www.bucknell.edu/leafs"}
        with open("meta/sensor1.xml", "r") as f:
            tree = etree.parse(f)
            # get measurement type and units
            # TODO: add streaming processing
            measure_type, uint = "", ""
            for output in tree.xpath('//sml:output', namespaces=ns):
                try:
                    element = output.xpath("//sml:DataInterface//sml:data//swe:DataStream", namespaces=ns)[0]
                    measure_type = element.xpath("//swe:elementType", namespaces=ns)[0].attrib["name"]
                    uint = element.xpath("//swe:uom", namespaces=ns)[0].attrib["code"]
                except Exception as ex:
                    print(ex)
                    continue

            # get sensor station location
            sensor_station = tree.xpath("//sml:position", namespaces=ns)[0]
            point = sensor_station.xpath("//gml:Point", namespaces=ns)[0]
            lat, lon = point.xpath("gml:coordinates", namespaces=ns)[0].text.split(" ")
            lat, lon = float(lat), float(lon)

            # get streaming topic
            topic = tree.xpath("//leafs:topic", namespaces=ns)[0].text

            self._sensor_list.append(Sensor(measure_type, uint, (lat, lon), topic))




if __name__ == "__main__":
    s = SensorML()
    print(s._sensor_list)
