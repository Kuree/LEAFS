class Sensor:
    def __init__(self, sensor_tag, type, unit, location, streamming_topic, db_tag):
        '''
        Note: type is used as standard name in CF
        '''
        self.tag = sensor_tag
        self.type = type
        self.unit = unit
        self.lat = location[0]
        self.lon = location[1]
        self.topic = streamming_topic
        self.db_tag = db_tag

    def in_range(self, lat, lon, distance):
        return True

    def __repr__(self):
        return "Measurement type: {0} uint: {1} location: ({2:.4f}, {3:.4f}) streaming topic: {4}".format(
            self.type, self.unit, self.lat, self.lon, self.topic)


