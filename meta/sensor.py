class Sensor:
    def __init__(self, type, unit, location, streamming_topic):
        '''
        Note: type is used as standard name in CF
        '''
        self.type = type
        self.unit = unit
        self.lat = location[0]
        self.lon = location[1]
        self.topic = streamming_topic

    def in_range(self, lat, lon, distance):
        return True

    def __repr__(self):
        return "Measurement type: {0} uint: {1} location: ({2:.4f}, {3:.4f}) streaming topic: {4}".format(
            self.type, self.unit, self.lat, self.lon, self.topic)


