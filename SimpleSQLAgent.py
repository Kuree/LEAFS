from paho.mqtt.client import Client
import sqlite3, json
from QueryObject import QueryObject
import paho.mqtt.publish as publish
from QueryAgent import QueryAgent


class SQLAgent(QueryAgent):
    '''
    A simple SQLite adapter to handle in coming queries
    '''

    def __init__(self, block_current_thread = False):
        # initialize the sqlite connection
        self.connection = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.connection.cursor()

        super().__init__("SQL", block_current_thread)


    def _query_data(self, topic, start, end):
        query = (topic, start, end)
        self.cursor.execute('select * from WaterLevel where SensorName=? and time >= ? and time <= ?', query)
        result = []
        for row in self.cursor:
            t = (row[1], row[2])
            result.append(t)
        return result


if __name__ == "__main__":
    sql = SQLAgent(True)