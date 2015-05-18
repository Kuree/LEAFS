from paho.mqtt.client import Client
import sqlite3, json
from QueryObject import QueryObject
import paho.mqtt.publish as publish

class SQLAdapter:
    '''
    A simple SQLite adapter to handle in coming queries
    '''

    _HOSTNAME = "mqtt.bucknell.edu"
    _QUERY_DATABASE_STRING = "Query/Database/+"
    _QUERY_RESULT_TOPIC_STRING = "Query/Result/"
    _COMPUTE_REQUEST_TOPIC_STRING = "Query/Compute/"

    def __init__(self, block_current_thread = False):

        # initialize the sqlite connection
        
        self.connection = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.connection.cursor()

        self.db_sb = Client()
        self.db_sb.on_message = self.on_db_message
        self.db_sb.connect(SQLAdapter._HOSTNAME)
        self.db_sb.subscribe(SQLAdapter._QUERY_DATABASE_STRING)
        

        if block_current_thread:
            self.db_sb.loop_forever()
        else:
            self.db_sb.loop_start()
     
    def on_db_message(self, mqttc, obj, msg):
        #TODO: filter the message
        topics = msg.topic.split("/")
        # TODO: need to change to log
        assert len(topics) == 3, "The format of query topic should be Query/Database/+"
        
        query_id = int(topics[-1])
        query_obj = QueryObject(msg.payload.decode(), query_id)
        query_topic = query_obj.topic
        start = query_obj.start
        end = query_obj.end

        data = self.query(query_topic, start, end)
        if query_obj.compute is None: 
            publish.single(SQLAdapter._QUERY_RESULT_TOPIC_STRING + str(query_id), json.dumps(data), hostname=SQLAdapter._HOSTNAME)
        else:
            query = {"data" : data, "compute" : query_obj.compute}
            publish.single(SQLAdapter._COMPUTE_REQUEST_TOPIC_STRING + str(query_id), json.dumps(query), hostname=SQLAdapter._HOSTNAME)
        
        pass

    def query(self, topic, start, end):
        query = (topic, start, end)
        self.cursor.execute('select * from WaterLevel where SensorName=? and time >= ? and time <= ?', query)
        result = []
        for row in self.cursor:
            t = (row[1], row[2])
            result.append(t)
        return result


if __name__ == "__main__":
    sql = SQLAdapter(True)