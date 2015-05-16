from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from SqlHelper import queryData
import threading
import json
import queue


class QueryCommand:
    '''
        Query command class used by the query system to control the state of query stream
    '''
    _START = 0
    _PAUSE = 1
    _DELETE = 2

    def __init__(self, query_id, command, topic):
        '''
            Initialize the query command object
            query_id: query id that's used in query system
            command: indicates the state of the query object
            topic: specific source stream to listen to
        '''
        self._query_id = int(query_id)
        self._command = int(command)
        self._topic = topic


class QuerySystem:
    _HOSTNAME = "mqtt.bucknell.edu"
    _QUERY_REQUEST_TOPIC_STRING = "Query/Request/+"
    _QUERY_COMMAND_TOPIC_STRING = "Query/Command/+"

    def __init__(self):
        from MongoDB import MongoDBClient
        self.mongodb = MongoDBClient()
        self.sub_list = {}
        self.query_sub = Client()
        self.query_sub.on_message = self.new_query_on_message
        self.query_sub.connect(QuerySystem._HOSTNAME)
        self.query_sub.subscribe(QuerySystem._QUERY_REQUEST_TOPIC_STRING, 0)
        

        command_sub = Client()
        command_sub.on_message = self.command_on_message
        command_sub.connect(QuerySystem._HOSTNAME)
        command_sub.subscribe(QuerySystem._QUERY_COMMAND_TOPIC_STRING)
        self.sub_list[command_sub] = QueryCommand(0, QueryCommand._START, QuerySystem._QUERY_COMMAND_TOPIC_STRING)

        self.t = threading.Thread(target=self.run_sub)
        self.t.start()
        self.message_queue = queue.Queue()
        # TODO change the implementation into thread safe queue

    def message_relay(self, mqttc, obj, msg):
        print("message relay", msg.payload)
        topic = self.sub_list[mqttc]._topic
        query_id = self.sub_list[mqttc]._query_id
        stream_data = json.loads(msg.payload.decode())
        current = stream_data["Timestamp"]
        # update the db current time
        self.update_current_time(query_id, current)

        sub = self.find_non_relay_sub_by_id(query_id)

        if sub is None:
            print("Something went wrong")
            return

        command_obj = self.sub_list[sub]

        if command_obj._command == QueryCommand._START:
            publish.single(topic, json.dumps(stream_data), hostname=QuerySystem._HOSTNAME)
            self.update_db_end(query_id, current)

    def new_query_on_message(self, mqttc, obj, msg):
        from QueryObject import QueryObject
        print("new query message", msg.payload)
        topics = msg.topic.split("/")
        print(topics)
        if len(topics) != 3: return
        query_id = int(topics[2])
        queryObj = QueryObject(msg.payload.decode(), query_id)
        print(queryObj.to_object())

        if queryObj.persistent:
            self.mongodb.add(queryObj)
            sub = Client()
            sub.connect(QuerySystem._HOSTNAME)
            print(queryObj.topic)
            sub.subscribe(queryObj.topic)
            sub.on_message = self.message_relay
            self.sub_list[sub] = QueryCommand(query_id, QueryCommand._START, "Query/Result/" + str(query_id))
            self.update_db_end(query_id, queryObj.end)

        QuerySystem.query_data(queryObj.topic, queryObj.start, queryObj.end, query_id)

    @staticmethod
    def query_data(topic, start, end, id):
        result = QuerySystem.query_db(topic, start, end)
        publish.single("Query/Result/" + str(id), json.dumps(result), hostname=QuerySystem._HOSTNAME)

    def command_control(self, query_id, command):
        command = int(command)
        key_list = self.get_sub_list()
        for key in key_list:
            query_command = self.sub_list[key]
            if query_command._query_id == query_id:
                if command == QueryCommand._PAUSE:
                    query_command._command = command
                    return
                elif command == QueryCommand._DELETE:
                    # disconnect before been deleted
                    key.disconnect()
                    del self.sub_list[key]
                    self.mongodb.delete_by_id(query_id)
                elif command == QueryCommand._START:
                    query_command._command = command
                    db_entry = self.mongodb.find_by_id(query_id)
                    start = db_entry["end"]
                    end = db_entry["current"]
                    db_entry["start"] = start
                    db_entry["end"] = end
                    self.mongodb.add(db_entry)
                    print("query resume", start, end)
                    QuerySystem.query_data(db_entry["topic"], start, end, query_id)


    def command_on_message(self, mqttc, obj, msg):
        payload = msg.payload.decode()
        topics = msg.topic.split("/")
        print(topics)
        if len(topics) != 3:
            return
        query_id = int(topics[-1])
        command = int(msg.payload)
        self.command_control(query_id, command)

    def update_db_end(self, query_id, end):
        query_obj = self.mongodb.find_by_id(query_id)
        if query_obj is None: return
        query_obj["end"] = end
        self.mongodb.add(query_obj)


    def update_current_time(self, query_id, current):
        query_obj = self.mongodb.find_by_id(query_id)
        if query_obj is None:
            print("Something went wrong")
            return
        query_obj["current"] = current
        self.mongodb.add(query_obj)


    def get_sub_list(self):
        key_list = []
        for key in self.sub_list:
            key_list.append(key)
        return key_list

    @staticmethod
    def query_db(topic, start, end):
        return queryData(topic, start, end)


    def find_non_relay_sub_by_id(self, query_id):
        key_list = self.get_sub_list()
        for key in key_list:
            if key in self.sub_list and self.sub_list[key]._query_id == query_id:
                return key
        return None

    def run_sub(self):
        while True:
            key_list = self.get_sub_list()

            for key in key_list:
                if not key in self.sub_list: continue
                key.loop(timeout=0.1)



if __name__ == "__main__":
    system = QuerySystem()







