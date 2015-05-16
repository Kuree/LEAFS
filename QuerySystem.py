from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from SqlHelper import queryData
import threading
import json
import queue
import time


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
    '''
        A query handling system that supports querying history data as well as streaming data.
        Users can pause and resume the stream.
    '''
    _HOSTNAME = "mqtt.bucknell.edu"
    _QUERY_REQUEST_TOPIC_STRING = "Query/Request/+"
    _QUERY_COMMAND_TOPIC_STRING = "Query/Command/+"
    _QUERY_RESULT_TOPIC_STRING = "Query/Result/"

    def __init__(self, block_current_thread = False):
        '''
            Initialize the query system. If block_current_thread is set to True, then it will block the current thread
        '''
        # TODO: need to improve this import part
        from MongoDB import MongoDBClient
        # connect to the mongodb
        self._mongodb = MongoDBClient()

        # this is a in-memory document holding link between query id and query
        # command
        self._query_command_dict = {}
        # this is a in-memory document holding link between a topic and query
        # id's (possibly multiple query id with single topic)
        self._topic_id_dict = {}

        # create query sub, listening to all the query requests
        self._query_request_sub = Client()
        self._query_request_sub.on_message = self._new_query_on_message
        self._query_request_sub.connect(QuerySystem._HOSTNAME)
        self._query_request_sub.subscribe(QuerySystem._QUERY_REQUEST_TOPIC_STRING, 0) # currently use qos level 0
        self._query_request_sub.loop_start() # start the loop in the background

        self._command_sub = Client()
        self._command_sub.on_message = self._command_on_message
        self._command_sub.connect(QuerySystem._HOSTNAME)
        self._command_sub.subscribe(QuerySystem._QUERY_COMMAND_TOPIC_STRING)
        self._command_sub.loop_start() # start the loop in the background

        
        self._query_relay_sub = Client()
        self._query_relay_sub.on_message = self._message_relay
        self._query_relay_sub.connect(QuerySystem._HOSTNAME)
        self._query_relay_sub.loop_start() # start the loop in the background

        if block_current_thread:
            while True:
                time.sleep(100)

    def _message_relay(self, mqttc, obj, msg):
        '''
            Forward the message to streaming topic with query id
        '''
        # TODO: replace the print with logging
        print("message relay", msg.payload)
        topic = msg.topic
        query_id_list = self._topic_id_dict[topic]
        raw_data = msg.payload.decode()
        stream_data = json.loads(raw_data)
        current = stream_data["Timestamp"]
        for query_id in query_id_list:  # loop through the id list (if any)
            # update the db current time
            self._update_current_time(query_id, current)

            # TODO: fix the bug here
            # dirty fix
            if query_id not in self._query_command_dict:
                return

            # get the command object from in memory document
            command_obj = self._query_command_dict[query_id]

            # if the client requests streaming data
            if command_obj._command == QueryCommand._START:
                publish.single(command_obj._topic, raw_data, hostname=QuerySystem._HOSTNAME)
                self._update_db_end(query_id, current)

    def _new_query_on_message(self, mqttc, obj, msg):
        '''
            Handle in coming new query request
        '''
        from QueryObject import QueryObject
        print("new query message", msg.payload)
        topic = msg.topic
        topics = topic.split("/")
        print(topics)
        if len(topics) != 3: 
            # TODO: need to add logging here
            return
        query_id = int(topics[2])
        queryObj = QueryObject(msg.payload.decode(), query_id)

        # TODO: replace with logging here
        print(queryObj.to_object())

        # record the topic as well as query id
        self.add_streaming_query(queryObj.topic, query_id)

        if queryObj.persistent:
            # because it is persistent, we need to store the data
            self._mongodb.add(queryObj)            
            self._query_relay_sub.subscribe(queryObj.topic)
            self._query_command_dict[query_id] = QueryCommand(query_id, QueryCommand._START, QuerySystem._QUERY_RESULT_TOPIC_STRING + str(query_id))

        # send back the historical data, if any
        QuerySystem._send_query_data(queryObj.topic, queryObj.start, queryObj.end, query_id)

    def add_streaming_query(self, topic, query_id):
        '''
            Add query_id and topic to the system manually
            topic: streaming topic to listen to
            query_id: unique query id
        '''
        if topic in self._topic_id_dict:
            self._topic_id_dict[topic].append(query_id)
        else:
            self._topic_id_dict[topic] = [query_id]

    @staticmethod
    def _send_query_data(topic, start, end, id):
        '''
            Publish the query data to the streaming client
        '''
        result = QuerySystem._query_db(topic, start, end)
        publish.single(QuerySystem._QUERY_RESULT_TOPIC_STRING + str(id), json.dumps(result), hostname=QuerySystem._HOSTNAME)

    def _handle_command_control(self, query_id, command):
        query_id = int(query_id)
        if query_id not in self._query_command_dict:
            # not in memory, reject the command request
            return
        command = int(command)
        query_command = self._query_command_dict[query_id]
        if command == QueryCommand._PAUSE:
            query_command._command = command
            return
        elif command == QueryCommand._DELETE:
            # remove the topic from subscription
            self._query_relay_sub.unsubscribe(query_command._topic)

            # delete the query object from mongodb
            self._mongodb.delete_by_id(query_id)

            # remove the key from command dictionary
            del self._query_command_dict[query_id]

            # remove the key from topic id list
            if query_command._topic not in self._topic_id_dict:
                return
            self._topic_id_dict[query_command._topic].remove(query_id)
            if len(self._topic_id_dict[query_command._topic]) == 0:
                # if the list is empty, then delete it
                del self._topic_id_dict[query_command._topic]
        elif command == QueryCommand._START:
            query_command._command = command
            db_entry = self._mongodb.find_by_id(query_id)
            start = db_entry["end"]
            end = db_entry["current"]
            db_entry["start"] = start
            db_entry["end"] = end
            self._mongodb.add(db_entry)
            # TODO : replace this with logging
            print("query resume", start, end)
            QuerySystem._send_query_data(db_entry["topic"], start, end, query_id)


    def _command_on_message(self, mqttc, obj, msg):
        payload = msg.payload.decode()
        topics = msg.topic.split("/")
        print(topics)
        if len(topics) != 3:
            # TODO: need to add logging here
            return
        query_id = int(topics[-1])
        command = int(msg.payload)
        self._handle_command_control(query_id, command)

    def _update_db_end(self, query_id, end):
        query_obj = self._mongodb.find_by_id(query_id)
        if query_obj is None: 
            # TODO: need to add logging here
            return
        query_obj["end"] = end
        self._mongodb.add(query_obj)


    def _update_current_time(self, query_id, current):
        query_obj = self._mongodb.find_by_id(query_id)
        if query_obj is None:
            # TODO: need to add logging here
            return
        query_obj["current"] = current
        self._mongodb.add(query_obj)

    @staticmethod
    def _query_db(topic, start, end):
        return queryData(topic, start, end)


if __name__ == "__main__":
    system = QuerySystem(block_current_thread = True)







