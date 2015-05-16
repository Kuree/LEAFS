from paho.mqtt.client import Client
import paho.mqtt.publish as publish
import threading
import random
from QueryObject import QueryObject
import json


class QueryClient:
    '''
        This class is used for query certain topic with streaming support
        Note: It spawns another thread in the background, hence it will not block the current thread
    '''
    _HOSTNAME = "mqtt.bucknell.edu"
    _QUERY_REQUEST_TOPIC = "Query/Request/"
    _QUERY_COMMAND_TOPIC = "Query/Command/"
    _QUERY_RESULT_STRING = "Query/Result/"
    _START = 0
    _PAUSE = 1
    _DELETE = 2

    def __init__(self, topic, start, end, persistent = True):
        '''
            Initialize the class with query requirement.
            topic:  the topic to listen to
            start: epoch time stamp. Data after the start time will be returned by the server
            end: epoch time stamp. Data before the end time will be returned by the server
            persistent: if set to true, then the server will store the query object and streaming data to that topic
            Note: once the object is created, it will automatically send the query to the server
        '''
        self._query_sub = Client()
        self._query_id = random.randrange(10000000)    # make sure that each query object id is unique
        query_obj = QueryObject.create_query_obj(topic, start, end, persistent, self._query_id)
        query_obj.persistent = persistent
        # subscribe the query result topic first before send the query to the server
        self._query_sub.connect(QueryClient._HOSTNAME)
        self._query_sub.subscribe(QueryClient._QUERY_RESULT_STRING + str(self._query_id))
        self._query_sub.on_message = self._on_message
        self._query_sub.loop_start()
        # send the query object to the server
        publish.single(QueryClient._QUERY_REQUEST_TOPIC + str(self._query_id ), json.dumps(query_obj.to_object()), hostname=QueryClient._HOSTNAME)

    def start(self):
        '''
            Start to receive streaming data. Only valid if the persistent is set to true.
            If start after a pause, the client will receive a chunk of data from when it paused to the current data.
            Then afterwards the client will receive streaming data
        '''
        publish.single(QueryClient._QUERY_COMMAND_TOPIC + str(self._query_id), QueryClient._START, hostname=QueryClient._HOSTNAME)

    def pause(self):
        '''
            Pause to receive streaming data. Only valid if the persistent is set to true
        '''
        publish.single(QueryClient._QUERY_COMMAND_TOPIC + str(self._query_id), QueryClient._PAUSE, hostname=QueryClient._HOSTNAME)

    def delete(self):
        '''
            Delete the query object on the server. The client will no longer receive any data from the server.
            Even after calling start it won't receive anything
        '''
        publish.single(QueryClient._QUERY_COMMAND_TOPIC + str(self._query_id), QueryClient._DELETE, hostname=QueryClient._HOSTNAME)

    def _on_message(self, mqttc, obj, msg):
        print(msg.payload)