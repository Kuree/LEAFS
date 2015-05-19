from paho.mqtt.client import Client
import paho.mqtt.publish as publish
import random, json
from QueryObject import QueryObject, QueryStreamObject
from LoggingHelper import logger


class QueryClient:
    '''
    This class is used for query certain topic with streaming support
    Note: It spawns another thread in the background, hence it will not block the current thread
    '''
    _START = 0
    _PAUSE = 1
    _DELETE = 2
    
    _QUERY_REQUEST_TOPIC = "/Query/Request/"
    _QUERY_COMMAND_TOPIC = "/Query/Command/"
    _QUERY_RESULT_STRING = "/Query/Result/"
    _WINDOW_REQUEST_TOPIC_STRING = "/Query/Window/"


    def __init__(self, api_key, hostname = "mqtt.bucknell.edu"):
        '''
        Initialize the class with query requirement.
        topic:  the topic to listen to
        start: epoch time stamp. Data after the start time will be returned by the server
        end: epoch time stamp. Data before the end time will be returned by the server
        persistent: if set to true, then the server will store the query object and streaming data to that topic
        Note: once the object is created, it will automatically send the query to the server
        '''

        self._HOSTNAME = hostname
        self.api_key = api_key
        self.queries = {}

        self._query_sub = Client()

        self.__has_started = False


    def add_query(self, db_tag,  topic, start, end, persistent= True, compute = None):
        query_id = random.randrange(10000000)    # make sure that each query object id is unique
        query_obj = QueryObject.create_query_obj(db_tag, topic, start, end, persistent, self.api_key, query_id)
        query_obj.compute = compute
        self.queries[query_obj.request_id] = query_obj

        # manually subscribe to topic is the client has connected to the broker
        if self.__has_started: 
            self._query_sub.subscribe(query_obj.db_tag + QueryClient._QUERY_RESULT_STRING + query_obj.request_id)
            # send the query object to the server
            publish.single(query_obj.db_tag + QueryClient._QUERY_REQUEST_TOPIC + query_obj.request_id, json.dumps(query_obj.to_object()), hostname=self._HOSTNAME)

            # request window agent
            if persistent and compute is not None:
                stream_obj = QueryStreamObject.create_stream_obj(self.api_key, query_id, query_obj.topic, db_tag, query_obj.compute)
                publish.single(QueryClient._WINDOW_REQUEST_TOPIC_STRING + query_obj.request_id, json.dumps(stream_obj.to_object()), hostname=self._HOSTNAME)
        

    def start(self):
        '''
        Start to receive streaming data. Only valid if the persistent is set to true.
        If start after a pause, the client will receive a chunk of data from when it paused to the current data.
        Then afterwards the client will receive streaming data
        '''
        self.__handle_command(QueryClient._START)

    def pause(self):
        '''
            Pause to receive streaming data. Only valid if the persistent is set to true
        '''
        self.__handle_command(QueryClient._PAUSE)

    def __handle_command(self, command):
        for request_id in self.queries:
            entry = self.queries[request_id]
            publish.single(entry.db_tag + QueryClient._QUERY_COMMAND_TOPIC + request_id, command, hostname=self._HOSTNAME)

    def delete(self):
        '''
        Delete the query object on the server. The client will no longer receive any data from the server.
        Even after calling start it won't receive anything
        '''
        self.__handle_command(QueryClient._DELETE)

    def _on_receive_message(self, mqttc, obj, msg):
        self.on_message(msg.topic, msg.payload.decode())

    def connect(self):
        '''
        Connect to the agent system
        '''
        # subscribe the query result topic first before send the query to the server
        self._query_sub.connect(self._HOSTNAME)
        self._query_sub.on_message = self._on_receive_message
        self._query_sub.loop_start()
        for request_id in self.queries:
            query_obj = self.queries[request_id]
            self._query_sub.subscribe(query_obj.db_tag + QueryClient._QUERY_RESULT_STRING + query_obj.request_id)
            # send the query object to the server
            publish.single(query_obj.db_tag + QueryClient._QUERY_REQUEST_TOPIC + query_obj.request_id, json.dumps(query_obj.to_object()), hostname=self._HOSTNAME)
            if query_obj.persistent and query_obj.compute is not None:
                stream_obj = QueryStreamObject.create_stream_obj(self.api_key, query_obj.request_id, query_obj.topic, query_obj.db_tag, query_obj.compute)
                publish.single(QueryClient._WINDOW_REQUEST_TOPIC_STRING + query_obj.request_id, json.dumps(stream_obj.to_object()), hostname=self._HOSTNAME)
        self.__has_started = True

    def on_message(self, topic, payload):
        '''
        Need to override if the client needs message from the system
        '''
        pass