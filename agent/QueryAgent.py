from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from SqlHelper import queryData
import json, time, logging
from core import logger as logger
from core import QueryCommand, QueryObject


class QueryAgent:
    """
    A query handling system that supports querying history data as well as streaming data.
    Users can pause and resume the stream.
    """
    

    def __init__(self, db_tag, block_current_thread = False):
        """
        Initialize the query system. If block_current_thread is set to True, then it will block the current thread
        """

        # declare the constants
        self._HOSTNAME = "mqtt.bucknell.edu"
        self._DATABASE_TAG = db_tag
        self._QUERY_REQUEST_TOPIC_STRING = self._DATABASE_TAG + "/Query/Request/+/+"
        self._QUERY_RESULT_TOPIC_STRING = self._DATABASE_TAG + "/Query/Result/"
        self._COMPUTE_REQUEST_TOPIC_STRING = self._DATABASE_TAG + "/Query/Compute/"
        
        # create query sub, listening to all the query requests
        self._query_request_sub = Client()
        self._query_request_sub.on_message = self._new_query_on_message
        self._query_request_sub.connect(self._HOSTNAME)
        self._query_request_sub.subscribe(self._QUERY_REQUEST_TOPIC_STRING, 0) # currently use qos level 0
        
        self.block_current_thread = block_current_thread

    def _new_query_on_message(self, mqttc, obj, msg):
        """
        Handle in coming new query request
        """
        logger.log(logging.INFO, "New query message: " +  msg.payload.decode())
        topic = msg.topic
        topics = topic.split("/")
        if len(topics) != 5: 
            logger.log(logging.ERROR, "Incorrect query request")
            return
        api_key = topics[-2]
        query_id = topics[-1]
        db_tag = topics[0]
        request_id = api_key + "/" + query_id
        query_obj = QueryObject(msg.payload.decode(), api_key, query_id)

        logger.log(logging.INFO, "Query message: " +  json.dumps(query_obj.to_object()))

        # record the topic as well as query id
        self.add_streaming_query(query_obj.topic, request_id)

        if query_obj.persistent:
            # because it is persistent, we need to store the query information
            self._mongodb.add(query_obj)            

            # register it in the window system
            if query_obj.compute is None:
                # let the query agent to relay the message
                self._query_relay_sub.subscribe(query_obj.topic)
                self._query_command_dict[request_id] = QueryCommand(request_id, QueryCommand.START)

        self._handle_query_request(query_obj.topic, query_obj.compute, request_id, query_obj.start, query_obj.end)

    def _query_data(self, topic, start, end):
        # This method needs to be overridden for any real database agent
        return []


              

    def _handle_query_request(self, topic, compute, request_id, start, end):
        # let the underlying database system handle it
        raw_data = self._query_data(topic, start, end)
        if compute is None:
            publish.single(self._QUERY_RESULT_TOPIC_STRING + request_id, json.dumps(raw_data), hostname=self._HOSTNAME)
        else:
            query = {"data" : raw_data, "compute" : compute}
            publish.single(self._COMPUTE_REQUEST_TOPIC_STRING + request_id, json.dumps(query), hostname=self._HOSTNAME)
        return

    def _command_on_message(self, mqttc, obj, msg):
        payload = msg.payload.decode()
        topics = msg.topic.split("/")
        if len(topics) != 5:
            logger.log(logging.ERROR, "Incorrect command request")
            return
        request_id = topics[-2] + "/" +  topics[-1]
        command = int(msg.payload)
        self._handle_command_control(request_id, command)

    def _update_db_end(self, request_id, end):
        query_obj = self._mongodb.find_by_id(request_id)
        if query_obj is None: 
            logger.log(logging.ERROR, "Could not find query id for updating end time. Query ID: " +  str(request_id))
            return
        query_obj["end"] = end
        self._mongodb.add(query_obj)


    @staticmethod
    def _query_db(topic, start, end):
        return queryData(topic, start, end)

    def connect(self):
        # start the loop in the background
        self._query_request_sub.loop_start()

        if self.block_current_thread:
            while True:
                time.sleep(100)

