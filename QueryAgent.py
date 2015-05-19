from paho.mqtt.client import Client
import paho.mqtt.publish as publish
from SqlHelper import queryData
import json, time, logging
from LoggingHelper import logger as logger
from QueryObject import QueryCommand, QueryObject


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
        self._QUERY_COMMAND_TOPIC_STRING = self._DATABASE_TAG + "/Query/Command/+/+"
        self._QUERY_RESULT_TOPIC_STRING = self._DATABASE_TAG + "/Query/Result/"
        self._COMPUTE_REQUEST_TOPIC_STRING = self._DATABASE_TAG + "/Query/Compute/"

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
        self._query_request_sub.connect(self._HOSTNAME)
        self._query_request_sub.subscribe(self._QUERY_REQUEST_TOPIC_STRING, 0) # currently use qos level 0
        

        self._command_sub = Client()
        self._command_sub.on_message = self._command_on_message
        self._command_sub.connect(self._HOSTNAME)
        self._command_sub.subscribe(self._QUERY_COMMAND_TOPIC_STRING)

        self._query_relay_sub = Client()
        self._query_relay_sub.on_message = self._message_relay
        self._query_relay_sub.connect(self._HOSTNAME)        

        self.block_current_thread = block_current_thread



    def _message_relay(self, mqttc, obj, msg):
        """
        Forward the message to streaming topic with query id
        """
        logger.log(logging.DEBUG, "message relay: " +  msg.payload.decode())
        topic = msg.topic
        if topic not in self._topic_id_dict:
            return # no client subscribe to it

        request_id_list = self._topic_id_dict[topic]
        raw_data = msg.payload.decode()
        stream_data = json.loads(raw_data)
        current = stream_data["Timestamp"]
        for request_id in request_id_list:  # loop through the id list (if any)
            
            # update the db current time
            self._update_current_time(request_id, current)

            if request_id not in self._query_command_dict: 
                logger.log(logging.ERROR, "Zero query subscriber, but still subscribed to incoming message")
                return # shouldn't hit here, but just in case

            # get the command object from in memory document
            command_obj = self._query_command_dict[request_id]

            # if the client requests streaming data
            if command_obj._command == QueryCommand.START:
                publish.single(command_obj._topic, raw_data, hostname=self._HOSTNAME)
                self._update_db_end(request_id, current)

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

    def add_streaming_query(self, topic, request_id):
        """
        Add request_id and topic to the system manually
        topic: streaming topic to listen to
        request_id: unique request id: API_KEY/ID
        """
        if topic in self._topic_id_dict:
            self._topic_id_dict[topic].append(request_id)
        else:
            self._topic_id_dict[topic] = [request_id]

    def _handle_command_control(self, request_id, command):
        if request_id not in self._query_command_dict:
            # not in memory, reject the command request
            return
        command = int(command)
        query_command = self._query_command_dict[request_id]
        if command == QueryCommand.PAUSE:
            query_command._command = command
            return
        elif command == QueryCommand.DELETE:
            # delete the query object from mongodb
            self._mongodb.delete_by_id(request_id)

            # remove the key from command dictionary
            del self._query_command_dict[request_id]

            # remove the key from topic id list
            query_topic = None
            for key in self._topic_id_dict: # find the key
                if request_id in self._topic_id_dict[key]:
                    query_topic = key
            if query_topic is None:
                logger.log(logging.ERROR, "Could not find query id for deleting query object. Query ID: " +  str(request_id))
                return
            self._topic_id_dict[query_topic].remove(request_id)
            if len(self._topic_id_dict[query_topic]) == 0:
                # if the list is empty, then delete it
                del self._topic_id_dict[query_topic]
                # remove the topic from subscription
            self._query_relay_sub.unsubscribe(query_topic)
        elif command == QueryCommand.START:
            query_command._command = command
            db_entry = self._mongodb.find_by_id(request_id)
            start = db_entry["end"]
            end = db_entry["current"]
            db_entry["start"] = start
            db_entry["end"] = end
            self._mongodb.add(db_entry)
            logger.log(logging.INFO, "Resume query " + "{0:d} {1:d}".format(start, end))
            # need to check the compute
            request_id = db_entry["id"]
            if "compute" in db_entry:
                self._handle_query_request(self._query_command_dict[request_id].compute, request_id, start, end)
            else:
                self._handle_query_request(db_entry["topic"], None, request_id, start, end)
            

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


    def _update_current_time(self, request_id, current):
        query_obj = self._mongodb.find_by_id(request_id)
        if query_obj is None:
            logger.log(logging.ERROR, "Could not find query id for updating current time. Query id: " +  str(request_id))
            return
        query_obj["current"] = current
        self._mongodb.add(query_obj)

    @staticmethod
    def _query_db(topic, start, end):
        return queryData(topic, start, end)

    def connect(self):
        # start the loop in the background
        self._query_request_sub.loop_start()
        self._command_sub.loop_start()
        self._query_relay_sub.loop_start() 

        if self.block_current_thread:
            while True:
                time.sleep(100)

