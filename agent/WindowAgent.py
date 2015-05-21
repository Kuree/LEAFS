import json, time, logging
from paho.mqtt.client import Client
from core import QueryStreamObject, QueryCommand
from paho.mqtt import publish
from core import logger
from core import MongoDBClient
from core import msgEncode

class WindowAgent:
    """
    Only new window request will go though this agent
    """

    _WINDOW_TOPIC_STRING = "+/Query/Window/+/+"
    _COMMAND_TOPIC_STRING = "+/Query/Command/+/+"
    _QUERY_RESULT_STRING = "/Query/Result/"
    _HOSTNAME = "mqtt.bucknell.edu"
    _COMPUTE_REQUEST_TOPIC_STRING = "/Query/Compute/"


    _MAX_QOS_0_WINDOW_SIZE = 2

    def __init__(self, block_current_thread = False):
        """Initialize the window agent"""
        self._stream_request_sub = Client()
        self._stream_request_sub.on_message = self.on_window_message
        self._stream_request_sub.connect(WindowAgent._HOSTNAME)
        self._stream_request_sub.subscribe(WindowAgent._WINDOW_TOPIC_STRING)

        self._stream_sub = Client()
        self._stream_sub.on_message = self.on_stream_message
        self._stream_sub.connect(WindowAgent._HOSTNAME)

        self._command_sub = Client()
        self._command_sub.on_message = self._on_command_message
        self._command_sub.connect(WindowAgent._HOSTNAME)
        self._command_sub.subscribe(WindowAgent._COMMAND_TOPIC_STRING)

        # TODO: currently is a in-memory db. need to replace it with a on disk database
        self._mongodb = MongoDBClient()

        self._topic_request_dict = {}
        self._stream_command = {}   # handle the streaming command

        # restore the in memory database
        for topic, request_id, streams in self._mongodb.get_all_topic_stream():
            for stream in streams:
                topics = request_id.split("/")
                api_key = topics[0]
                query_id = topics[1]
                stream_command = QueryStreamObject(stream, api_key, query_id)
                if topic not in self._topic_request_dict:
                    self._topic_request_dict[topic] = [stream_command]
                else:
                    self._topic_request_dict[topic].append(stream_command)

        for request_id, command in self._mongodb.get_all_command():
            self._stream_command[request_id] = command

        # used for window buffering
        # usage request_id as key 
        self._window_buffer = {}


        self.block_current_thread = block_current_thread

    def _on_command_message(self, mqttc, obj, msg):
        """Handle the stream command like START and PAUSE"""
        api_key, query_id = WindowAgent.get_query_client_info(msg.topic)
        request_id = api_key + "/" + query_id
        query_command = QueryCommand(request_id, int(msg.payload))
        if request_id in self._stream_command:
            if query_command != self._stream_command[request_id]:
                self._stream_command[request_id] = query_command._command
                self._mongodb.update_command(request_id, query_command._command)
                topic, stream = self.find_stream_command(api_key, query_id)
                if query_command._command == QueryCommand.PAUSE:
                    if stream is not None:
                        stream.data = [] # clear old data because next time it starts, the data is not streaming any more
                elif query_command._command == QueryCommand.DELETE:
                    # delete the stuff
                    if topic is not None:
                        self._topic_request_dict[topic].remove(stream)
                        del self._stream_command[request_id]

                        self._mongodb.delete_command(request_id)
                        self._mongodb.delete_single_topic_stream(topic)

                        if self._mongodb.count_topic_stream(topic) == 0:
                            self._mongodb.delete_all_topic_stream(topic)
                        

    def find_stream_command(self, api_key, query_id):
        for topic in self._topic_request_dict:
            for stream in self._topic_request_dict[topic]:
                if stream.api_key == api_key and stream.query_id == query_id:
                    return topic, stream
        return None, None

    @staticmethod
    def get_query_client_info(topic):
        topics = topic.split("/")
        if len(topics) != 5:  logger.log(logging.WARN, "Query should have 5 levels")
        api_key = topics[-2]
        query_id = topics[-1]
        return api_key, query_id

    def on_window_message(self, mqttc, obj, msg):
        """
        Handle the window request message. It will keep the interested topic into memory so that later on
        when streaming message arrives it can perform window task.
        """
        api_key, query_id = WindowAgent.get_query_client_info(msg.topic)
        stream_command = QueryStreamObject(msg.payload.decode(), api_key, query_id)
        request_id = api_key + "/" + query_id
        stream_topic = stream_command.topic

        self._stream_command[request_id] = QueryCommand.START
        self._mongodb.add_stream_command(request_id, QueryCommand.START)

        # db check
        if stream_topic in self._topic_request_dict:
            self._topic_request_dict[stream_topic].append(stream_command)
        else:
            self._topic_request_dict[stream_topic] = [stream_command]
            self._stream_sub.subscribe(stream_command.topic)     

        self._mongodb.add_topic_stream(stream_topic, request_id, stream_command.to_object())       

    def _get_list_copy(self, lst):
        """
        A helper function to deal with multi-threading list change
        """
        result = []
        for entry in lst:
            result.append(entry)
        return result

    def on_stream_message(self, mqttc, obj, msg):
        """
        Handle the streaming message. It will look up the streaming command table and see if the data should be kept
        in memory to perform window task.
        """
        topic = msg.topic
        qos = msg.qos
        data_point = msgEncode.decode(msg.payload)
        timestamp, sequence_number, value = data_point
        if topic in self._topic_request_dict:
            # put the data into data store
            stream_command_list = self._topic_request_dict[topic]
            for stream_command in self._get_list_copy(stream_command_list):
                # check if we needs to send the data to client/compute system
                # TODO: make this more reliable
                request_id = stream_command.api_key + "/" + stream_command.query_id
                if self._stream_command[request_id] != QueryCommand.START: continue # skip over the non-start command

                # get the interval
                # there might be better way to do it...
                interval = stream_command.compute_command[0][1] if stream_command.compute_command is not None else 0

                # deal with window buffer now.
                result = self.add_stream_data(stream_command, qos, data_point, interval, request_id)
                if result is not None:
                    if stream_command.compute_command is not None:
                        publish.single(stream_command.db_tag + WindowAgent._COMPUTE_REQUEST_TOPIC_STRING + request_id, msgEncode.encode(result, compute=stream_command.compute_command), hostname=WindowAgent._HOSTNAME)
                    else:
                        publish.single(stream_command.db_tag + WindowAgent._QUERY_RESULT_STRING + request_id,
                                       msgEncode.encode(result), hostname=WindowAgent._HOSTNAME)

                    # empty the list for the next interval
                    # and then add the data
                    stream_command.data = []

    def add_stream_data(self, stream_command, qos, data_point, interval, request_id):
        data_list = stream_command.data
        if len(data_list) == 0:
            # list is empty. cool.
            stream_command.data.append(data_point)
            return None
        # we assume that the stream_command.data is already sorted by sequence number
        sequence_number = data_point[1]
        if sequence_number < data_list[-1][1]: # it is in the window
            if qos != 2:
                # check duplicated points
                search = [x for x in data_list if x[1] == sequence_number]
                if len(search) == 0: # this is duplicated points
                    return None
            data_list.append(data_point)
            data_list.sort(key=lambda point: point[1]) # sort the list according to the sequence number
            if data_list[0][0] + interval <= data_list[-1][0]:
                # the window is full
                return data_list
            else:
                return None
        else:
            # okay need to check if we need to send the window
            if data_point[0] >= data_list[0][0] + interval:
                # outside the window 
                if request_id not in self._window_buffer: self._window_buffer[request_id] = []
                self._window_buffer[request_id].append(data_point)
                stream_command.data.sort(key=lambda x : x[1])
                if qos == 0 and len(self._window_buffer[request_id]) > WindowAgent._MAX_QOS_0_WINDOW_SIZE:
                    # okay something really messed up. need to return the window
                    # the network might just got really slow
                    result = stream_command.data
                    stream_command.data = self._window_buffer[request_id]
                    stream_command.data.sort(key=lambda x : x[1])
                    self._window_buffer[request_id] = []
                    # bump the buffer to the actual window and clear the window
                    return result
                else:
                    # add it to the buffer
                    self._window_buffer[request_id].append(data_point)
                    return None
            else:
                # inside the window
                data_list.append(data_point)
                data_list.sort(key=lambda x: x[1])
                return None
        return None 

    def connect(self):
        """
        Call this function if you want to start the window agent
        Note: if you want to change the behavior of the window agent, you can replace the on_message methods
        """
        self._stream_request_sub.loop_start()
        self._stream_sub.loop_start()
        self._command_sub.loop_start()

        for topic in self._topic_request_dict:
            for stream_command in self._topic_request_dict[topic]:
                self._stream_sub.subscribe(stream_command.topic)



        if self.block_current_thread:
            # block the current thread
            while True:
                time.sleep(100)


                   
if __name__ == "__main__":
    a = WindowAgent(True)
    a.connect()