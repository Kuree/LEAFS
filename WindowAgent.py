import sqlite3, json
from paho.mqtt.client import Client
from QueryObject import QueryStreamObject

class WindowAgent:
    '''
    Only new window request will go though this agent
    '''

    _WINDOW_TOPIC_STRING = "+/Query/Window/+/+"
    _COMMAND_TOPIC_STRING = "+/Query/Command/+/+"
    _HOSTNAME = "mqtt.bucknell.edu"

    def __init__(self, block_current_thread = False):
        self._stream_request_sub = Client()
        self._stream_request_sub.on_message = self.on_window_message
        self._stream_request_sub.connect(_HOSTNAME)
        self._stream_request_sub.subscribe(_WINDOW_TOPIC_STRING)

        self._stream_sub = Client()
        self._stream_sub.on_message = self.on_stream_message
        self._stream_request_sub.connect(_HOSTNAME)

        # TODO: currently is a in-memory db. need to replace it with a on disk database
        self._topic_request_dict = {}

        if block_current_thread:
            self._stream_request_sub.loop_forever()
            self._stream_sub.loop_forever()
        else:
            self._stream_request_sub.loop_start()
            self._stream_sub.loop_start()

    def on_window_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        # TODO: replace this with log
        assert len(topics) == 3, "Query should have 5 levels"
        api_key = topics[-2]
        query_id = topics[-1]
        stream_command = QueryStreamObject(msg.payload.decode())
        request_id = api_key + "/" + query_id
        stream_topic = stream_command.topic

        # db check
        if stream_topic in self._topic_request_dict:
            self._topic_request_dict[stream_topic].append(stream_command)
        else:
            self._topic_request_dict[stream_topic] = [stream_command]
            self._stream_sub.subscribe(stream_command.topic)            

    def on_stream_message(self, mqttc, obj, msg):
         pass