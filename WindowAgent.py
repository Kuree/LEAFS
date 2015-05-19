import sqlite3, json, time
from paho.mqtt.client import Client
from QueryObject import QueryStreamObject
from paho.mqtt import publish


class WindowAgent:
    '''
    Only new window request will go though this agent
    '''

    _WINDOW_TOPIC_STRING = "+/Query/Window/+/+"
    _COMMAND_TOPIC_STRING = "+/Query/Command/+/+"
    _QUERY_RESULT_STRING = "/Query/Result/"
    _HOSTNAME = "mqtt.bucknell.edu"
    _COMPUTE_REQUEST_TOPIC_STRING = "/Query/Compute/"

    def __init__(self, block_current_thread = False):
        self._stream_request_sub = Client()
        self._stream_request_sub.on_message = self.on_window_message
        self._stream_request_sub.connect(WindowAgent._HOSTNAME)
        self._stream_request_sub.subscribe(WindowAgent._WINDOW_TOPIC_STRING)

        self._stream_sub = Client()
        self._stream_sub.on_message = self.on_stream_message
        self._stream_sub.connect(WindowAgent._HOSTNAME)

        # TODO: currently is a in-memory db. need to replace it with a on disk database
        self._topic_request_dict = {}

        self._stream_request_sub.loop_start()
        self._stream_sub.loop_start()

        if block_current_thread:
            while True:
                time.sleep(100)

    def on_window_message(self, mqttc, obj, msg):
        topics = msg.topic.split("/")
        # TODO: replace this with log
        assert len(topics) == 5, "Query should have 5 levels"
        api_key = topics[-2]
        query_id = topics[-1]
        stream_command = QueryStreamObject(msg.payload.decode(), api_key, query_id)
        request_id = api_key + "/" + query_id
        stream_topic = stream_command.topic

        # db check
        if stream_topic in self._topic_request_dict:
            self._topic_request_dict[stream_topic].append(stream_command)
        else:
            self._topic_request_dict[stream_topic] = [stream_command]
            self._stream_sub.subscribe(stream_command.topic)            

    def on_stream_message(self, mqttc, obj, msg):
        topic = msg.topic
        data_dict = json.loads(msg.payload.decode())
        timestamp, value = data_dict["Timestamp"], data_dict["Value"]
        if topic in self._topic_request_dict:
            # put the data into data store
            stream_command_list = self._topic_request_dict[topic]
            for stream_command in stream_command_list:
                # check if we needs to send the data to client/compute system
                # TODO: make this more reliable
                interval = stream_command.compute_command[0]["arg"][0] if stream_command.compute_command is not None else 1
                if len(stream_command.data) > 0 and stream_command.data[0][0] + interval <= timestamp:
                    request_id = stream_command.api_key + "/" + stream_command.query_id
                    if stream_command.compute_command is not None:
                        query = {"data" : stream_command.data, "compute" : stream_command.compute_command}
                        publish.single(stream_command.db_tag + WindowAgent._COMPUTE_REQUEST_TOPIC_STRING + request_id, json.dumps(query), hostname=WindowAgent._HOSTNAME)
                    else:
                        publish.single(stream_command.db_tag + WindowAgent._QUERY_RESULT_STRING + request_id,
                                       json.dumps(stream_command.data), hostname=WindowAgent._HOSTNAME)

                    # empty the list for the next interval
                    # and then add the data
                    stream_command.data = []
                stream_command.data.append((timestamp, value))

                   
if __name__ == "__main__":
    a = WindowAgent(True)
            
