import sqlite3, json, time
from paho.mqtt.client import Client
from QueryObject import QueryStreamObject, QueryCommand
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

        self._command_sub = Client()
        self._command_sub.on_message = self.on_command_message
        self._command_sub.connect(WindowAgent._HOSTNAME)
        self._command_sub.subscribe(WindowAgent._COMMAND_TOPIC_STRING)


        # TODO: currently is a in-memory db. need to replace it with a on disk database
        self._topic_request_dict = {}
        self._stream_command = {}   # handle the streaming command

        self._stream_request_sub.loop_start()
        self._stream_sub.loop_start()
        self._command_sub.loop_start()

        if block_current_thread:
            while True:
                time.sleep(100)


    def on_command_message(self, mqttc, obj, msg):
        api_key, query_id = WindowAgent.get_query_client_info(msg.topic)
        request_id = api_key + "/" + query_id
        query_command = QueryCommand(request_id, int(msg.payload))
        if request_id in self._stream_command:
            if query_command != self._stream_command[request_id]:
                self._stream_command[request_id] = query_command._command
                topic, stream = self.find_stream_command(api_key, query_id)
                if query_command._command == QueryCommand._PAUSE:
                    if stream is not None:
                        stream.data = [] # clear old data because next time it starts, the data is not streaming any more
                elif query_command._command == QueryCommand._DELETE:
                    # delete the stuff
                    if topic is not None:
                        self._topic_request_dict[topic].remove(stream)
                        del self._stream_command[request_id]


    def find_stream_command(self, api_key, query_id):
        for topic in self._topic_request_dict:
            for stream in self._topic_request_dict[topic]:
                if stream.api_key == api_key and stream.query_id == query_id:
                    return topic, stream
        return None, None

    @staticmethod
    def get_query_client_info(topic):
        topics = topic.split("/")
        # TODO: replace this with log
        assert len(topics) == 5, "Query should have 5 levels"
        api_key = topics[-2]
        query_id = topics[-1]
        return api_key, query_id

    def on_window_message(self, mqttc, obj, msg):
        api_key, query_id = WindowAgent.get_query_client_info(msg.topic)
        stream_command = QueryStreamObject(msg.payload.decode(), api_key, query_id)
        request_id = api_key + "/" + query_id
        stream_topic = stream_command.topic

        self._stream_command[request_id] = QueryCommand._START

        # db check
        if stream_topic in self._topic_request_dict:
            self._topic_request_dict[stream_topic].append(stream_command)
        else:
            self._topic_request_dict[stream_topic] = [stream_command]
            self._stream_sub.subscribe(stream_command.topic)            

    def _get_list_copy(self, lst):
        result = []
        for entry in lst:
            result.append(entry)
        return result

    def on_stream_message(self, mqttc, obj, msg):
        topic = msg.topic
        data_dict = json.loads(msg.payload.decode())
        timestamp, value = data_dict["Timestamp"], data_dict["Value"]
        if topic in self._topic_request_dict:
            # put the data into data store
            stream_command_list = self._topic_request_dict[topic]
            for stream_command in self._get_list_copy(stream_command_list):
                # check if we needs to send the data to client/compute system
                # TODO: make this more reliable
                request_id = stream_command.api_key + "/" + stream_command.query_id
                if self._stream_command[request_id] != QueryCommand._START: continue # skip over the non-start command

                # get the interval
                # there might be better way to do it...
                interval = stream_command.compute_command[0]["arg"][0] if stream_command.compute_command is not None else 1
                if len(stream_command.data) > 0 and stream_command.data[0][0] + interval <= timestamp:
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
            
