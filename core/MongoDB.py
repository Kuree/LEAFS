from pymongo import MongoClient
from core.QueryObject import QueryObject

class MongoDBClient:
    '''
    Mongodb client for query object storage
    '''
    def __init__(self):
        '''
        Connect to the mongodb
        '''
        self.client = MongoClient("mqtt.bucknell.edu", 27017)
        self.db = self.client.queryDB

    def add_topic_stream(self, topic, request_id, stream_command):
        '''
        add or update the data to mongodb
        '''
        commands = self.get_stream_from_topic(topic)
        if not commands:
            # it's new topic
            commands.append(stream_command)
            self.db.topic.update({"topic" : topic}, {"$set": {"stream_command" : commands}})
        else:
            commands = [stream_command]
            self.db.topic.insert_one({"topic" : topic, "request_id" : request_id, "stream_command" : commands})

    def count_topic_stream(self, topic):
        return self.db.topic.find({"topic" : topic}).count()

    def get_all_topic_stream(self):
        for entry in self.db.topic.find():
            yield entry["topic"], entry["request_id"], entry["stream_command"]

    def get_stream_from_topic(self, topic):
        result = []
        for t, request_id, stream_command in self.get_all_topic_stream():
            if topic == t:
                result.append(stream_command)
        return result

    def delete_single_topic_stream(self, request_id):
        self.db.topic.remove({"request_id" : request_id})

    def delete_all_topic_stream(self, topic):
        self.db.topic.remove({"topic" : topic})

    def add_stream_command(self, request_id, command):
        self.db.stream_command.insert_one({"request_id" : request_id, "command" : command})

    def get_all_command(self):
        for entry in self.db.stream_command.find():
            yield entry["request_id"], entry["command"]

    def update_command(self, request_id, command):
        self.db.stream_command.update({"request_id" : request_id}, {"$set" : {"command" : command}})

    def delete_command(self, request_id):
        self.db.stream_command.remove({"request_id" : request_id})

if __name__ == "__main__":
    mongo = MongoDBClient()