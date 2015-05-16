from pymongo import MongoClient
from QueryObject import QueryObject

class MongoDBClient:
    def __init__(self):
        self.client = MongoClient("mqtt.bucknell.edu", 27017)
        self.db = self.client.queryDB

    def add(self, data):
        if isinstance(data, QueryObject):
            self.db.posts.update({"id": data.id}, data.to_object(), upsert=True)
        else:
             self.db.posts.update({"id": data["id"]}, data, upsert=True)

    def find_by_id(self, id):
        return self.db.posts.find_one({"id":id})

    def delete_by_id(self, id):
        self.db.posts.delete_one({"id" : int(id)})

if __name__ == "__main__":
    mongo = MongoDBClient()