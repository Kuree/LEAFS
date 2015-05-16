from pymongo import MongoClient
from QueryObject import QueryObject

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

    def add(self, data):
        '''
            add or update the data to mongodb
        '''
        if isinstance(data, QueryObject):
            self.db.query.update({"id": data.id}, data.to_object(), upsert=True)
        else:
             self.db.query.update({"id": data["id"]}, data, upsert=True)

    def find_by_id(self, id):
        '''
            Return the document from given query id
        '''
        return self.db.query.find_one({"id":id})

    def delete_by_id(self, id):
        '''
           Delete the query  from given query id
        '''
        self.db.query.delete_one({"id" : int(id)})

if __name__ == "__main__":
    mongo = MongoDBClient()