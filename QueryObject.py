import json, logging
from LoggingHelper import log


class QueryObject:
    def __init__(self, data, id):
        '''
            Initialize the query object with string sent from network
            data: serialized query object
            id: unique query id
        '''
        log(logging.DEBUG, "Query object created from string: " + str(data))
        self.current = None
        if data is None:
            # this creates an empty object
            self.start = None
            self.end = None
            self.id  = None
            self.topic = None
            self.persistent = None
            return
        data = json.loads(data) if isinstance(data, str) else json.loads(data.decode())
        self.start = data["start"]
        self.topic = data["topic"]
        self.id = int(id)
        self.end = data["end"]
        self.persistent = data["persistent"]

        # TODO: existing implementation assume user provides end

    def to_object(self):
        '''
            Convert the object into dictionary object
        '''
        result = {}
        result["start"] = self.start
        result["topic"] = self.topic
        result["id"] = self.id
        result["current"] = self.current if self.current is not None else self.end
        result['end'] = self.end
        result["persistent"] = self.persistent
        return result

    @staticmethod
    def create_query_obj(topic, start, end, persistent, id):
        '''
            A factory method to create query object
            start: epoch time stamp
            end: epoch time stamp
            persistent: set to True if streaming data is required
            id: unique query id for identification
        '''
        result = QueryObject(None, None)
        result.start = start
        result.end = end
        result.topic = topic
        result.id = int(id)
        result.persistent = persistent
        print("Query object created", result.to_object())
        return result