import json, logging
from LoggingHelper import log


class QueryCommand:
    AVERAGE = "avg"
    SUM = "sum"
    MAX = "max"
    MIN = "min"
    DEV = "dev"
    ONE_MINUTE = 60
    FIVE_MINUTE = 60 * 5

    def __init__(self):
        self.commands = []

    def add_compute_command(self, type, interval = None):
        self.commands.append({"name" : type, "arg" : [interval] if interval is not None else []})

    def to_compute_obj(self):
        return self.commands

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

        self.compute = data["compute"] if "compute" in data else None

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
        if self.needs_compute():
            result["compute"] = self.compute
        return result

    def needs_compute(self):
        return self.compute is not None

    @staticmethod
    def create_query_obj(topic, start, end, persistent, id, compute = None):
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
        result.compute = None
        print("Query object created", result.to_object())
        return result