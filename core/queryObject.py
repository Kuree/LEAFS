import json, logging
from core import logger


class ComputeCommand:
    AVERAGE = 0
    SUM = 1
    MAX = 2
    MIN = 3
    DEV = 4
    COUNT = 5

    ONE_MINUTE = 60
    FIVE_MINUTE = 60 * 5

    def __init__(self):
        self.commands = []

    def add_compute_command(self, type, interval = None):
        self.commands.append([type, interval if interval is not None else 0])

    def to_obj(self):
        return self.commands


class QueryCommand:
    """
        Query command class used by the query system to control the state of query stream
    """
    START = 0
    PAUSE = 1
    DELETE = 2

    def __init__(self, request_id, command):
        """
        Initialize the query command object
        request_id: query id that's used in query system. in the system it's KEY/ID
        command: indicates the state of the query object
        """
        self._request_id = request_id
        self._command = int(command)


class QueryStreamObject:
    def __init__(self, strData, api_key, query_id, timeout = 5):
        '''
        timeout is in second 
        '''
        self.api_key = api_key
        self.query_id = query_id
        self.request_id = self.api_key + "/" + str(self.query_id)
        self.timeout = timeout

        self.timeout_time = 0

        if strData is None:
            # create an empty query stream object
            self.compute_command = None
            self.topic = None
            # this tag is only used for return data topic. therefore the client can only subscribe one topic for query result
            self.db_tag = None  
            self.data = []
        else:
            raw_data = json.loads(strData) if isinstance(strData, str) else strData
            self.compute_command = raw_data["compute"]
            self.db_tag = raw_data["db-tag"]
            self.topic = raw_data["topic"]
            self.data = raw_data["data"]

    def to_object(self):
        return {"compute": self.compute_command, "db-tag" : self.db_tag, "data" : self.data, "topic" : self.topic, 
                "request_id" : self.request_id, "timeout" : self.timeout}

    @staticmethod
    def create_stream_obj(api_key, query_id, topic, db_tag, compute = None ):
        result = QueryStreamObject(None, api_key, query_id)
        result.compute_command = compute if compute is not None else None
        result.topic = topic
        result.data = []
        result.db_tag = db_tag
        return result

class QueryObject:
    def __init__(self, data, api_key, query_id):
        """
            Initialize the query object with string sent from network
            data: serialized query object
            id: unique query id
        """
        logger.log(logging.DEBUG, "Query object created from string: " + str(data))
        self.current = None
        if data is None:
            # this creates an empty object
            self.start = None
            self.end = None
            self.request_id  = None
            self.topic = None
            self.persistent = None
            self.db_tag = None
            return
        data = json.loads(data) if isinstance(data, str) else json.loads(data.decode())
        self.start = data["start"]
        self.topic = data["topic"]
        self.request_id = api_key + "/" + str(query_id)
        self.end = data["end"]
        self.persistent = data["persistent"]
        self.db_tag = data["db-tag"]

        self.compute = data["compute"] if "compute" in data else None

        # TODO: existing implementation assume user provides end

    def to_object(self):
        """
            Convert the object into dictionary object
            It contains more information that it's necessary
            But to be safe, some information is included as well
        """
        result = {}
        result["start"] = self.start
        result["topic"] = self.topic
        result["id"] = self.request_id
        result["current"] = self.current if self.current is not None else self.end
        result['end'] = self.end
        result["persistent"] = self.persistent
        result["db-tag"] = self.db_tag
        if self.needs_compute():
            result["compute"] = self.compute
        return result

    def needs_compute(self):
        return self.compute is not None

    @staticmethod
    def create_query_obj(db_tag, topic, start, end, persistent, api_key, query_id, compute = None):
        """
            A factory method to create query object
            start: epoch time stamp
            end: epoch time stamp
            persistent: set to True if streaming data is required
            id: unique query id for identification
        """
        result = QueryObject(None, None, None)
        result.db_tag = db_tag
        result.start = start
        result.end = end
        result.topic = topic
        result.request_id = api_key + "/" + str(query_id)
        result.persistent = persistent
        result.compute = None
        logger.log(logging.INFO, "Query object created: " + json.dumps(result.to_object()))
        return result