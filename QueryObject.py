import json

class QueryObject:
    def __init__(self, data, id):
        print("Query object created from string", data)
        self.command = 0 # 0 is start. Need to move it to upper level
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
        result = {}
        result["start"] = self.start
        result["topic"] = self.topic
        result["id"] = self.id
        result["command"] = self.command
        result["current"] = self.current if self.current is not None else self.end
        result['end'] = self.end
        result["persistent"] = self.persistent
        return result

    @staticmethod
    def createQueryObj(topic, start, end, persistent, id):
        result = QueryObject(None, None)
        result.start = start
        result.end = end
        result.topic = topic
        result.id = int(id)
        result.persistent = persistent
        print("Query object created", result.to_object())
        return result