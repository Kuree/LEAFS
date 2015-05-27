from paho.mqtt.publish import single
from paho.mqtt.client import Client
import sqlite3
from functools import reduce
import time
from client import QueryClient
from core import ComputeCommand


class benchmark_query:
    def __init__(self, name, start = 1432400000, end = 1432561297):
        self.start = start
        self.end = end
        self.interval = 1000
        
        self.name = name

        self._temp_end = 0

    def run(self):
        pass

    def _on_message(self, a, b):
        self._temp_end = time.time()

    def mqtt_operation(self):
        start = time.time()
        client = QueryClient("query_test")
        compute = ComputeCommand()
        compute.add_compute_command(ComputeCommand.AVERAGE, self.interval)
        client.add_query("SQL", self.name, self.start, self.end, compute = compute.to_obj())

        client.on_message = self._on_message

        client.connect()

        while self._temp_end == 0:
            time.sleep(0.5)

        return self._temp_end - start

    def sql_operation(self):
        start = time.time()
        conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        c = conn.cursor()
        query = (self.start, self.end)
        c.execute("SELECT AVG(time), ROUND(AVG(sequence_number)), AVG(value) from " + self.name +" where time >= ? and time <= ? \
        GROUP BY ROUND(time/" + str(self.interval) + ")", query)
        data_points = []
        for row in c:
            data_points.append((row[0], row[1], row[2]))
        end =  time.time()
        conn.close()
        return end - start

    def in_memory_operation(self):
        start  = time.time()
        conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        c = conn.cursor()
        query = (self.start, self.end)
        c.execute('select * from Building where time >= ? and time <= ?', query)
        data_points = []    
        count = 0
        for row in c.fetchall():
            data_points.append((float(row[0]), count, float(row[1])))
        chunks = benchmark_query.split_into_chunk(data_points, self.interval)
        def get_middle(chunk, index):
            return (chunk[-1][index] - chunk[0][index]) // 2

        def avg(chunk):
            return sum([x[2] for x in chunk]) / len(chunk)

        result =  [(get_middle(chunk, 0), get_middle(chunk, 1), avg(chunk)) for chunk in chunks]
        end = time.time()
        conn.close()
        return end - start

    @staticmethod
    def split_into_chunk(data, interval):
        result = []
        chunk = []
        pre_timestamp = data[0][0] # the first time stamp in a chunk
        for data_point in data:
            timestamp = data_point[0]
            value = data_point[1]
            if timestamp < pre_timestamp + interval:
                chunk.append(data_point)
            else:
                result.append(chunk)
                chunk = [data_point]
                pre_timestamp = timestamp
        result.append(chunk)
        return result
