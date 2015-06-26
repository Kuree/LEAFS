import sqlite3
import time
import random
try:
    from agent.QueryAgent import QueryAgent
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from core import msgEncode
    from agent.QueryAgent import QueryAgent


class SQLAgent(QueryAgent):
    """
    A simple SQLite adapter to handle in coming queries
    """

    def __init__(self, block_current_thread = False):
        # initialize the sqlite connection
        self.connection = sqlite3.connect('test/dummy_data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.connection.cursor()

        super().__init__("SQLite", block_current_thread)

    def _query_data(self, topic, start, end):
        if start > 14352613310:
            start = start // 1000
            end = end // 1000 
        self.cursor.execute('select * from test where topic = ? and timestamp between ? and ?', (topic, start, end,))
        result = []
        for row in self.cursor:
            t = (row[1], row[2], row[3])
            result.append(t)
        return result


    def initialize_database(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
        table_list = self.cursor.fetchall()
        if len(list(table_list)) == 1:
            return
        self.cursor.execute("CREATE TABLE IF NOT EXISTS test (topic text, timestamp real, sequence_number integer, value real)")
        # insert dummy data
        start_time = int(time.time() - 3000000)
        end_time = int(time.time() + 3000000)
        count = 0
        topic_list = ["test/test/1", "test/test/2"]
        for i in range(start_time, end_time, 900):
            for topic in topic_list:
                self.cursor.execute("INSERT INTO test VALUES(?, ?, ?, ?)", (topic, i, count, random.uniform(0, 10),))
        self.connection.commit()
        


if __name__ == "__main__":
    a = SQLAgent(True)
    a.initialize_database()
    a.connect()
