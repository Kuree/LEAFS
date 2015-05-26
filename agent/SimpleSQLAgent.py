import sqlite3
from agent.QueryAgent import QueryAgent


class SQLAgent(QueryAgent):
    """
    A simple SQLite adapter to handle in coming queries
    """

    def __init__(self, block_current_thread = False):
        # initialize the sqlite connection
        self.connection = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        self.cursor = self.connection.cursor()

        super().__init__("SQL", block_current_thread)

    def _query_data(self, topic, start, end):
        query = (start, end)
        self.cursor.execute('select * from ' + str(topic) + ' where time >= ? and time <= ?', (start, end,))
        result = []
        for row in self.cursor:
            t = (row[0], row[1], row[2])
            result.append(t)
        return result
