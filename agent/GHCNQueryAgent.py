import sqlite3
import time
import random
import ulmo
import pandas
import datetime

try:
    from agent.QueryAgent import QueryAgent
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from agent.QueryAgent import QueryAgent


class GHCNQueryAgent(QueryAgent):
    """
    A simple SQLite adapter to handle in coming queries
    """

    def __init__(self, block_current_thread = False):
        super().__init__("GHCN", block_current_thread)


    def _query_data(self, topic, start, end):
        result = []
        start_time = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
        data = ulmo.ncdc.ghcn_daily.get_data('GM000010147', as_dataframe=True)
        tm = data['TMAX'].copy()
        tm.value=tm.value/10.0
        for timestamp, value in tm['value'][start:end].iteritems():
            t = time.mktime(datetime.datetime.strptime(str(timestamp), "%Y-%m-%d").timetuple())
            result.append((t, 0, float(value))) # use 0 for sequence number because it's lagecy database
        return result


        


if __name__ == "__main__":
    a = GHCNQueryAgent(True)
    a.initialize_database()
    a.connect()

