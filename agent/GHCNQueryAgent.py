import sqlite3
import time
import random
import pandas
import datetime
import math

try:
    from agent.QueryAgent import QueryAgent
    from core import get_data
except:
    import os, sys
    sys.path.insert(1, os.path.join(sys.path[0], '..'))
    from agent.QueryAgent import QueryAgent
    from core import get_data


class GHCNQueryAgent(QueryAgent):
    """
    A simple GHCN query agent adapter to handle in coming queries
    """

    def __init__(self, block_current_thread = False):
        QueryAgent.__init__(self, "GHCN", block_current_thread)


    def _query_data(self, topic, start, end):
        result = []
        if start > 10000000000:
            start = start / 1000
            end = end / 1000
        start_time = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S')
        station_name = topic.split("_")[0]
        # ONLY TMAX IS USED HERE
        data = get_data(station_name, ["TMAX"])
        if 'TMAX' not in data:
            return []
        tm = data['TMAX'].copy()
        tm.value=tm.value/10.0
        for timestamp, value in tm['value'][start_time:end_time].iteritems():
            t = time.mktime(datetime.datetime.strptime(str(timestamp), "%Y-%m-%d").timetuple())
            float_value = float(value)
            if not math.isnan(float_value):
                result.append((t, 0, float_value)) # use 0 for sequence number because it's lagecy database
        return result


        


if __name__ == "__main__":
    a = GHCNQueryAgent(True)
    a.connect()

