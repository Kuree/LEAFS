import paho.mqtt.publish as publish
from QueryClient import QueryClient
import json
import time
import threading
from SqlHelper import putData
from QueryObject import QueryCommand

should_stop = False

def push_data_forever():
    count = 20
    while not should_stop:
        obj = {"Timestamp": count, "Value": 1}
        publish.single("test/test", json.dumps(obj), hostname="mqtt.bucknell.edu")
        time.sleep(0.2)
        count += 1

def push_data_into_sql():
    for i in range(100):
        putData("test/test/1", i, i)

if __name__ == '__main__':
    #t = threading.Thread(target=push_data_forever)
    #t.start()
    #q = QueryClient("test/test", 0, 20)
    #time.sleep(2)
    #print("pause the query")
    #q.pause()
    #time.sleep(2)
    #print("start the query")
    #q.start()
    #time.sleep(2)
    #print("stop the query")
    #q.delete()
    #time.sleep(2)

    #should_stop = True
    #print("stop testing")
    #exit(1)
    compute = QueryCommand()
    compute.add_compute_command(QueryCommand.AVERAGE, 5)
    q = QueryClient("test/test/1", 0, 100, "test", "SQL", False, compute.to_compute_obj())
    time.sleep(5)