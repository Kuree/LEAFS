import paho.mqtt.publish as publish
from QueryClient import QueryClient
import json
import time
import threading
from SqlHelper import putData
from QueryObject import ComputeCommand

should_stop = False

def push_data_forever():
    count = 20
    while not should_stop:
        obj = {"Timestamp": count, "Value": 1}
        publish.single("test/test/1", json.dumps(obj), hostname="mqtt.bucknell.edu")
        time.sleep(0.1)
        count += 1

def push_data_into_sql():
    for i in range(100):
        putData("test/test/1", i, i)

if __name__ == '__main__':
    t = threading.Thread(target=push_data_forever)
    t.start()
    #q = QueryClient("test/test/1", 0, 20, "test", "SQL", True)
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
    #exit(0)
    compute = ComputeCommand()
    compute.add_compute_command(ComputeCommand.AVERAGE, 5)
    q = QueryClient("test")
    q.add_query("SQL", "test/test/1", 0, 20,True, compute.to_obj())
    q.on_message = lambda a, b : print(b)
    q.connect()
    time.sleep(3)
    q.pause()
    print("pause")
    time.sleep(1)
    q.start()
    print("start")
    time.sleep(3)
    q.delete()
    print("delete")
    time.sleep(3)