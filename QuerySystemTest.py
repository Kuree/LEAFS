import paho.mqtt.publish as publish
from QueryClient import QueryClient
import json
import time
import threading

should_stop = False

def push_data_forever():
    count = 20
    while not should_stop:
        obj = {"Timestamp": count, "Value": 1}
        publish.single("test/test", json.dumps(obj), hostname="mqtt.bucknell.edu")
        time.sleep(0.2)
        count += 1



if __name__ == '__main__':
    t = threading.Thread(target=push_data_forever)
    t.start()
    q = QueryClient("test/test", 0, 20)
    time.sleep(2)
    print("pause the query")
    q.pause()
    time.sleep(2)
    print("start the query")
    q.start()
    time.sleep(2)
    print("stop the query")
    q.delete()
    time.sleep(2)

    should_stop = True
    print("stop testing")
    exit(1)
