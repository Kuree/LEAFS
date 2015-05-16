import paho.mqtt.publish as publish
from QueryClient import QueryClient
import json
import time
import threading

def pushData():
    def run_forever():
        count = 20
        while True:
            obj = {"Timestamp": count, "Value": 1}
            publish.single("test/test", json.dumps(obj), hostname="mqtt.bucknell.edu")

            time.sleep(0.4)
            count += 1
    t = threading.Thread(target=run_forever)
    t.start()


if __name__ == '__main__':
    pushData()
    q = QueryClient("test/test", 0, 20)
    time.sleep(9)
    print("pause the query")
    q.pause()
    time.sleep(20)
    print("start the query")
    q.start()
    time.sleep(15)
    print("stop the query")
    q.delete()
    time.sleep(20)

    exit()
