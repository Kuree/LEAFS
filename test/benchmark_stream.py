import paho.mqtt.publish as publish
from client.QueryClient import QueryClient
import time
import threading
from SqlHelper import putData
from core import ComputeCommand
from core import msgEncode

should_stop = False

def push_data_forever(maximum):
    count = 0
    while count < maximum:
        obj = (count, count - 20, count)
        publish.single("test/test/stream", msgEncode.encode(obj), hostname="mqtt.bucknell.edu")
        count += 1

def push_data_into_sql():
    for i in range(100):
        putData("test/test/1", i, i)

if __name__ == '__main__':
    t = threading.Thread(target=push_data_forever, args=(10, ))
    t.start()
    compute = ComputeCommand()
    compute.add_compute_command(ComputeCommand.AVERAGE, 5)
    q = QueryClient("benchmark_stream")
    q.add_query("SQL", "test/test/stream", 0, 20,True, compute.to_obj())
    q.on_message = lambda a, b : print(msgEncode.decode(b))
    q.connect()
    time.sleep(3)
    q.pause()
    print("pause")
    time.sleep(1)
    q.start()
    print("start")
    time.sleep(3)
    #q.delete()
    #print("delete")
    time.sleep(3)