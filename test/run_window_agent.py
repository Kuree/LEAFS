from agent import WindowAgent
from paho.mqtt.publish import single
import time

if __name__ == "__main__":
    a = WindowAgent(is_benchmark=True) # don't have to let the agent block the thread
    a.connect()

    # a house keeping threading to control timeout speed
    while True:
        single("Query/Timeout", qos=2, hostname="mqtt.bucknell.edu")
        time.sleep(1)