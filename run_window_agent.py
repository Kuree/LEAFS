from agent import WindowAgent
from paho.mqtt.publish import single


if __name__ == "__main__":
    a = WindowAgent() # don't have to let the agent block the thread
    a.connect()

    # a house keeping threading to control timeout speed
    single("Query/Timeout", qos=2)