from paho.mqtt.client import Client
import time, json, logging

class LoggingAgent:

    _LOG_TOPIC_STRING = ["Query/Log", "+/Query/+/+/+"]

    def __init__(self, block_current_thread = False, hostname = "mqtt.bucknell.edu", logger_config = None):
        self._log_sub = Client()
        self._log_sub.on_message = self.on_log_message
        self._log_sub.connect(hostname)
        for topic_string in LoggingAgent._LOG_TOPIC_STRING:
            self._log_sub.subscribe(topic_string)
        
        # configure the logging
        if logger_config is None:
            logging.basicConfig(format="%(asctime)s %(message)")
        else:
            logging.basicConfig(logger_config)

        self.log_funct = {logging.DEBUG: logging.debug, logging.INFO: logging.info, logging.WARN: logging.warn, logging.ERROR: logging.error}

        self.block_current_thread = block_current_thread

    def on_log_message(self, mqttc, obj, msg):
        if msg.topic != LoggingAgent._LOG_TOPIC_STRING[0]:
            # traffic log
            logging.info("Topic: {0}, Payload: {1}".format(msg.topic, msg.payload.decode()))
        else:
            logging_data = json.loads(msg.payload.decode())
            level = logging_data[0]
            message = logging_data[1]
            self.log_funct[level](message)

    def connect(self):
        self._log_sub.loop_start()

        if self.block_current_thread:
            while True:
                time.sleep(100)

if __name__ == "__main__":
    a = LoggingAgent(True)
    a.connect()