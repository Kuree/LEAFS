from paho.mqtt.client import Client
import time, json, logging

class LoggingAgent:

    _LOG_TOPIC_STRING = "Query/Log"

    def __init__(self, blocking = False, hostname = "mqtt.bucknell.edu", logger_config = None):
        self._log_sub = Client()
        self._log_sub.on_message = self.on_log_message
        self._log_sub.connect(hostname)
        self._log_sub.subscribe(LoggingAgent._LOG_TOPIC_STRING)
        
        self._log_sub.loop_start()

        # configure the logging
        if logger_config is None:
            logging.basicConfig(format="%(asctime)s %(message)")
        else:
            logging.basicConfig(logger_config)

        self.log_funct = {logging.DEBUG: logging.debug, logging.INFO: logging.info, logging.WARN: logging.warn, logging.ERROR: logging.error}

        if blocking:
            while True:
                time.sleep(100)

    def on_log_message(self, mqttc, obj, msg):
        logging_data = json.loads(msg.payload.decode())
        level = logging_data[0]
        message = logging_data[1]
        self.log_funct[level](message)


if __name__ == "__main__":
    a = LoggingAgent(True)