from test.benchmark_query import benchmark_query
from agent import SQLAgent, ComputeAgent
import time


if __name__ == "__main__":
    s = SQLAgent()
    c = ComputeAgent()
    s.connect()
    c.connect()
    time.sleep(0.2) # wait till the server starts

    benchmark = benchmark_query("Building")

    mqtt_time = benchmark.mqtt_operation()

    qure_sql_time = benchmark.sql_operation()

    in_memory_time = benchmark.in_memory_operation()

    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)

    benchmark = benchmark_query("WaterLevel", start=0, end=time.time())

    mqtt_time = benchmark.mqtt_operation()

    qure_sql_time = benchmark.sql_operation()

    in_memory_time = benchmark.in_memory_operation()

    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)