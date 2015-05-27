from test.benchmark_query import benchmark_query
from agent import SQLAgent, ComputeAgent
import time


if __name__ == "__main__":
    s = SQLAgent()
    c = ComputeAgent()
    s.connect()
    c.connect()
    time.sleep(0.2) # wait till the server starts

    benchmark = benchmark_query("Building", start=1432040000, end = time.time())

    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()
    
    print("Building data 100K")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)

    benchmark = benchmark_query("Building", start=1431610000, end = time.time())

    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()

    print("Building data 500K")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)

    benchmark = benchmark_query("Building", start=0, end = time.time())

    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()

    print("Building data 1000K")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)


    benchmark = benchmark_query("WaterLevel", start=1429500000, end=time.time())
    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()

    print("WaterLevel Data 100K: ")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)

    benchmark = benchmark_query("WaterLevel", start=1413734000, end=time.time())
    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()

    print("WaterLevel Data 500K: ")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)

    benchmark = benchmark_query("WaterLevel", start=0, end=time.time())
    time.sleep(0.5)
    mqtt_time = benchmark.mqtt_operation()
    time.sleep(0.5)
    qure_sql_time = benchmark.sql_operation()
    time.sleep(0.5)
    in_memory_time = benchmark.in_memory_operation()

    print("WaterLevel Data 1000K: ")
    print("MQTT Time: ", mqtt_time)
    print( "SQL Time: ", qure_sql_time)
    print("In memory: ", in_memory_time)