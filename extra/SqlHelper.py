import sqlite3
import datetime


conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
c = conn.cursor()


def putData(sensorID, timestamp, value):
    '''
    Prototyping simple database. Will move to Cassandra or mongodb
    '''
    print(sensorID, timestamp, value)
    c.execute('insert into WaterLevel values(?, ?, ?)', (sensorID, timestamp, value))
    conn.commit()


def queryData(dbName, sensor, start, end):
    '''
    Prototyping simple database. Will move to Cassandra or mongodb
    '''
    query = (dbName, sensor, start, end)
    c.execute('select * from ? where SensorName=? and time >= ? and time <= ?', query)
    result = []
    for row in c:
        t = (row[1], row[2])
        result.append(t)
    return result


def createTable():
    c.execute('create table If not exists WaterLevel (SensorName text, time INT, value real)')
    conn.commit()

def test():
    sensorID = "test"
    timestamp = datetime.datetime.now()
    value = 1
    putData(sensorID, timestamp, value)
    print(queryData(sensorID))

if __name__ == "__main__":
    createTable()
    test()