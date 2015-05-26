import requests
import json
import csv
import sqlite3

def get_data_from_opentsdb(senso_tag):
    url = "http://amm-csr2:4242/api/query" # notice this is behind Bucknell firewall
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    request_body = {
    "start": 0,
    "end": 1432561297,
    "queries": [
        {
            "aggregator": "avg",
            "metric": "Table",
            "tags": {
                "sensor_name": senso_tag,
            }
        }]
    }
    request_json = json.dumps(request_body)
    req = requests.post(url, json=request_body, headers=headers)
    return json.loads(req.text)

def write_csv(data):
    dps = data[0]["dps"]
    with open("water_level.csv", 'w') as f:
        writer = csv.writer(f)
        writer.writerow(("Timestamp", "Value"))
        for key in dps:
            value = dps[key]
            writer.writerow((int(key), float(value)))

def write_to_sqlite(raw_data):
    conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    c = conn.cursor()

    # create table
    c.execute('create table If not exists WaterLevel (time INT, sequence_number INT, value real)')
    conn.commit()

    # putting data
    dps = raw_data[0]["dps"]
    count = 0
    for key in dps:
        value = dps[key]
        c.execute('insert into WaterLevel values(?, ?, ?)', (key, count, value))
    conn.commit()


if __name__ == "__main__":
    result = get_data_from_opentsdb("WaterLevel")
    write_csv(result)
    write_to_sqlite(result)