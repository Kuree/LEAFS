import requests
import json
import csv
import sqlite3

def get_data_from_opentsdb(senso_tag):
    url = "http://amm-csr2:4242/api/query" # notice this is behind Bucknell firewall
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    request_body = {
    "start": 1431500000,
    "end": 1432561297,
    "queries": [
        {
            "aggregator": "avg",
            "metric": "wattnode_187669",
            "tags": {
                "parameter": senso_tag,
            }
        }]
    }
    request_json = json.dumps(request_body)
    req = requests.post(url, json=request_body, headers=headers)
    return json.loads(req.text)


def write_to_sqlite(raw_data):
    conn = sqlite3.connect('data.db', detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
    c = conn.cursor()

    # create table
    c.execute('create table If not exists Building (time INT, sequence_number INT, value real)')
    conn.commit()

    # putting data
    dps = raw_data[0]["dps"]
    count = 0
    for key in dps:
        value = dps[key]
        c.execute('insert into Building values(?, ?, ?)', (key, count, value))
        count += 1
    conn.commit()

def write_csv(data):
    dps = data[0]["dps"]
    with open("building.csv", 'w') as f:
        writer = csv.writer(f)
        writer.writerow(("Timestamp", "Value"))
        for key in dps:
            value = dps[key]
            writer.writerow((int(key), float(value)))

if __name__ == "__main__":
    result = get_data_from_opentsdb("PowerSum")
    write_csv(result)
    write_to_sqlite(result)
