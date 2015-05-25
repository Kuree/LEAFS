"""
base : http://water.weather.gov/ahps2/rss/obs/lwbp1.rss
type : text/html
value : Minor Stage: 18 ft<br />
Minor Flow: 0 kcfs<br />
<br />
Latest Observation: 1.52 ft<br />
Observation Time: Oct  3, 2014 03:45 PM EDT<br />
<br />
language : None

"""

import requests
import html.parser
import re
import datetime
import json
import sqlite3
import xml.etree.ElementTree as ET

def process(url = "http://water.weather.gov/ahps2/rss/obs/lwbp1.rss"):
    r = requests.get(url)
    xml = r.text
    def decode_and_strip(s):
        txt = re.sub('<[^<]+?>', '', html.parser.HTMLParser().unescape(s))
        return txt.split(':', 1)[1].strip()

    data = {'Latest Observation': None,
            'Observation Time': None}
    #check if both parameters exist in the xml
    if sum([x in xml for x in data.keys()]) == len(data):
        for line in xml.split('\n'):
            for key in data.keys():
                if key in line:
                    data[key] = decode_and_strip(line)
    if data['Latest Observation'] == None or data['Observation Time'] == None:
        return None
    geo = getGeo(xml)
    if data['Latest Observation'] == 'N/A': return None
    data = convert(data)
    data["lat"] = geo[0]
    data["long"] = geo[1]
    return data


def getGeo(xml):
    lat= re.search(r'<geo:lat>(.*)</geo:lat>', xml).group(1)
    lon= re.search(r'<geo:long>(.*)</geo:long>', xml).group(1)
    return lat, lon


def cvt_observation(str):
    "strings like: 1.47 ft"
    # return 1.47
    return str.split(' ')[0]
def cvt_time(str):
    """
    strings like: Oct  7, 2014 07:45 AM EDT, sometimes timezone is the UTC offset
    like Apr  7, 2015 08:45 AM -0400
    """
    date = None
    try:
        date =  datetime.datetime.strptime(str, "%b %d, %Y %I:%M %p %Z")
    except ValueError:
        date =  datetime.datetime.strptime(str, "%b %d, %Y %I:%M %p %z")

    return int((date.replace(tzinfo=None) - datetime.datetime(1970,1,1)).total_seconds())

def convert(data):
    c = {'Latest Observation': cvt_observation,
         'Observation Time': cvt_time}
    out = {}
    for k in data.keys():
        if k in c:
            out[k] = c[k](data[k])

    return out


epoch_notz = datetime.datetime(1970, 1, 1)
epoch_tz = datetime.datetime.strptime("Jan 01, 1970 01:00 AM -0400", "%b %d, %Y %I:%M %p %z")

def convert_time(ts):
    if ts.tzinfo == None:
        epoch = epoch_notz
    else:
        epoch = epoch_tz

    return int((ts - epoch +
              datetime.timedelta(hours=4)).total_seconds())



def unconvert_time(uxtime):
    return datetime.datetime(1970,1,1) + \
            datetime.timedelta(seconds=uxtime) - \
            datetime.timedelta(hours=4)

def put(data, url = "http://amm-csr2:4242/api/put?details"):
    sample = {}
    if 'Latest Observation' in data and\
        'Observation Time' in data:
        sample = {"metric": "lwbp1",
                  "timestamp": convert_time(data['Observation Time']),
                  "value": data['Latest Observation'][0],
                  "tags": {"units": data['Latest Observation'][1]}
        }

        r = requests.post(url, data = json.dumps(sample))

        if r.status_code != requests.codes.ok:
            for key,val in r.json()['error'].iteritems():
                print("{}: {}".format(key, val))
        else:
            rslt = json.loads(r.text)
            if len(rslt['errors']) > 0:
                print('errors in pushing data')
                print(rslt)

    else:
        print ("Missing Observation Data")


process()