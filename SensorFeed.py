from RssParser import process
import time
import paho.mqtt.publish as publish
from geoHelper import getPlace
import json
from urllib.request  import urlopen
import re


'''
Simple feed bot to send sensor data to the server.
It parses the web page to get the RSS link, then parse the description in RSS to get the time stamp and value
'''

def getAllPAUrl():
    result = []
    searchURL = "http://water.weather.gov/ahps/rss/gauges.php?state=pa&fcst_type=obs"
    text_value = urlopen(searchURL).read().decode('utf-8')
    pattern = r'/obs(.*).rss"'
    for match in re.findall(pattern, text_value):
        url = "http://water.weather.gov/ahps2/rss/obs" + match + ".rss"
        result.append(url)
    return result

urlList = getAllPAUrl()
lastTime = []

for i in range(len(urlList)):
    lastTime.append(0)


while True:
    for i in range(len(urlList)):
        url = urlList[i]
        # https://github.com/kennethreitz/requests/issues/1915
        # requests bug here

        try:
            returnData = process(url)
        except:
            continue
        if returnData == None or returnData["Observation Time"] == lastTime[i]:
            continue
        else:
            lastTime[i] = returnData["Observation Time"]
            cor = (returnData["lat"], returnData["long"])
            location = getPlace(cor[0], cor[1])
            if location == None:
                print("Location is none")
                continue # error processing the geo location
            sensorName = re.search(r'obs/(.*).rss', url).group(1)
            pushString = location[0] + "/" +  (location[1] if location[1] != None else  "" )+ "/" + sensorName + "/WaterLevel"
            print(pushString)
            result = {}
            result["Timestamp"] = returnData["Observation Time"]
            result["Value"] = returnData["Latest Observation"]
            publish.single(pushString, json.dumps(result), hostname = "mqtt.bucknell.edu")
    time.sleep(10)



