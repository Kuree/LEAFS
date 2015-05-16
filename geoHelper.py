import sqlite3


conn = sqlite3.connect('geo.db', detect_types=sqlite3.PARSE_DECLTYPES)
c = conn.cursor()

#c.execute('create table Location (Lat real, Lon real, State text, County text)')
#conn.commit()


from geopy.geocoders import googlev3

geolocator = googlev3.GoogleV3(api_key="AIzaSyDlU14No0PJoXeD01zXREk4yXOmagudWlg")


locationDic = {}
c.execute('select * from Location')
for row in c:
    locationDic[(row[0], row[1])] = (row[2], row[3])

print(len(locationDic))

def getPlace(lat, lon):
    # url = "http://maps.googleapis.com/maps/api/geocode/json?"
    # url += "latlng=%s,%s&sensor=false" % (lat, lon)
    # v = urlopen(url).read().decode('utf-8')
    # j = json.loads(v)
    # print(lat, lon)
    # components = j['results'][0]['address_components']
    # state = town = None
    # for c in components:
    #     if "administrative_area_level_1" in c['types']:
    #         state = c['short_name']
    #     if "administrative_area_level_2" in c['types']:
    #         town = c['short_name']
    if (lat, lon) not in locationDic:
        try:
            location = geolocator.reverse(str(lat) + ", " + str(lon))[0].address
            results = location.split(", ")
            town = results[-3]
            state = results[-2].split()[0]
            locationDic[(lat, lon)] = (state, town)
            c.execute('insert into Location values(?, ?, ?, ?)', (lat, lon, state, town))
            conn.commit()
        except:
            return None
    return locationDic[(lat, lon)]


# if __name__ == "__main__":
#     print(getPlace(40.958333, -79.547222))
