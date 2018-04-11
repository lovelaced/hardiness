# converts 3-letter country codes in a GeoJSON file to their corresponding 2 letter ones

import json

with open("countries.txt", encoding='latin-1') as countries:
    countrylist = countries.readlines()
    with open("world.json") as worldjson:
       # features = worldjson.readlines()
       # print(features[0])
        stuff = json.loads(worldjson.read())
        for country in stuff["features"]:
            name = country["properties"]["name"]
            for line in countrylist:
                if name.lower() in line.lower():
                    country["id"] = line.lower().split(";")[1].upper().strip()
            if len(country["id"]) is not 2:
                print(country)

        print(stuff)

