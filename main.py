import os
import pygeohash
import pprint
import time
import datetime
from influxdb import InfluxDBClient
from statistics import mean

pp = pprint.PrettyPrinter(indent=4)
DIR = "/home/leaf/Downloads/noaa/2017/2017out/"

def send_to_influx(data):
    for station in data:
        metrics = {}
        metrics['measurement'] = "min_avg_temp"
        metrics['tags'] = {}
        metrics['fields'] = {}
        metrics['tags']['station_name'] = data[station]["STATION NAME"]
        metrics['tags']['geohash'] = data[station]["GEOHASH"]
        metrics['tags']['country'] = data[station]["CTRY"]
        metrics['tags']['usaf_code'] = data[station]['USAF']
        if data[station]["ST"]:
            metrics['tags']['state'] = data[station]["ST"]
        for year in data[station]["TEMPS"].keys():
            metrics['time'] = time.mktime(datetime.datetime.strptime(year, "%Y").timetuple())
            metrics['fields']["value"] = data[station]["TEMPS"][year]
      #  print(data[station]["TEMPS"].keys())
        print(metrics)

def get_station_info(usaf_num):
    station_dict = {}
    station_info = []
    with open(DIR+"supportfiles/isd-history.txt") as support_file:
        for line in support_file:
            if line.startswith("USAF"):
                col_headers = divide_station_line(line)
            if line.startswith(usaf_num):
                station_info = divide_station_line(line)
        info_i = 0
        for column in col_headers:
            station_dict[column] = station_info[info_i]
            info_i += 1
    return station_dict

def divide_station_line(line):
    line_entry = []
    line_entry.append(line[0:7])
    line_entry.append(line[7:13])
    line_entry.append(line[13:43])
    line_entry.append(line[43:48])
    line_entry.append(line[48:51])
    line_entry.append(line[51:57])
    line_entry.append(line[57:65])
    line_entry.append(line[65:74])
    line_entry.append(line[74:82])
    line_entry.append(line[82:91])
    line_entry.append(line[91:94])
    for i in range(0, len(line_entry)-1):
        line_entry[i] = line_entry[i].strip()
    return line_entry

info_list = []
for filename in os.listdir(DIR):
    print("NEW FILE!!!")
    if os.path.isdir(DIR + filename):
       # print(filename, "is a directory, skipping...")
        continue
    with open(DIR + filename) as file:
        data = file.readline()
        all_temps = []
        while(data):
            data = file.readline()
         #   print(data)
            year_s = data[13:17]
            month_s = data[17:19]
            day_s = data[19:21]

            year = data[13:17]
            month = data[17:19]
            day = data[19:21]
            day_temps = []
            while year == year_s and month == month_s and day == day_s:
             #   print(data)
                #print(len(data))
                temp = "not an int"

                try:
                    temp = int(data[83:88], 16)
                except ValueError:
                    data = file.readline()
                    if not data:
                        break
                    year = data[13:17]
                    month = data[17:19]
                    day = data[19:21]


                if type(temp) is int:
                    day_temps.append(temp)



                data = file.readline()
                if not data:
                    break

                year = data[13:17]
                month = data[17:19]
                day = data[19:21]
            day_temps.sort()
            coldest_day_temps = day_temps[0:3]
          #  print(coldest_day_temps)
            if len(coldest_day_temps) < 3:
                continue
            try:
                all_temps.append(mean(coldest_day_temps))
            except:
                continue
            all_temps.sort()
        if not all_temps:
            continue
        min_avg_temp = mean(all_temps[0:7])
            #print(min(month_temps))

        station_info = filename.strip('.out').split('-')
        station_info.append(min_avg_temp)
        print(station_info)
        if not info_list:
            info_list.append({station_info[0]: {"TEMPS": {station_info[2]: station_info[3]}}})
        for station_dict in info_list:
            if station_info[0] in station_dict.keys():
                station_dict[station_info[0]]["TEMPS"][station_info[2]] = station_info[3]
            else:
                info_list.append({station_info[0]: {"TEMPS": {station_info[2]: station_info[3]}}})
     #   print(info_list)

infodict = {}
tempdict = {}
for station in info_list:
    usaf = station.keys()
    for key in usaf:
        tempdict = get_station_info(key)
        lat = None
        lon = None
        if tempdict["LAT"]:
            tempdict["LAT"] = float(tempdict["LAT"][1:])
            lat = tempdict["LAT"]
        if tempdict["LON"]:
            tempdict["LON"] = float(tempdict["LON"][1:])
            lon = tempdict["LON"]
        if tempdict["ELEV(M)"]:
            tempdict["ELEV(M)"] = float(tempdict["ELEV(M)"][1:])
        if lat and lon:
            geohash = pygeohash.encode(lat, lon)
            tempdict["GEOHASH"] = geohash
        for k, v in station[key].items():
            tempdict[k] = v
        infodict[key] = tempdict
pp.pprint(infodict)

send_to_influx(infodict)
