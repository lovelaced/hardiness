import os
import pygeohash
import pprint
import time
import datetime
from influxdb import InfluxDBClient
from statistics import mean

client = InfluxDBClient(host='127.0.0.1', port=8086, database='noaa')
pp = pprint.PrettyPrinter(indent=4)
DIR = "/home/leaf/Downloads/noaa/"

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
            metrics['time'] = year + "-01-01T00:00:00Z00:00"
            metrics['fields']["value"] = float(data[station]["TEMPS"][year])
        client.write_points([metrics])

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

def parse_data_date(line):
    ymd = line[13:21]
    return ymd


info_list = []
infodict = {}
for filename in os.listdir(DIR+"2017/2017out/"):
    if os.path.isdir(DIR + "2017/2017out/" + filename):
       # print(filename, "is a directory, skipping...")
        continue
    with open(DIR + "2017/2017out/" + filename) as file:
        data = file.readlines()[1:]
        first = True
        prev_date = ""
        all_temps = []
        for line in data:
            date = parse_data_date(line)
            if first:
                prev_date = date
                first = False
                day_temps = []
            if date == prev_date:
                # check to see if there's a valid temperature in the line
                try:
                    temp = int(line[83:88], 16)
                except ValueError:
                    continue
                # intake another line once we got the temperature
                # check if we've reached the end of the file yet
                day_temps.append(temp)
                # sort all the temperatures from the day we've collected
            else:
                # keep the three lowest temps
                day_temps.sort()
                coldest_day_temps = day_temps[0:3]
                day_temps = []
                if any(coldest_day_temps):
                    all_temps.append(mean(coldest_day_temps))
                # add the mean day temperature to the list of all day temps
                first = True
        # sort all the year's daily temperatures
        all_temps.sort()
        # if there's not any reported temperatures, we can skip this file
        if not any(all_temps):
            break
        # get a week's worth of coldest temps
        min_avg_temp = mean(all_temps[0:7])

        station_info = filename.strip('.out').split('-')
        station_info.append(min_avg_temp)

        infodict[station_info[0]] = {"TEMPS": {station_info[2]: station_info[3]}}

#infodict = {}
tempdict = {}
for key in infodict.keys():
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
    for k, v in infodict[key].items():
        tempdict[k] = v
    infodict[key] = tempdict

send_to_influx(infodict)
