# This is YANDEX SDC Distance Report
# By Alex Ekimenko 28-05-2019

# usage:
#    distance.py http://url-to-data-file

# default url: https://sdcimages.s3.yandex.net/test_task/data


import json
import math
import sys

#
# functions
#

def downloadFile(URL=None):
    import requests
    req = requests.get(URL)
    return req

# упорядочены ли значения по времени?
def isSorted(e):
    ordered = 0
    result = True
    last_ts = e[0]["ts"]
    for item in e:
        if (item["ts"] < last_ts):
            ordered = ordered + 1
        last_ts = item["ts"]
    if ordered > 0: result = False
    return result

# функция для сортировки элементов списка по ts
def sortByTS(e):
  return e["ts"]

def getDistance(lon1, lon2, lat1, lat2):
    #Только для близких расстояний!
    KM_IN_GRAD_LON = lat_2_km(lat1)
    KM_IN_GRAD_LAT = 111
    distance = math.sqrt(((lon1-lon2)*KM_IN_GRAD_LON) ** 2 + ((lat1-lat2)*KM_IN_GRAD_LAT) ** 2)
    return distance

def lat_2_km(lat):
    return 6371 * (math.pi/180)*math.cos(lat*math.pi/180)



#
# main pipeline:
#

# get data

data_url = 'https://sdcimages.s3.yandex.net/test_task/data'
if (len(sys.argv) == 2): data_url = sys.argv[1]
print('\nDistance Couner Yandex SDC: ')
print("---------------------------")
print("Get data from this url: ", data_url)

try:
    handle = downloadFile(data_url)
    data = handle.text.splitlines()
except:
    print("Error opening URL!")
    sys.exit()

try:
    elements = []
    for line in data:
        elements.append(json.loads(line))
except:
    print("Error converting JSON!")
    sys.exit()

# если isSorted != True => нужно отсортировать по timestamp
# расстояние будем считать между соседними точками и складывать для каждого отрезка
# упростим перевод расстояния, примем допущение, что в Лас Вегасе Земля плоская
# => будем считать Евклидово расстояние (кривизной поверхности пренебрегаем)

if (isSorted(elements) == False):
    print("Datafile is not sorted by timestamp!")
    print("Sorting ...")
    elements.sort(key=sortByTS)
    print("Is now sorted? ", isSorted(elements))


last_lon = 0
last_lat = 0
current_dist = 0
current_mode = "unknown"
path_id = 1

paths = dict()
for item in elements:
    if "control_switch_on" in item.keys():
        if ((item["control_switch_on"] == True) & (current_mode != "auto")):
            # переключаем трэк
            path_id = path_id + 1
            current_mode = "auto"
            current_dist = 0
        if ((item["control_switch_on"] == False) & (current_mode != "manual")):
            # переключаем трэк
            path_id = path_id + 1
            current_mode = "manual"
            current_dist = 0
    if "geo" in item.keys():
        if (item["geo"]["lon"] != 0) & (item["geo"]["lat"] != 0):
            #just miss incorrect lat & lon
            if (last_lon != 0) & (last_lat != 0):
                # не первый раз получаем координаты
                # считаем расстояние от предыдущей точки
                current_dist = current_dist + getDistance(last_lon, item["geo"]["lon"], last_lat, item["geo"]["lat"])
            # обновляем запись о расстоянии
            paths[path_id] = {'mode': current_mode, 'distance': current_dist}
            last_lon = item["geo"]["lon"]
            last_lat = item["geo"]["lat"]

auto_distance = 0
manual_distance = 0
unknown_distance = 0

print("\nPaths extracted:")
print('No.\tDistance[km]\tMode')
print('-----------------------------')

for i in range(1,path_id+1):
    # print(paths[i])
    print(i,'\t', round(paths[i]["distance"], 3), '\t\t', paths[i]["mode"])
    if (paths[i]["mode"] == "auto"): auto_distance = auto_distance + paths[i]["distance"]
    if (paths[i]["mode"] == "manual"): manual_distance = manual_distance + paths[i]["distance"]
    if (paths[i]["mode"] == "unknown"): unknown_distance = unknown_distance + paths[i]["distance"]

print("\nTotal:")
print('No.\tDistance[km]\tMode')
print('-----------------------------')
print(1,'\t',round(auto_distance, 3),'\t\t','Auto')
print(2,'\t',round(manual_distance, 3),'\t\t','Manual')
print(3,'\t',round(unknown_distance, 3),'\t\t','Unknown')