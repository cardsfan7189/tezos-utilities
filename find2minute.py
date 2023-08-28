import requests
import math
from datetime import datetime
import time
from calendar import timegm
import os

def open_interval_found(prev_cal_time,cal_time,interval):
    utc_time = time.strptime(prev_cal_time,'%Y-%m-%dT%H:%M:%SZ')
    prev_epoch_time = timegm(utc_time)
    utc_time = time.strptime(cal_time,'%Y-%m-%dT%H:%M:%SZ')
    epoch_time = timegm(utc_time)

    if epoch_time - prev_epoch_time >= interval:
        return True
    else:
        return False

interval = os.getenv("INTERVAL_SECS")
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
curr_cycle = data["cycle"]

resp = requests.get("https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + str(curr_cycle) + "&limit=10000&status=future")


data = resp.json()
cal_time_prev = None
print(data[0])
for rec in data:
    #print(rec)
    cal_time = rec["timestamp"]
    if cal_time_prev:
        if open_interval_found(cal_time_prev,cal_time,interval):
            print("90 second window found at {0}".format(cal_time_prev))
    cal_time_prev = cal_time


