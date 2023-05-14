import requests
import math
from datetime import datetime
import time
from calendar import timegm

def two_minutes_found(prev_cal_time,cal_time):
    utc_time = time.strptime(prev_cal_time,'%Y-%m-%dT%H:%M:%SZ')
    prev_epoch_time = timegm(utc_time)
    utc_time = time.strptime(cal_time,'%Y-%m-%dT%H:%M:%SZ')
    epoch_time = timegm(utc_time)

    if epoch_time - prev_epoch_time >= 90:
        return True
    else:
        return False

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
curr_cycle = data["cycle"]
resp = requests.get("https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + str(curr_cycle) + "&limit=10000&status=future")
#resp = requests.get("https://api.ghostnet.tzkt.io/v1/rights?baker=tz1gjJfsjbB2ZwBfG7SiCxXUKWLvyLEEvP7U&cycle=530&limit=8000")
#   .7094 cycle 525 .7629 cycle 526 .96 cycle 527  .9958  cycle 528  .9950 cycle 529 .9945 cycle 530 .9819 cycle 531 .9834  cycle 532
#  .9932 first full cycle on vps  533  .9936  534  .9915  535 .9928 536 .9771  537 .9888 538  .9894 539 .9936 back to vps 542 .9926 543
data = resp.json()
cal_time_prev = None
for rec in data:
    #print(rec)
    cal_time = rec["timestamp"]
    if cal_time_prev:
        if two_minutes_found(cal_time_prev,cal_time):
            print("90 second window found at {0}".format(cal_time_prev))
    cal_time_prev = cal_time


