import pytz
import requests
import math
from datetime import datetime
import time
from calendar import timegm
from pytz import timezone
import os
import boto3

def lambda_handler(event,context):
    print("about to end lambda handler")
    return 0
def open_interval_found(prev_cal_time,cal_time,interval):
    utc_time = time.strptime(prev_cal_time,'%Y-%m-%dT%H:%M:%SZ')
    prev_epoch_time = timegm(utc_time)
    utc_time = time.strptime(cal_time,'%Y-%m-%dT%H:%M:%SZ')
    epoch_time = timegm(utc_time)

    if epoch_time - prev_epoch_time >= interval:
        local_time = datetime.fromtimestamp(prev_epoch_time).astimezone(pytz.timezone('US/Central'))
        return local_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        return None

s3 = boto3.client('s3')
report_file_path = os.getenv("REPORT_FILE_PATH")
interval = int(os.getenv("INTERVAL_SECS"))
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
curr_cycle = data["cycle"]

resp = requests.get("https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + str(curr_cycle) + "&limit=10000&status=future")

report = "<HTML><BODY><PRE>\n"

data = resp.json()
cal_time_prev = None
for rec in data:
    #print(rec)
    cal_time = rec["timestamp"]
    if cal_time_prev:
        interval_time = open_interval_found(cal_time_prev,cal_time,interval)
        if interval_time:
            print(interval_time)
            report += "{0} second window found at {1} central time\n".format(interval,interval_time)
    cal_time_prev = cal_time

report += "</PRE></BODY></HTML>"
#print(report)
with open(report_file_path,"w") as outfile:
    outfile.write(report)
    outfile.flush()
    outfile.close()
s3.upload_file(report_file_path, "teznebraska", "times_to_fund_payout.html",
               ExtraArgs={'ContentType':'text/html'})