import requests
import boto3
import math
from datetime import datetime


def send_email(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="Baking Alert")

def get_last_cycle(s3,file_name):
    s3.download_file('last-cycle', 'last_cycle.txt', file_name)
    fo = open(file_name,"r+")
    line = fo.read(8)
    fo.close()
    return int(line)


def update_last_cycle(s3,file_name,latest_cycle):
    with open(file_name,mode="wt") as f:
        f.write(latest_cycle)
        f.flush()
        f.close()
    response = s3.upload_file(file_name, "last-cycle", "last_cycle.txt")

def lambda_handler(event,context):
    file_name = "/tmp/last_cycle.txt"
    return 0

def process(starting_cycle,save_starting_cycle,file_name):
    report = ""
    base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&limit=8000&cycle="
    resp = requests.get(base_URL + str(starting_cycle))
    data = resp.json()
    max_cycle = starting_cycle + 9;
    while (starting_cycle < max_cycle):
        for rec in data:
            if rec["type"] == "baking" and rec["round"] < 4 and rec["status"] == "future":
                print(rec)
                level_url = "https://tzkt.io/" + str(rec["level"])
                report = report + "Baking at cycle {3}, round {0}, level {1}, at {2}\n".format(rec["round"],level_url,rec["timestamp"],rec["cycle"])
        starting_cycle = starting_cycle + 1
        resp = requests.get(base_URL + str(starting_cycle) + "&limit=5000")
        data = resp.json()

    if len(report) > 0:
        print(report)
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    #send_email(report,topic_arn)
    update_last_cycle(s3,file_name,str(save_starting_cycle))

file_name = "/tmp/last_cycle.txt"
file_name = "C:\\Users\\DREWA\\Downloads\\last_cycle.txt"
s3 = boto3.client('s3')
last_cycle = int(get_last_cycle(s3,file_name))
print(last_cycle)

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
starting_cycle = int(data["cycle"])
save_starting_cycle = starting_cycle
if starting_cycle > last_cycle:
    process(starting_cycle,save_starting_cycle,file_name)
