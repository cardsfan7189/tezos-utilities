import requests
import boto3
import math
from datetime import datetime


def check_for_baking(data,base_URL,starting_cycle):
    while data:
        #print(data)
        for rec in data:

            if rec["type"] == "baking":
                print(rec)
        starting_cycle = starting_cycle + 1
        resp = requests.get(base_URL + str(starting_cycle))
        data = resp.json()

def check_future_schedule(data,base_URL,starting_cycle):
    max_cycle = starting_cycle + 7
    while (starting_cycle < max_cycle):
        for rec in data:
            print(rec)
        starting_cycle = starting_cycle + 1
        resp = requests.get(base_URL + str(starting_cycle))
        data = resp.json()

def send_email(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="Tracking baker")

def get_last_level(s3,file_name):
    s3.download_file('last-level', 'last_level.txt', file_name)
    fo = open(file_name,"r+")
    line = fo.read(8)
    fo.close()
    return int(line)

def update_last_level(s3,file_name,latest_level):
    with open(file_name,mode="wt") as f:
        f.write(latest_level)
        f.flush()
        f.close()

    response = s3.upload_file(file_name, "last-level", "last_level.txt")
    print(response)

def get_percentage_for_cycle(starting_cycle,latest_cycle):
    response = ""
    cycle = starting_cycle
    base_URL = "https://api.tzkt.io/v1/rights?type=endorsing&baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle="
    while cycle <= latest_cycle:
        resp = requests.get(base_URL + str(cycle) + "&limit=8000")
        data = resp.json()
        missed_slots = 0
        endorsed_slots = 0
        total_slots = 0
        for rec in data:
            if rec["status"] == "realized":
                endorsed_slots = endorsed_slots + int(rec["slots"])
            elif rec["status"] == "missed":
                missed_slots = missed_slots + int(rec["slots"])
        total_slots = missed_slots + endorsed_slots
        response = response + "\nCycle {0}, Total slots: {1}, endorsed slots: {2}, missed slots: {3}, " \
                   "reliability {4}".format(cycle,total_slots,endorsed_slots,missed_slots,endorsed_slots/total_slots)
        cycle = cycle + 1

    return response

def lambda_handler(event,context):
    file_name = "/tmp/last_level.txt"
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    missed_slots = 0
    endorsed_slots = 0
    endorsing_realized = 0
    endorsing_missed = 0
    blocks_baked = 0
    blocks_missed = 0
    #resp = requests.get("https://api.tzkt.io/v1/head")
    #data = resp.json()
    #print(data)
    s3 = boto3.client('s3')
    last_level = get_last_level(s3,file_name)
    starting_level = last_level + 1


#starting_level = int(data["level"]) - 360

    #starting_level = 2328168
    base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&limit=8000&level="
    #base_URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1cwwjwgQJSvAq8ZMqerJVxU6qZGuYD4357&limit=8000&level="
    #base_URL = "https://api.tzkt.io/v1/rights?baker=tz1fbumHTEhLMBmtT1GjagbMnhnTD5YZAJh2&limit=8000&level="
    future_flag = False
    first_record = True
    report = "empty"
    latest_level = 0
    latest_timestamp = None

    while future_flag == False:
        resp = requests.get(base_URL + str(starting_level))
        data = resp.json()
        if data:
            rec = data[0]
            #print(rec)
            if first_record:
                first_record = False
                report = "Starting level: {0}, timestamp: {1}\n".format(rec["level"],rec["timestamp"])

            if rec["type"] == "baking" and rec["status"] == "realized":
                blocks_baked = blocks_baked + 1
            elif rec["type"] == "baking" and rec["status"] == "missed":
                blocks_missed = blocks_missed + 1
            elif rec["status"] == "realized":
                endorsed_slots = endorsed_slots + int(rec["slots"])
                endorsing_realized = endorsing_realized + 1
            elif rec["status"] == "missed":
                missed_slots = missed_slots + int(rec["slots"])
                endorsing_missed = endorsing_missed + 1
            elif rec["status"] == "future":
                future_flag = True;

            if future_flag == False:
                latest_level = rec["level"]
                latest_timestamp = rec["timestamp"]

        starting_level = starting_level + 1

    if latest_level == 0:
        latest_level = last_level
    else:
        update_last_level(s3,file_name,str(latest_level))

    report = report + "Last level: {0}, timestamp: {1}\n".format(latest_level,latest_timestamp)

    update_last_level(s3,file_name,str(latest_level))

    report = report + "endorsed slots: {0}, endorsing realized: {1}\n".format(endorsed_slots,endorsing_realized)
    report = report + "missed slots: {0}, endorsing missed: {1}\n".format(missed_slots,endorsing_missed)
    report = report + "baked blocks {0}\n".format(blocks_baked)
    report = report + "missed blocks: {0}".format(blocks_missed)
    report = report + get_percentage_for_cycle(starting_cycle,latest_cycle)

    print(report)
    send_email(report,topic_arn)

file_name = "C:\\Users\\drewa\\Downloads\\last_level.txt"
topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
missed_slots = 0
endorsed_slots = 0
endorsing_realized = 0
endorsing_missed = 0
blocks_baked = 0
blocks_missed = 0
#resp = requests.get("https://api.tzkt.io/v1/head")
#data = resp.json()
#print(data)

s3 = boto3.client('s3')
last_level = get_last_level(s3,file_name)
starting_level = last_level + 1


#starting_level = 2328168
base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&limit=8000&level="
#base_URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1cwwjwgQJSvAq8ZMqerJVxU6qZGuYD4357&limit=8000&level="
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1fbumHTEhLMBmtT1GjagbMnhnTD5YZAJh2&limit=8000&level="
future_flag = False
first_record = True
report = "empty"
latest_level = 0
latest_timestamp = None

while future_flag == False:
    resp = requests.get(base_URL + str(starting_level))
    data = resp.json()

    if data:
        rec = data[0]
        #print(rec)
        if first_record:
            first_record = False
            starting_cycle = rec["cycle"]
            report = "Starting level: {0}, timestamp: {1}\n".format(rec["level"],rec["timestamp"])

        if rec["type"] == "baking" and rec["status"] == "realized":
           blocks_baked = blocks_baked + 1
        elif rec["type"] == "baking" and rec["status"] == "missed":
           blocks_missed = blocks_missed + 1
        elif rec["status"] == "realized":
           endorsed_slots = endorsed_slots + int(rec["slots"])
           endorsing_realized = endorsing_realized + 1
        elif rec["status"] == "missed":
           missed_slots = missed_slots + int(rec["slots"])
           endorsing_missed = endorsing_missed + 1
        elif rec["status"] == "future":
           future_flag = True;

        if future_flag == False:
            latest_level = rec["level"]
            latest_cycle = rec["cycle"]
            latest_timestamp = rec["timestamp"]

    starting_level = starting_level + 1

if latest_level == 0:
    latest_level = last_level
else:
    update_last_level(s3,file_name,str(latest_level))


report = report + "Last level: {0}, timestamp: {1}\n".format(latest_level,latest_timestamp)


report = report + "endorsed slots: {0}, endorsing realized: {1}\n".format(endorsed_slots,endorsing_realized)
report = report + "missed slots: {0}, endorsing missed: {1}\n".format(missed_slots,endorsing_missed)
report = report + "baked blocks {0}\n".format(blocks_baked)
report = report + "missed blocks: {0}\n\n".format(blocks_missed)
report = report + get_percentage_for_cycle(starting_cycle,latest_cycle)
print(report)
send_email(report,topic_arn)
