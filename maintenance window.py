import requests
import boto3
import math
from datetime import datetime
import time
from calendar import timegm


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

def check_the_next_hour(epoch,thisdict):
    max_epoch = epoch + 3600
    count = 0
    while epoch < max_epoch + 1:
        if epoch in thisdict.keys():
            if thisdict[epoch] == "endorsing":
                count = count + 1
            else:
                return -1
        epoch = epoch + 1

    return count

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message)

def lambda_handler(event,context):
    topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
    #resp = requests.get("https://api.tzkt.io/v1/head")
    #data = resp.json()
    #print(data)
    s3 = boto3.client('s3')
    thisdict = {}
    msg = None
    base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&status=future&limit=600"
    resp = requests.get(base_URL )
    data = resp.json()
    for rec in data:
        utc_time = time.strptime(rec["timestamp"],'%Y-%m-%dT%H:%M:%SZ')
        epoch_time = timegm(utc_time)
        thisdict[epoch_time] = rec["type"]
    last_epoch_time = epoch_time
    for i in thisdict:
        #print(thisdict[i] + "," + str(i))
        if thisdict[i] == "baking":
            continue
        elif (i + 3600) > last_epoch_time:
            break
        else:
            result = check_the_next_hour(i,thisdict)
            if result < 4 and result > -1:
                print(i)
                time_formatted = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(i))
                temp_msg = "One hour maintenance period found at " + time_formatted
                print(temp_msg)
                if msg:
                    msg = msg + "\n" + temp_msg
                else:
                    msg = temp_msg
    if msg:
        send_msg(msg,topic_arn)

lambda_handler(None,None)