import json
import requests
import boto3
import math
import os

def lambda_handler(event,context):
    print("about to end lambda handler")
    return 0

def send_message(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="Monitor Overdelegator")

def get_last_level(s3,file_name):
    s3.download_file('monitor-overdelegators', 'last_level.txt', file_name)
    fo = open(file_name,"r+")
    line = fo.read(64)
    fo.close()
    return (line)

def update_last_level(s3,file_name,latest_level,flag, staking_balance):
    with open(file_name,mode="wt") as f:
        f.write(str(latest_level) + "," + flag + "," + str(staking_balance))
        f.flush()
        f.close()

    response = s3.upload_file(file_name, "monitor-overdelegators", "last_level.txt")
    #print(response)

def find_overdelegator(prev_level,prev_staking_balance,staking_capacity,delegators):
    #https://api.tzkt.io/v1/operations/transactions?anyof.sender.target=
    delegator_list = []
    for rec in delegators:
        #print(rec["address"])
        resp = requests.get("https://api.tzkt.io/v1/accounts/" + rec["address"] + "/operations?type=delegation,transaction&limit=1000")
        operations = resp.json()
        for oper in operations:
            if int(oper["level"]) > int(prev_level):
                if int(oper["amount"]) != 0:
                    if (oper["type"] == "transaction" and oper["target"]["address"] == rec["address"]) or (oper["type"] == "delegation" and oper["newDelegate"]["alias"] == "Tez Nebraska"):
                        amt = str(oper["amount"])
                        delegator_list.append(str(oper["level"]) + ":" + rec["address"] + ":" + amt)
                    elif (oper["type"] == "transaction" and oper["sender"]["address"] == rec["address"]) or (oper["type"] == "delegation" and oper["prevDelegate"]["alias"] == "Tez Nebraska"):
                        amt = str(oper["amount"] * -1)
                        delegator_list.append(str(oper["level"]) + ":" + rec["address"] + ":" + amt)
            else:
                break

    delegator_list.sort()
    for del_rec in delegator_list:
        temp_list = del_rec.split(":")
        address = temp_list[1]
        amount = int(temp_list[2])
        prev_staking_balance += amount
        if prev_staking_balance > staking_capacity:
            return address

    return("overdelegator not found")
    #return("tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")



topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
s3 = boto3.client('s3')
file_path = os.getenv("FILE_PATH")
#prev_level_info = get_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt")
#prev_level_info = get_last_level(s3,"/tmp/last_level.txt")
prev_level_info = get_last_level(s3,file_path)
print(prev_level_info)
temp_list = prev_level_info.split(',')
prev_level = temp_list[0]
prev_status = temp_list[1]
prev_staking_balance = int(temp_list[2])
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_level = data["level"]
resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/operations?type=set_deposits_limit")
data = resp.json()
rec = data[0]
frozen_deposit_limit = 10 * int(rec["limit"])
resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")
data = resp.json()
delegator_balance_limit = 10 * int(data["balance"])
resp = requests.get("https://api.tzkt.io/v1/delegates/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")
data = resp.json()

staking_capacity = min(frozen_deposit_limit,delegator_balance_limit)

resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/delegators?limit=300")
delegators = resp.json()

staking_balance = int(data["stakingBalance"])
if staking_balance > staking_capacity and staking_balance > prev_staking_balance and prev_status == "Not Overdelegated":
    result = find_overdelegator(prev_level,prev_staking_balance,staking_capacity,delegators)
    if result != "overdelegator not found":
        report = "{0} may be overdelegator".format(result)
        send_message(report,topic_arn)
    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,"Overdelegated",staking_balance)
    update_last_level(s3,file_path,current_level,"Overdelegated",staking_balance)
else:
    #exit(0)
    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,prev_status,staking_balance)
    indicator = "Overdelegated"
    if staking_balance <= staking_capacity:
        indicator = "Not Overdelegated"

    update_last_level(s3,file_path,current_level,indicator,staking_balance)
