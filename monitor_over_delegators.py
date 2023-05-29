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

def load_overdelegations(s3,file_name):
    #file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('monitor-overdelegators', 'overdelegations.json', file_name)
    fo = open(file_name,"r+")
    line = fo.readline()
    fo.close()
    return json.loads(line)

def load_overdelegators(overdelegations):
    overdelegator_list = []

    for overdelegation in overdelegations:
        if overdelegation["endDate"] == None:
            #print(overdelegation)
            for rec in overdelegation["overDelegators"]:
                overdelegator_list.append(rec["delegator"])

    return overdelegator_list

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

def find_overdelegators(prev_level,prev_staking_balance,staking_capacity,delegators):
    #https://api.tzkt.io/v1/operations/transactions?anyof.sender.target=
    new_overdelegator_list = []
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
                        type = oper["type"]
                        #delegator_list.append(str(oper["level"]) + ":" + rec["address"] + ":" + amt)
                    elif (oper["type"] == "transaction" and oper["sender"]["address"] == rec["address"]) or (oper["type"] == "delegation" and oper["prevDelegate"]["alias"] == "Tez Nebraska"):
                        amt = str(oper["amount"] * -1)
                        type = oper["type"]
                    delegator_list.append(str(oper["level"]) + "|" + rec["address"] + "|" + amt + "|" + type + "|" + str(rec["delegationLevel"]) + "|" + rec["delegationTime"])

    delegator_list.sort()

    if prev_staking_balance > staking_capacity:
        overdelegation_triggered = True
    else:
        overdelegation_triggered = False

    for del_rec in delegator_list:
        temp_list = del_rec.split("|")
        address = temp_list[1]
        amount = int(temp_list[2])
        oper_type = temp_list[3]
        del_level = int(temp_list[4])
        del_date = temp_list[5]
        prev_staking_balance += amount
        if prev_staking_balance > staking_capacity:
            over_delegator = {
                "delegator" : address,
                "delgationDate" : del_date,
                "delegationLevel" : del_level
            }
            if overdelegation_triggered == False:
                overdelegation_triggered = True
                new_overdelegator_list.append(over_delegator)
            elif oper_type == "delegation":
                new_overdelegator_list.append(over_delegator)

    return new_overdelegator_list
    #return("tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")

def build_report(result):
    temp = ""
    for rec in result:
        temp = temp + rec["delegator"] + "\n"

    report_str = "New overdelegators:\n{0}".format(temp)
    return report_str

def update_overdelegations_file(s3,result,overdelegations,overdelegations_file_path,overdelegation_end_date,overdelegation_end_level,flag):
    if flag == "New":
        newOverDelegation = {
            "startDate" : result[0]["delegationDate"],
            "startLevel" : result[0]["delegationLevel"],
            "endDate" : None,
            "endLevel" : None,
            "overDelegators" : result
        }
        overdelegations.append(newOverDelegation)
    elif flag == "Update":
        array_length = len(overdelegations)
        for rec in result:
            overdelegations[array_length - 1]["overDelegators"].append(rec)
    elif flag == "Close":
        array_length = len(overdelegations)
        overdelegations[array_length - 1]["endDate"] = overdelegation_end_date
        overdelegations[array_length - 1]["endLevel"] = overdelegation_end_level

    with open(overdelegations_file_path,"w") as outfile:
        json_dump = json.dumps(overdelegations)
        outfile.write(json_dump)
        outfile.flush()
        outfile.close()

    response = s3.upload_file(overdelegations_file_path, "monitor-overdelegators", "overdelegations.json")

topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
s3 = boto3.client('s3')
file_path = os.getenv("FILE_PATH")
overdelegations_file_path = os.getenv("OVERDELEGATIONS_FILE_PATH")

#prev_level_info = get_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt")
#prev_level_info = get_last_level(s3,"/tmp/last_level.txt")
overdelegations = load_overdelegations(s3,overdelegations_file_path)
overdelegators = load_overdelegators(overdelegations)

prev_level_info = get_last_level(s3,file_path)
print(prev_level_info)
temp_list = prev_level_info.split(',')
prev_level = temp_list[0]
prev_status = temp_list[1]
prev_staking_balance = int(temp_list[2])
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_level = data["level"]
current_timestamp = data["timestamp"]
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
result = []
if staking_balance > staking_capacity:
    result = find_overdelegators(prev_level,prev_staking_balance,staking_capacity,delegators)
    if len(result) > 0:
        report = build_report(result)
        #report = "{0} may be overdelegator".format(result)
        send_message(report,topic_arn)
        if prev_status == "Not Overdelegated":
            flag = "New"
        else:
            flag = "Update"
        update_overdelegations_file(s3,result,overdelegations,overdelegations_file_path,None,None,flag)

    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,"Overdelegated",staking_balance)
    update_last_level(s3,file_path,current_level,"Overdelegated",staking_balance)
else:
    if prev_status == "Overdelegated":
        update_overdelegations_file(s3,None,overdelegations,overdelegations_file_path,current_timestamp,current_level,"Close")
    #exit(0)
    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,prev_status,staking_balance)
    update_last_level(s3,file_path,current_level,"Not Overdelegated",staking_balance)
