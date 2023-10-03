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
    #s3.download_file('monitor-overdelegators', 'overdelegations.json', file_name)
    fo = open(file_name,"r+")
    overdelegations = json.load(fo)
    fo.close()
    return overdelegations

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

    #response = s3.upload_file(file_name, "monitor-overdelegators", "last_level.txt")
    #print(response)

def find_overdelegators(prev_level,prev_staking_balance,staking_capacity,delegators):
    #https://api.tzkt.io/v1/operations/transactions?anyof.sender.target=
    new_overdelegator_list = []
    print("prev staking balance: {0}, staking_capacity {1}".format(prev_staking_balance,staking_capacity))
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
                    delegator_list.append(str(oper["level"]) + "|" + rec["address"] + "|" + amt + "|" + type + "|" + str(rec["delegationLevel"]) + "|" + rec["delegationTime"] + "|" + oper["timestamp"])

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
        trans_level = temp_list[0]
        trans_date = temp_list[6]
        prev_staking_balance += amount
        print("{0},{1},{2},{3}".format(address,oper_type,amount,prev_staking_balance))
        if prev_staking_balance > staking_capacity:
            if oper_type == "delegation":
                over_delegation_date = del_date
                over_delegation_level = del_level
            else:
                over_delegation_date = trans_date
                over_delegation_level = trans_level
            over_delegator = {
                "delegator" : address,
                "delegationDate" : del_date,
                "delegationLevel" : del_level,
                "overDelegationType" : oper_type,
                "overDelegationDate" : over_delegation_date,
                "overDelegationLevel" : over_delegation_level
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

def update_overdelegations_file(s3,result,overdelegations,overdelegations_file_path,overdelegation_end_date,overdelegation_end_level,overdelegation_end_cycle,flag):
    if flag == "New":
        newOverDelegation = {
            "startDate" : result[0]["overDelegationDate"],
            "startLevel" : result[0]["overDelegationLevel"],
            "endDate" : None,
            "endLevel" : None,
            "endCycle" : None,
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
        overdelegations[array_length - 1]["endCycle"] = overdelegation_end_cycle

    with open(overdelegations_file_path,"w") as outfile:
        json_dump = json.dumps(overdelegations,indent=4)
        outfile.write(json_dump)
        outfile.flush()
        outfile.close()

    #s3.upload_file(overdelegations_file_path, "monitor-overdelegators", "overdelegations.json")
    #s3.upload_file(overdelegations_file_path, "teznebraska", "overdelegations.json")

topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
s3 = boto3.client('s3')
file_path = os.getenv("FILE_PATH")
overdelegations_file_path = os.getenv("OVERDELEGATIONS_FILE_PATH")

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_level = data["level"]
current_timestamp = data["timestamp"]
current_cycle = data["cycle"]

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_cycle = int(data["cycle"]) - 1
#current_cycle = 634
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
resp = requests.get(base_url + str(current_cycle) + "?limit=500")
prev_split = resp.json()
current_cycle += 1
resp = requests.get(base_url + str(current_cycle) + "?limit=500")

while resp.status_code == 200:
    split = resp.json()

    if split["stakingBalance"] > split["activeStake"]:
        indicator = "*** Over-delegated"
    print("Cycle {0}, stakingBalance {1}, activeStake {2}, cycle started {3}, {4}".format(current_cycle,split["stakingBalance"],split["activeStake"],cycle_data["startTime"],indicator))
    temp_delegators_list = split["delegators"]
    for rec in temp_delegators_list:
        if rec["address"] in overdelegator_dict.keys():
            print(rec)
            print(rec["address"] + "," + overdelegator_dict[rec["address"]])

    #print(temp_delegators_list)
    current_cycle += 1
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
