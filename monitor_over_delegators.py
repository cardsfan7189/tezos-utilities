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

def load_overdelegations(s3,file_name,file_path):
    #file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('monitor-overdelegators', file_name, file_path)
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

def get_last_level_cycle(s3,file_name,file_path):
    s3.download_file('monitor-overdelegators',file_name,file_path)
    fo = open(file_name,"r+")
    line = fo.read(64)
    fo.close()
    return line

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
                amt = 0
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

def reconcile_perm_overdelegation_list(s3,temp_overdelegations,overdelegations,data,prev_cycle_status,prev_cycle,prev_cycle_snapshot_level):
    print("in reconcile perm overdelegation list")
    return overdelegations

def close_perm_overdelegations(overdelegations):
    print("in close perm over delegations")
    return overdelegations
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

    s3.upload_file(overdelegations_file_path, "monitor-overdelegators", "overdelegations.json")
    s3.upload_file(overdelegations_file_path, "teznebraska", "overdelegations.json")

topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
s3 = boto3.client('s3')
last_level_file_path = os.getenv("LAST_LEVEL_FILE_PATH")
last_cycle_file_path = os.getenv("LAST_CYCLE_FILE_PATH")
overdelegations_file_path = os.getenv("OVERDELEGATIONS_FILE_PATH")
temp_overdelegations_file_path = os.getenv("TEMP_OVERDELEGATIONS_FILE_PATH")

temp_overdelegations = load_overdelegations(s3,"temp_overdelegations.json",temp_overdelegations_file_path)
temp_overdelegators = load_overdelegators(temp_overdelegations)

prev_level_info = get_last_level_cycle(s3,"last_level.txt",last_level_file_path)
print(prev_level_info)
temp_list = prev_level_info.split(',')
prev_level = temp_list[0]
prev_status = temp_list[1]
prev_staking_balance = int(temp_list[2])

prev_cycle_info = get_last_level_cycle(s3,"last_cycle.txt",last_cycle_file_path)
print(prev_cycle_info)
temp_list = prev_cycle_info.split(',')
prev_cycle = temp_list[0]
prev_cycle_snapshot_level = temp_list[1]
prev_cycle_status = temp_list[2]

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_level = data["level"]
current_timestamp = data["timestamp"]
current_cycle = data["cycle"]

if int(current_cycle) > int(prev_cycle):
    overdelegations = load_overdelegations(s3,"overdelegations.json",overdelegations_file_path)
    overdelegators = load_overdelegators(overdelegations)
    resp = requests.get("https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(current_cycle))
    data = resp.json()
    if data["stakingBalance"] > data["activeStake"]:
        updated_overdelegations = reconcile_perm_overdelegation_list(s3,temp_overdelegations,overdelegations,data,prev_cycle_status,prev_cycle,prev_cycle_snapshot_level)
        temp_overdelegations.clear()
    else:
        temp_overdelegations.clear()
        updated_overdelegations = close_perm_overdelegations(overdelegations)
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
        update_overdelegations_file(s3,result,overdelegations,overdelegations_file_path,None,None,None,flag)

    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,"Overdelegated",staking_balance)
    update_last_level(s3,last_level_file_path,current_level,"Overdelegated",staking_balance)
else:
    if prev_status == "Overdelegated":
        update_overdelegations_file(s3,None,overdelegations,overdelegations_file_path,current_timestamp,current_level,current_cycle,"Close")
    #exit(0)
    #update_last_level(s3,"c:\\users\\drewa\\downloads\\last_level.txt",current_level,prev_status,staking_balance)
    update_last_level(s3,last_level_file_path,current_level,"Not Overdelegated",staking_balance)
