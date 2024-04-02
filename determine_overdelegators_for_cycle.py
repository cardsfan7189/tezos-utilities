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

def update_dictionary(address,change_flag,new_overdelegator_dict):

    if change_flag == "Overdelegation triggered":
        new_overdelegator_dict[address] = "Triggered overdelegation"
    elif change_flag == "Additional overdelegator":
        new_overdelegator_dict[address] = "Additional overdelegator"
    elif change_flag == "Added to overdelegation":
        if address in new_overdelegator_dict.keys() and new_overdelegator_dict[address] == "Triggered overdelegation":
            pass
        else:
            new_overdelegator_dict[address] = "Added to overdelegation"

    return new_overdelegator_dict
def find_overdelegators(prev_level
                        ,curr_level
                        ,prev_staking_balance
                        ,curr_staking_balance
                        ,active_stake,
                        delegators
                        ,prev_delegators
                        ,prev_cycle_overdelegated):
    #https://api.tzkt.io/v1/operations/transactions?anyof.sender.target=
    new_overdelegator_list = ""
    delegator_dict = {}
    prev_delegator_dict = {}

    results = ""

    for rec in delegators:
        delegator_dict[rec["address"]] = rec["balance"]

    for rec in prev_delegators:
        prev_delegator_dict[rec["address"]] = rec["balance"]

    for rec in delegator_dict.copy():
        current_balance = delegator_dict[rec]
        resp = requests.get("https://api.tzkt.io/v1/accounts/" + rec + "/balance_history/" + str(prev_level))
        if resp.status_code == 200:
            if int(resp.json()) >= current_balance and rec in prev_delegator_dict:
                del  delegator_dict[rec]

    delegator_list = []

    new_overdelegator_dict = {}

    for rec in delegators:
        #print(rec["address"])
        if rec["address"] == "tz1SSxhSAq6GYoQLaDjBEQSJsoDV7SxJnPTA":
            print(rec["address"])
        resp = requests.get("https://api.tzkt.io/v1/accounts/" + rec["address"] + "/operations?type=delegation,transaction&limit=1000&level.gt=" + str(prev_level) + "&level.lt=" + str(curr_level))
        operations = resp.json()
        for oper in operations:
            amt = 0
            if int(oper["amount"]) > 0:
                if (oper["type"] == "transaction" and oper["target"]["address"] == rec["address"] and oper["sender"]["address"] != "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j") or (oper["type"] == "delegation" and oper["newDelegate"]["alias"] == "Tez Nebraska"):
                    amt = str(oper["amount"])
                    type = oper["type"]
                    #delegator_list.append(str(oper["level"]) + ":" + rec["address"] + ":" + amt)
            elif int(oper["amount"]) < 0:
                if (oper["type"] == "transaction" and oper["sender"]["address"] == rec["address"]) or (oper["type"] == "delegation" and oper["prevDelegate"]["alias"] == "Tez Nebraska"):
                    amt = str(oper["amount"] * -1)
                    type = oper["type"]
            if amt != 0:
                delegator_list.append(str(oper["level"]) + "|" + rec["address"] + "|" + str(amt) + "|" + type + "|" + "0" + "|" + "1901-01-01T00:00:00" + "|" + oper["timestamp"])

    delegator_list.sort()

    if prev_staking_balance > active_stake:
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
        change_flag = "No change"

        #print("{0} updated by {1}, {2}".format(prev_staking_balance / 1000000, address,amount/1000000))
        if prev_staking_balance > active_stake:
            if overdelegation_triggered == False:
                change_flag = "Overdelegation triggered"
                overdelegation_triggered = True;
            elif oper_type == "delegation":
                change_flag = "Additional overdelegator"
            elif oper_type == "transaction":
                change_flag = "Added to overdelegation"
        else:
            if overdelegation_triggered == True:
                # no longer overdelegated
                new_overdelegator_dict = {}
                overdelegation_triggered = False

        if change_flag != "No change":
            new_overdelegator_dict = update_dictionary(address,change_flag,new_overdelegator_dict)

    for key in delegator_dict:
        current_balance = delegator_dict[key]
        if key in prev_delegator_dict.keys():
            if prev_delegator_dict[key] >= delegator_dict[key]:
                del new_overdelegator_dict[key]

    for key in new_overdelegator_dict:
        if key in prev_delegator_dict.keys():
            prev_balance = prev_delegator_dict[key] / 1000000
        else:
            prev_balance = 0

        if key in delegator_dict.keys():
            curr_balance = delegator_dict[key] / 1000000
            if curr_balance < 10:
                continue
        else:
            # omit delegator whose balance decreased
            continue

        results = results + "{0},{1},{2},{3}\n".format(key,new_overdelegator_dict[key],str(prev_balance),str(curr_balance))

    return results

resp = requests.get("https://api.tzkt.io/v1/cycles")
cycles = resp.json()
last_cycle = cycles[0]["index"]
last_cycle = 720
prev_cycle = cycles[1]["index"]
prev_cycle = 719
last_snapshot_level = cycles[0]["snapshotLevel"]
last_snapshot_level = 5264384
prev_snapshot_level = cycles[1]["snapshotLevel"]
prev_snapshot_level = 5235712
out_file_name = "/home/arbest/overdelegators_" + str(last_cycle) + ".txt"
out_file_name = "C:\\USERS\\DREWA\\DOWNLOADS\\overdelegators_" + str(last_cycle) + ".txt"

resp = requests.get("https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(last_cycle) + "?limit=500")
rewards_split = resp.json()

output = "Not over-delegated"

if rewards_split["stakingBalance"] > rewards_split["activeStake"]:
    output = ""
    prev_cycle_overdelegated = False
    resp = requests.get("https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(prev_cycle) + "?limit=500")
    prev_rewards_split = resp.json()

    if prev_rewards_split["stakingBalance"] > prev_rewards_split["activeStake"]:
        prev_cycle_overdelegated = True
        prev_cycle_overdelegated = False
    print("Previous snapshot level: {0}, current snapshot level: {1}".format(prev_snapshot_level,last_snapshot_level))
    overdelegator_results = find_overdelegators(prev_snapshot_level
                                                ,last_snapshot_level
                                                ,prev_rewards_split["stakingBalance"]
                                                ,rewards_split["stakingBalance"]
                                                ,rewards_split["activeStake"]
                                                ,rewards_split["delegators"]
                                                ,prev_rewards_split["delegators"]
                                                ,prev_cycle_overdelegated)
    print(overdelegator_results)
    if len(overdelegator_results) > 1:

        #print(overdelegator_results)
        overdelegator_recs = overdelegator_results.split('\n')
        for rec in overdelegator_recs:
            temp = None
            #rec = rec.strip()
            fields = rec.split(',')
            if len(fields) == 1:
                continue
            address = fields[0]
            flag = fields[1]
            prev_balance = round(float(fields[2]))
            curr_balance = round(float(fields[3]))
            if flag == "Triggered overdelegation":
                max_balance = round(prev_balance + (rewards_split["activeStake"]/1000000) - (prev_rewards_split["stakingBalance"]/1000000))
                temp = "{0},max balance,{1}".format(address,max_balance)
            elif flag == "Additional overdelegator":
                temp = "{0},ignore".format(address)
            elif flag == "Added to overdelegation":
                if prev_balance < 10:
                    temp = "{0},ignore".format(address)
                else:
                    temp = "{0},max balance,{1}".format(address,prev_balance)
            else:
                print(fields)

            if temp:
                output += temp + "\n"

print(output)
with open(out_file_name,mode="wt") as f:
    f.write(output)
    f.flush()
    f.close()

