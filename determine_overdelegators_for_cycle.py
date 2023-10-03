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

resp = requests.get("https://api.tzkt.io/v1/cycles")
cycles = resp.json()
last_cycle = cycles[0]["index"]
prev_cycle = cycles[1]["index"]
last_snapshot_level = cycles[0]["snapshotLevel"]
prev_snapshot_level = cycles[1]["snapshotLevel"]

resp = requests.get("https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(last_cycle) + "?limit=500")
rewards_split = resp.json()

if rewards_split["stakingBalance"] > rewards_split["activeStake"]:

    prev_cycle_overdelegated = False
    resp = requests.get("https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(prev_cycle) + "?limit=500")
    prev_rewards_split = resp.json()

    if prev_rewards_split["stakingBalance"] > prev_rewards_split["activeStake"]:
        prev_cycle_overdelegated = True
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
        file_name = "C:\\users\\drewa\\downloads\\overdelegators_" + str(last_cycle) + ".txt"
        with open(file_name,mode="wt") as f:
            f.write(overdelegator_results)
            f.flush()
            f.close()

        print(overdelegator_results)
        overdelegator_recs = overdelegator_results.split('\n')
        for rec in overdelegator_recs:
            #rec = rec.strip()
            fields = rec.split(',')
            address = fields[0]
            flag = fields[1]
            prev_balance = round(float(fields[2]))
            curr_balance = round(float(fields[3]))
            if flag == "Triggered overdelegation":
                max_balance = round(prev_balance + (rewards_split["activeStake"]/1000000) - (prev_rewards_split["stakingBalance"]/1000000))
                print("{0} max balance: {1}".format(address,max_balance))
            elif flag == "Additional overdelegator":
                print("{0} - ignore".format(address))
            elif flag == "Added to overdelegation":
                if prev_balance < 10:
                    print("{0} - ignore".format(address))
                else:
                    print("{0} max balance: {1}".format(address,prev_balance))
            else:
                print(fields)
