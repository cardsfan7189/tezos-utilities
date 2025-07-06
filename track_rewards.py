import time

import requests
import boto3
import json
from decimal import Decimal
import os
import math
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

#URL = "https://prater.beaconcha.in/api/v1/validator/stats/66536"
#URL = "https://prater.beaconcha.in/api/v1/validator/stats/284747"
#URL = "https://api.tzkt.io/v1/cycles/200"
#URL = "https://api.tzkt.io/v1/statistics"
URL = "https://api.tzkt.io/v1/delegates?active=true&limit=700"
#687567698853889
#687960287985538
#693319188891231
#682314220224283
#685,911,936.003519

def insert_table_item(ddb,table_name,cycle_info):
    #cycle_info = {'baker_address': "tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6", "cycle_index": 0, "cycle_rewards": 0,"accumulated_rewards": Decimal(str(3547.73))}
    table = ddb.Table(table_name)
    put_response = table.put_item(Item=cycle_info)

    if put_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return True
    else:
        return False

def get_cycle_adjustments(baker_address,firstLevel,lastLevel):
    adjustment_amt = 0
    original_last_level = lastLevel
    lastLevel += 10800
    #firstLevel = original_last_level
    ops_url = "https://api.tzkt.io/v1/accounts/" + baker_address + "/operations?type=transaction&level.ge=" + str(firstLevel) + "&level.le=" + str(lastLevel)
    #print(ops_url)
    resp = requests.get(ops_url)
    ops = resp.json()
    if len(ops) > 2:
        print("More than three payments during last two rewards period. Exiting")
        return None
    if ops[0]["sender"]["address"] == baker_address:
        adjustment_amt += ops[0]["amount"]
    if ops[1]["sender"]["address"] == baker_address:
        adjustment_amt -= ops[1]["amount"]

    return adjustment_amt

def send_email(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="Rewards Tracking")
def get_rewards_info(baker_address,firstLevel,lastLevel):
    baking_rewards = 0
    base_account_url = "https://api.tzkt.io/v1/accounts/" + baker_address + "/balance_history/"
    resp = requests.get(base_account_url + str(lastLevel))
    end_of_cycle_balance = resp.json() / 1000000
    resp = requests.get(base_account_url + str(firstLevel - 1))
    beginning_of_cycle_balance = resp.json() / 1000000
    #rights_url = "https://api.tzkt.io/v1/rights?type=baking&baker={0}&cycle={1}&status=realized".format(baker_address,cycle)
    rewards = end_of_cycle_balance - beginning_of_cycle_balance

    return rewards

def getLastDBItem(ddb,table_name,baker_address,cycle):
    table = ddb.Table(table_name)
    response = None
    response = table.query(
        ScanIndexForward=False,
        KeyConditionExpression=Key("baker_address").eq(baker_address) & Key("cycle_index").gte(cycle)
    )

    if response["Count"] > 0:
        return response["Items"]
    else:
        return None

def lambda_handler(event,context):
    print(event)
    print("calling main")
    main()

def load_cycle_info(file_name):
    fo = open(file_name,"r+")
    line = fo.readline()
    fields = line.split(",")
    fo.close()
    return fields

def save_cycle_info(file_name,cycle_info):
    with open(file_name,"w") as outfile:
        outfile.write(cycle_info)
        outfile.flush()
        outfile.close()

def get_tezpay_rewards(cycle):
    file_path = "/home/arbest/tezpay/reports/{0}/summary.json".format(cycle)

    while not os.path.exists(file_path):
        time.sleep(120)

    fo = open(file_path,"r+")
    rewards = json.load(fo)
    summary = rewards["summary"]
    fo.close()
    total_payment = int(summary["distributed_rewards"]) + int(summary["donated_total"])
    return total_payment / 1000000


def main():
    saved_cycle = 0
    ddb = boto3.resource('dynamodb')
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    table_name = "tezos_rewards"
    baker_address = "tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6"
    result = requests.get("https://rpc.tzkt.io/mainnet/chains/main/blocks/head")
    head = result.json()
    cycle = int(head["metadata"]["level_info"]["cycle"])
    cycle_position = int(head["metadata"]["level_info"]["cycle_position"])
    latest_db_cycle_info = getLastDBItem(ddb,table_name,baker_address,cycle - 2)

    last_cycle_info = latest_db_cycle_info[0]
    if cycle == last_cycle_info["cycle_index"] + 2:
        target_cycle = cycle - 1
        accumulated_rewards = last_cycle_info["accumulated_rewards"]
        resp = requests.get("https://api.tzkt.io/v1/cycles/" + str(target_cycle))
        cycle_info = resp.json()
        first_level = cycle_info["firstLevel"]
        last_level = cycle_info["lastLevel"]
        rewards = get_rewards_info(baker_address,first_level,last_level)
        print(rewards)
        #rewards_to_delegators = get_tezpay_rewards(saved_cycle)
        #adjusted_rewards = rewards - rewards_to_delegators
        #accumulated_rewards += Decimal(str(adjusted_rewards))
        accumulated_rewards += Decimal(str(rewards))
        cycle_info = {'baker_address': baker_address, "cycle_index": cycle, "cycle_rewards": Decimal(str(rewards)),"accumulated_rewards": accumulated_rewards}
        insert_table_item(ddb,table_name,cycle_info)
        message = "Rewards for cycle {0}: {1}, total accumulated rewards: {2}".format(cycle,rewards,accumulated_rewards)
        print(message)
        send_email(message,topic_arn)


#main()