import time
import requests
import math
import boto3
import subprocess
import json

import os.path

from datetime import datetime, timedelta, date
import pytz

def send_message(message):
    topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
    sns = boto3.client('sns')
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="[URGENT] Action needed today - Payout task")

def get_start_time(level_end_time):
    #unaware_date_time = datetime.strptime(data[2]["endTime"],"%Y-%m-%dT%H:%M:%SZ")
    unaware_date_time = datetime.strptime(level_end_time,"%Y-%m-%dT%H:%M:%SZ")
    utc_date_time = pytz.utc.localize(unaware_date_time)
    #print(pytz.all_timezones)
    date_time = utc_date_time.astimezone(tz=pytz.timezone('America/Chicago'))

    while True:
        test_date_time = date_time + timedelta(minutes=15)
        hour = int(test_date_time.strftime('%H'))
        if hour > 8 and hour < 22:
            return test_date_time
        else:
            date_time = test_date_time

def time_to_pay(target_time_str):
    target_time_dt = datetime.strptime(target_time_str,"%Y-%m-%d %H:%M:%S%z")
    current_time_dt = datetime.now().astimezone(tz=pytz.timezone('America/Chicago'))
    if current_time_dt > target_time_dt - timedelta(minutes=16) and current_time_dt < target_time_dt + timedelta(minutes=16):
        return True
    else:
        return False

def load_cycle_info(file_name):
    fo = open(file_name,"r+")
    line = fo.readline()
    fields = line.split(",")
    fo.close()
    return fields

def get_total_payments(file_name):
    fo = open(file_name,"r+")
    rewards = json.load(fo)
    summary = rewards["summary"]
    fo.close()
    total_payment = int(summary["cycle_rewards"]) + int(summary["donated_total"])
    resp = requests.get("https://api.tzkt.io/v1/accounts/tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j/balance")
    balance = resp.json()
    total_payment -= balance
    return int(total_payment / 1000000)

def save_cycle_info(file_name,cycle_info):
    with open(file_name,"w") as outfile:
        outfile.write(cycle_info)
        outfile.flush()
        outfile.close()
def pull_cycle_rewards(cycle):
    try:
        base_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6?cycle="
        resp = requests.get(base_url+str(cycle))
        rewards_data = resp.json()
        total_est_rewards= (rewards_data[0]["futureBlockRewards"] + rewards_data[0]["blockRewards"] + rewards_data[0]["missedBlockRewards"] + rewards_data[0]["blockFees"] + rewards_data[0]["futureEndorsementRewards"]) / 1000000
        resp = requests.get("https://api.tzkt.io/v1/accounts/tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j")
        data = resp.json()
        total_est_rewards = (total_est_rewards * .33) - (data["balance"]/1000000)
        print(total_est_rewards)
        if total_est_rewards < 20:
            return 99999
        return math.ceil(total_est_rewards)
    except requests.exceptions.HTTPError as errh:
        print("HTTP Error at pull_cycle_rewards")
        print(errh.args[0])
        return(99999)

def delay_until_waking_hours():
    # Get the current time
    central = pytz.timezone("America/Chicago")
    time_format = "%H:%M:%S"
    start_time_string = "08:00:00"
    end_time_string = "23:00:00"

    start_time_object = datetime.strptime(start_time_string, time_format).time()
    end_time_object = datetime.strptime(end_time_string, time_format).time()

    current_time = datetime.now(central).time()

    while current_time < start_time_object and current_time > end_time_object:
        time.sleep(60)
        current_time = datetime.now(central).time()

    return

def get_payment_amount(cycle):

    end_cycle = cycle - 2
    expected_rewards = 0

    while cycle >= end_cycle:
        resp = requests.get("https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6?cycle=" + str(cycle))
        rewards = resp.json()

        cycle_rewards = rewards[0]["futureBlockRewards"] + rewards[0]["blockRewardsDelegated"] + rewards[0]["blockRewardsStakedOwn"] + rewards[0]["blockRewardsStakedEdge"] + rewards[0]["blockRewardsStakedShared"] \
                        + rewards[0]["futureEndorsementRewards"] + rewards[0]["endorsementRewardsDelegated"] + rewards[0]["endorsementRewardsStakedOwn"] + rewards[0]["endorsementRewardsStakedEdge"] + rewards[0]["endorsementRewardsStakedShared"] \
                        + rewards[0]["futureDalAttestationRewards"] + rewards[0]["dalAttestationRewardsDelegated"] + rewards[0]["dalAttestationRewardsStakedOwn"] + rewards[0]["dalAttestationRewardsStakedEdge"] + rewards[0]["dalAttestationRewardsStakedShared"]
        expected_rewards += cycle_rewards
        print("expected rewards for cycle {0}: {1}".format(cycle,cycle_rewards / 1000000))
        cycle -= 1

    total_payment = round((expected_rewards * .297 * .96) / 1000000)

    return total_payment

def get_payout_acct_balance(address):
    resp = requests.get("https://api.tzkt.io/v1/accounts/" + address + "/balance")
    balance = resp.json()
    return math.floor(balance / 1000000)

payout_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"

file_name = "/home/arbest/cycle_info.txt"
#file_name = "C:\\Users\\DREWA\\Downloads\\cycle_info.txt"

saved_cycle_info = load_cycle_info(file_name)
date_fields = saved_cycle_info[1].split('-')
prev_cycle = saved_cycle_info[0]
past_date = date(int(date_fields[0]),int(date_fields[1]),int(date_fields[2]))
current_date = date.today()
delta = current_date - past_date
if delta.days >= 4:
    resp = requests.get("https://api.tzkt.io/v1/cycles")
    cycles = resp.json()
    futuremost_cycle = cycles[0]["index"]
    if futuremost_cycle - int(prev_cycle) < 3:
        exit(0)
else:
    exit(0)

new_date = date.today().strftime('%Y-%m-%d')
payment_amount = get_payment_amount(futuremost_cycle)
payout_account_balance = get_payout_acct_balance(payout_address)
if payout_account_balance < 10:
    payment_amount -= payout_account_balance
cycle_info_rec = "{0},{1},{2},{3}".format(futuremost_cycle,new_date,"ready to pay",payment_amount)
save_cycle_info(file_name,cycle_info_rec)