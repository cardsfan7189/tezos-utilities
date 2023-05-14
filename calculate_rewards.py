import requests
import math
from datetime import datetime

def get_rewards_and_fees(delegatorAddress,payorAddress):
    total_delegator_rewards = 0
    total_fees = 0
    resp = requests.get("https://api.tzkt.io/v1/accounts/" + payorAddress + "/operations?type=transaction&target=" + delegatorAddress)
    if resp:
        data = resp.json()
        if (len(data) > 0):
            for transaction in data:
                total_delegator_rewards = total_delegator_rewards + transaction["amount"]
                total_fees = total_fees + transaction["bakerFee"] + transaction["storageFee"]

    return(tuple((total_delegator_rewards,total_fees)))

def validate_payouts(delegator_list,payor_address,current_cycle):
    resp = requests.get("https://api.tzkt.io/v1/cycles/" + str(current_cycle))
    prev_cycle = resp.json()
    last_level = prev_cycle["lastLevel"]
    for rec in delegator_list:
        if (rec["balance"] > 9999999):
            resp= requests.get("https://api.tzkt.io/v1/accounts/" + payor_address + "/operations?type=transaction&target=" + rec["address"] + "&sort=Descending")
            data = resp.json()
            if len(data) > 0:
                transaction = data[0]
                if transaction["level"] < last_level:
                    print("payout may have been missed for " + rec["address"])
            else:
                print("payout may have been missed for " + rec["address"])

payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
last_cycle = int(data["cycle"]) - 1
base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
#starting_cycle = 525
starting_cycle = 482
current_cycle = starting_cycle
total_delegate_rewards = 0
while current_cycle <= last_cycle:
    resp = requests.get(base_rewards_url + str(current_cycle))
    rewards = resp.json()
    print(rewards)
    total_delegate_rewards = total_delegate_rewards + rewards["blockRewards"] + rewards["endorsementRewards"] + rewards["blockFees"]
    current_cycle = current_cycle + 1

base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = []
current_cycle = starting_cycle
while current_cycle <= last_cycle:
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
    split = resp.json()
    temp_delegators_list = split["delegators"]
    for delegator in temp_delegators_list:
        delegators_list.append(delegator["address"])
    if (current_cycle == last_cycle):
        validate_payouts(temp_delegators_list,payor_address,current_cycle)
    current_cycle = current_cycle + 1
data = resp.json()
latest_rewards = data["blockRewards"] + data["endorsementRewards"] + data["blockFees"]
print("Latest rewards: {0}".format(latest_rewards))
total_delegator_rewards = 0
total_fees = 0
list_set = set(delegators_list)
delegators_list_unique = list(list_set)
for rec in delegators_list_unique:
    tuple_result = get_rewards_and_fees(rec,payor_address)
    total_delegator_rewards = total_delegator_rewards + tuple_result[0]
    total_fees = total_fees + tuple_result[1]

net_baker_rewards = total_delegate_rewards - total_delegator_rewards - total_fees

print("Total baking rewards: {0}, total delegator rewards: {1}, total fees: {2}, net baker rewards: {3}".format(total_delegate_rewards,total_delegator_rewards,total_fees,net_baker_rewards))