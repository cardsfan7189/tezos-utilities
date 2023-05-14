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
starting_cycle = 611
overdelegators = ['tz1TGXxNCximgsvk7T88AHLWigBkLWsTdtn9',
                  'tz1XowM9gCxYxbKW8SiUWSBXmwv7mnorQkFy',
                  'tz1eexKABYjDYhidDBGcDaQv6uzHRpMBuJiy',
                  'tz1gW4pi34zmJDkbYtSgCk9VHMbbMjUh7qdm',
                  'tz1Y8kCWcGzTBFrnGZ1YCLPG7wrchY3AmmYn'
                  ]
current_cycle = starting_cycle
total_delegate_rewards = 0
resp = requests.get(base_rewards_url + str(current_cycle))
rewards = resp.json()
print(rewards)
total_rewards = rewards["blockRewards"] + rewards["endorsementRewards"] + rewards["blockFees"]
#total_rewards = 12277
print(rewards)
delegate_balance = rewards["stakingBalance"] - rewards["delegatedBalance"]
print("Delegate percentage: {0}".format(delegate_balance / rewards["activeStake"]))
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = {}
resp = requests.get(base_url + str(current_cycle) + "?limit=500")
split = resp.json()
print(split)
#print("Missed block rewards: {0}, missed block fees: {1}, missed endorsement rewards {2}".format(split["missedBlockRewards"],split["missedBlockFees"],split["missedEndorsementRewards"]))
temp_delegators_list = split["delegators"]
total_delegated_balance = 0
total_all_delegator_balance = 0
for delegator in temp_delegators_list:
    print(delegator)
    total_all_delegator_balance = total_all_delegator_balance + delegator["balance"]
    #if delegator["balance"] >= 10000000 or delegator["address"] in overdelegators:
    if delegator["balance"] >= 10000000 and delegator["address"] not in overdelegators:
        total_delegated_balance = total_delegated_balance + delegator["balance"]
    delegators_list[delegator["address"]] = delegator["balance"]
total_eligible_stake_balance = total_delegated_balance + delegate_balance
print("total eligible stake balance {0}".format(total_eligible_stake_balance))
print("Total staking balance, including below-minimum delegators {0}".format((total_all_delegator_balance + delegate_balance) / 1000000))
total_delegator_rewards = 0
for rec in delegators_list:
    if rec not in overdelegators and delegators_list[rec] >= 10000000:
        ratio = (delegators_list[rec] / total_eligible_stake_balance) * .97
        delegator_reward = total_rewards * ratio
        print("{0}: {1}".format(rec,delegator_reward / 1000000))
        total_delegator_rewards = total_delegator_rewards + delegator_reward
print("Total delegator rewards: {0}".format(total_delegator_rewards / 1000000))
print("Baker rewards: {0}".format((total_rewards - total_delegator_rewards) / 1000000))
print(rewards)
if rewards["stakingBalance"] > rewards["activeStake"]:
    print("stacking balance: {0}, active stake {1}".format(rewards["stakingBalance"]/1000000,rewards["activeStake"]/1000000))
    print("***Overdelegated***")

for rec in overdelegators:
    if rec in delegators_list:
        print(rec)