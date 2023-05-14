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

def calc_rewards(current_cycle):
    base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
    overdelegators = ['tz1eRwGZqssQd9vVBsnrCtQTNDkUc3vpHGiJ','tz1cqK7Pe21EVDnoUe1u7wXyytDTTEUXKRSm']
    total_delegate_rewards = 0
    resp = requests.get(base_rewards_url + str(current_cycle))
    rewards = resp.json()
    #print(rewards)
    total_rewards = rewards["blockRewards"] + rewards["endorsementRewards"] + rewards["blockFees"]
    #total_rewards = 12277
    delegate_balance = rewards["stakingBalance"] - rewards["delegatedBalance"]
    #print("Delegate percentage: {0}".format(delegate_balance / rewards["activeStake"]))
    base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
    delegators_list = {}
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
    split = resp.json()
    temp_delegators_list = split["delegators"]
    total_delegated_balance = 0
    total_all_delegator_balance = 0
    for delegator in temp_delegators_list:
        #print(delegator)
        total_all_delegator_balance = total_all_delegator_balance + delegator["balance"]
        #if delegator["balance"] >= 10000000 or delegator["address"] in overdelegators:
        if delegator["balance"] >= 10000000:
            total_delegated_balance = total_delegated_balance + delegator["balance"]
        delegators_list[delegator["address"]] = delegator["balance"]
    total_eligible_stake_balance = total_delegated_balance + delegate_balance
    #print("total eligible stake balance {0}".format(total_eligible_stake_balance))
    #print("Total staking balance, including below-minimum delegators {0}".format((total_all_delegator_balance + delegate_balance) / 1000000))
    total_delegator_rewards = 0
    for rec in delegators_list:
        ratio = (delegators_list[rec] / total_eligible_stake_balance) * .97
        delegator_reward = total_rewards * ratio
        #print("{0}: {1}".format(rec,delegator_reward / 1000000))
        total_delegator_rewards = total_delegator_rewards + delegator_reward
    #print("Total delegator rewards: {0}".format(total_delegator_rewards / 1000000))
    #print("Baker rewards: {0}".format((total_rewards - total_delegator_rewards) / 1000000))
    return total_rewards - total_delegator_rewards

def convert_rewards(cycle,tez_rewards):
    resp = requests.get("https://api.tzkt.io/v1/cycles/" + str(cycle) + "?quote=usd")
    data = resp.json()
    return tez_rewards * int(data["quote"]["usd"])

starting_cycle = 482
last_cycle = 561

total_rewards_usd = 0

while starting_cycle <= last_cycle:
    rewards = calc_rewards(starting_cycle) / 1000000
    rewards_usd = convert_rewards(starting_cycle,rewards)
    total_rewards_usd = total_rewards_usd + rewards_usd
    starting_cycle = starting_cycle + 1

print(total_rewards_usd)

