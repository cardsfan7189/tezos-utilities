import requests
import boto3
import json
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
                break
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
def load_overdelegations(s3):
    file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('monitor-overdelegators', 'overdelegations.json', file_name)
    fo = open(file_name,"r+")
    overdelegations = json.load(fo)
    fo.close()
    return overdelegations
def load_overdelegators(overdelegations,last_cycle):
    overdelegator_list = []
    overdelegation_hold = None

    list_length = len(overdelegations)

    for overdelegation in overdelegations:
        if overdelegation["endCycle"]:
            if overdelegation["endCycle"] + 6 >=  last_cycle:
                overdelegation_hold = overdelegation
                break
            else:
                continue
        else:
            overdelegation_hold = overdelegation

    for rec in overdelegation_hold["overDelegators"]:
        overdelegator_list.append(rec["delegator"])

    return overdelegator_list

s3 = boto3.client('s3')
payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
resp = requests.get("https://api.tzkt.io/v1/head")
overdelegators = []
data = resp.json()
last_cycle = int(data["cycle"]) - 1
base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
resp = requests.get(base_rewards_url + str(last_cycle))
rewards = resp.json()
#last_cycle = 622
if rewards["activeStake"] < rewards["stakingBalance"]:
    overdelegations = load_overdelegations(s3)
    #print(overdelegations)
    overdelegators = load_overdelegators(overdelegations, last_cycle)

total_delegate_rewards = 0

#print(rewards)
total_rewards = rewards["blockRewards"] + rewards["endorsementRewards"] + rewards["blockFees"] + rewards["missedEndorsementRewards"] + rewards["missedBlockRewards"] + rewards["missedBlockFees"]

delegate_balance = rewards["stakingBalance"] - rewards["delegatedBalance"]
print("Delegate percentage: {0}".format(delegate_balance / rewards["activeStake"]))
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = {}

resp = requests.get(base_url + str(last_cycle) + "?limit=500")
split = resp.json()
#print(split)
#print("Missed block rewards: {0}, missed block fees: {1}, missed endorsement rewards {2}".format(split["missedBlockRewards"],split["missedBlockFees"],split["missedEndorsementRewards"]))
temp_delegators_list = split["delegators"]
total_delegated_balance = 0
total_all_delegator_balance = 0
total_overdelegator_balance = 0
for delegator in temp_delegators_list:
    total_all_delegator_balance = total_all_delegator_balance + delegator["balance"]
    #if delegator["balance"] >= 10000000 or delegator["address"] in overdelegators:
    if delegator["balance"] >= 10000000 and delegator["address"] not in overdelegators:
        total_delegated_balance = total_delegated_balance + delegator["balance"]
    elif delegator["address"] in overdelegators:
        total_overdelegator_balance = total_overdelegator_balance + delegator["balance"]
        if rewards["stakingBalance"] - total_overdelegator_balance - delegator["balance"] < rewards["activeStake"]:
            temp_result = delegator["balance"] - (rewards["stakingBalance"] - rewards["activeStake"])
            delegator["balance"] = round(temp_result / 1000000,0) * 1000000
            print("*** Partial over-delegator {0} max balance is {1}".format(delegator["address"],delegator["balance"]/1000000))
            total_delegated_balance = total_delegated_balance + delegator["balance"]
            overdelegators.remove(delegator["address"])
    delegators_list[delegator["address"]] = delegator["balance"]
total_eligible_stake_balance = total_delegated_balance + delegate_balance
print("total eligible stake balance {0}, total eligible delegator balance {1}, total baker eligible balance {2}".format(total_eligible_stake_balance,total_delegated_balance,delegate_balance))
print("Total staking balance, including below-minimum delegators {0}".format((total_all_delegator_balance + delegate_balance) / 1000000))
print("Total over-delegator staking balance: {0}".format(total_overdelegator_balance))
print("Total rewards this cycle: {0}".format(total_rewards / 1000000))
total_delegator_rewards = 0
total_actual_payments = 0
for rec in delegators_list:
    if rec not in overdelegators and delegators_list[rec] >= 10000000:
        ratio = (delegators_list[rec] / total_eligible_stake_balance) * .97
        delegator_reward = total_rewards * ratio
        actual_tuple = get_rewards_and_fees(rec,payor_address)
        total_actual_payments = actual_tuple[0] + total_actual_payments
        print("{0}: {1}, actual payment {2}, total fees {3}".format(rec,delegator_reward / 1000000,actual_tuple[0]/1000000,actual_tuple[1]/1000000))
        total_delegator_rewards = total_delegator_rewards + delegator_reward
print("Total delegator rewards: {0}".format(total_delegator_rewards / 1000000))
print("Total actual payments: {0}".format(total_actual_payments / 1000000))
print("Baker rewards: {0}".format((total_rewards - total_delegator_rewards) / 1000000))
if rewards["stakingBalance"] > rewards["activeStake"]:
    print("staking balance: {0}, active stake {1}".format(rewards["stakingBalance"]/1000000,rewards["activeStake"]/1000000))
    print("***Overdelegated. Following are over-delegators***")

for rec in overdelegators:
    if rec in delegators_list:
        print(rec)
#print("\n\n Don't forget to remove tz1TG9wxgEFataMzLPVt1Hwx3QrUraWm5QCT exception from load_overdelegators")                                                                                                      wq