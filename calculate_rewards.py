import requests
import boto3
import json

def load_total_rewards(s3,file_name):
    s3.download_file('teznebraska', 'rewards_totals.json', file_name)
    fo = open(file_name,"r+")
    #line = fo.readline()
    total_rewards = json.load(fo)
    fo.close()
    return total_rewards
def get_rewards_and_fees(delegatorAddress,payorAddress,counter_max):
    counter = 0
    total_delegator_rewards = 0
    total_fees = 0
    resp = requests.get("https://api.tzkt.io/v1/accounts/" + payorAddress + "/operations?type=transaction&target=" + delegatorAddress + "&limit=500")
    if resp:
        data = resp.json()
        if (len(data) > 0):
            for transaction in data:
                total_delegator_rewards = total_delegator_rewards + transaction["amount"]
                total_fees = total_fees + transaction["bakerFee"] + transaction["storageFee"]
                counter += 1
                if counter == counter_max:
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

rewards_totals = {}
s3 = boto3.client('s3')
file_name = "C:\\users\\drewa\\downloads\\rewards_totals.json"
payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
rewards_totals = load_total_rewards(s3,file_name)

if rewards_totals == {}:
    print("NO REWARDS TOTALS FOUND")
    exit(-1)

last_totals_cycle = rewards_totals["cycle"]

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
last_cycle = int(data["cycle"]) - 1
base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
#starting_cycle = 525
starting_cycle = last_totals_cycle + 1
current_cycle = starting_cycle
total_delegate_rewards = 0
if current_cycle > last_cycle:
    exit(0)

cycle_counter = last_cycle - current_cycle + 1

while current_cycle <= last_cycle:
    resp = requests.get(base_rewards_url + str(current_cycle))
    if resp.status_code != 200:
        current_cycle = current_cycle + 1
        continue
    rewards = resp.json()
    print(rewards)
    total_delegate_rewards = total_delegate_rewards + rewards["blockRewards"] + rewards["endorsementRewards"] + rewards["blockFees"]
    current_cycle = current_cycle + 1
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = []
current_cycle = starting_cycle
while current_cycle <= last_cycle:
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
    if resp.status_code != 200:
        current_cycle = current_cycle + 1
        continue
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
    tuple_result = get_rewards_and_fees(rec,payor_address,cycle_counter)
    total_delegator_rewards = total_delegator_rewards + tuple_result[0]
    total_fees = total_fees + tuple_result[1]

net_baker_rewards = total_delegate_rewards - total_delegator_rewards - total_fees

print("Total baking rewards since last cycle: {0}, total delegator rewards: {1}, total fees: {2}, net baker rewards: {3}".format(total_delegate_rewards,total_delegator_rewards,total_fees,net_baker_rewards))
rewards_totals["totalBakingRewards"] = rewards_totals["totalBakingRewards"] + total_delegate_rewards
rewards_totals["totalDelegatorRewards"] = rewards_totals["totalDelegatorRewards"] + total_delegator_rewards
rewards_totals["totalBakerRewards"] = rewards_totals["totalBakerRewards"] + net_baker_rewards
rewards_totals["cycle"] = last_cycle
print("Total baking rewards: {0}".format(rewards_totals["totalBakingRewards"] / 1000000))
print("Total delegator rewards: {0}".format(rewards_totals["totalDelegatorRewards"] / 1000000))
print("Total baker rewards: {0}".format(rewards_totals["totalBakerRewards"] / 1000000))

with open(file_name,"w") as outfile:
    json_dump = json.dumps(rewards_totals,indent=4)
    outfile.write(json_dump)
    outfile.flush()
    outfile.close()

s3.upload_file(file_name, "teznebraska", "rewards_totals.json")
