import requests
import math
from datetime import datetime

#base_URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1cwwjwgQJSvAq8ZMqerJVxU6qZGuYD4357&limit=8000&status=missed&cycles="
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1fbumHTEhLMBmtT1GjagbMnhnTD5YZAJh2&limit=8000&status=realized&cycle="
# resp = requests.get(base_URL)
# data = resp.json()
# slots = 0
# for rec in data:
#     print(rec)
#     if rec["type"] == "endorsing":
#         slots = slots + int(rec["slots"])
# print(slots)
cycle="642"
#.99745
rightsList = []
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=525&limit=8000"
resp = requests.get("https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + cycle + "&limit=10000")
data  = resp.json()
for rec in data:
    rightsList.append(rec)
resp = requests.get("https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + cycle + "&limit=10000&offset=10000")
data  = resp.json()
for rec in data:
    rightsList.append(rec)
#resp = requests.get("https://api.ghostnet.tzkt.io/v1/rights?baker=tz1gjJfsjbB2ZwBfG7SiCxXUKWLvyLEEvP7U&cycle=530&limit=8000")
#   .7094 cycle 525 .7629 cycle 526 .96 cycle 527  .9958  cycle 528  .9950 cycle 529 .9945 cycle 530 .9819 cycle 531 .9834  cycle 532
#  .9932 first full cycle on vps  533  .9936  534  .9915  535 .9928 536 .9771  537 .9888 538  .9894 539 .9936 back to vps 542 .9926 543
#data = resp.json()
missed_blocks = 0
baked_blocks = 0
future_blocks = 0
missed_slots = 0
endorsed_slots = 0
future_slots = 0
total_slots = 0
for rec in rightsList:
    #print(rec)
    if rec["status"] == "realized":
        if rec["type"] == "endorsing":
            endorsed_slots = endorsed_slots + int(rec["slots"])
        else:
            print(rec)
            baked_blocks += 1
    elif rec["status"] == "missed":
        if rec["type"] == "endorsing":
            missed_slots = missed_slots + int(rec["slots"])
        else:
            print(rec)
            missed_blocks += 1
    else:
        if rec["type"] == "endorsing":
            future_slots = future_slots + int(rec["slots"])
        else:
            if rec["round"] == 0:
                print(rec)
                future_blocks += 1
total_slots = missed_slots + endorsed_slots

if total_slots > 0:
    reliability = endorsed_slots/total_slots
else:
    reliability = 0
print("Total slots: {0}, endorsed slots: {1}, missed slots: {2}, reliability {3}".format(total_slots,endorsed_slots,missed_slots,reliability))
print("Baked blocks: {0}, Missed blocks: {1}, Future blocks: {2}".format(baked_blocks,missed_blocks,future_blocks))
print("If no more slots missed: {0}".format(1 - (missed_slots/ (total_slots + future_slots))))
print("Total scheduled slots for the cycle: {0}".format(total_slots + future_slots))
#base_url = "https://api.tzkt.io/v1/rights?baker=tz1dbfppLAAxXZNtf2SDps7rch3qfUznKSoK&status=realized&type=baking&limit=500&cycle="
base_url = "https://api.tzkt.io/v1/rights?baker=tz1Kf25fX1VdmYGSEzwFy1wNmkbSEZ2V83sY&status=realized&type=baking&limit=500&cycle="
starting_cycle = 634
total_blocks = 0
total_missed_successor_blocks = 0

while starting_cycle <= 643:
    resp = requests.get(base_url + str(starting_cycle))
    data = resp.json()
    for rec in data:
        total_blocks += 1
        block_resp = requests.get("https://api.tzkt.io/v1/blocks/" + str(rec["level"] +1))
        block_info = block_resp.json()
        if block_info["proposer"]["address"] != block_info["producer"]["address"] or block_info["payloadRound"] > 0 or block_info["blockRound"] > 0:
            missed_baker_address = "none"
            total_missed_successor_blocks += 1
            missed_baker_resp = requests.get("https://api.tzkt.io/v1/rights?type=baking&status=missed&level=" + str(block_info["level"]))
            missed_baker_info = missed_baker_resp.json()
            if len(missed_baker_info) > 0:
                missed_baker_address = missed_baker_info[0]["baker"]["address"]

            print("Block time {0}, level {1}, block proposer: {2}, block producer: {3}, payload round: {4}, block round: {5}, baker who missed: {6}".format(
            block_info["timestamp"],block_info["level"],block_info["proposer"]["address"],block_info["producer"]["address"]
                ,block_info["payloadRound"],block_info["blockRound"],missed_baker_address
             ))

    starting_cycle += 1
print("*****Total blocks: {0}, total missed successor blocks {1}, percentage missed {2}".format(total_blocks,total_missed_successor_blocks,total_missed_successor_blocks/total_blocks))
exit(0)
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = {}
resp = requests.get(base_url + "641" + "?limit=500")
split = resp.json()
print(split)
temp_delegators_list = split["delegators"]
active_stake = split["activeStake"]
staking_balance = split["stakingBalance"]
delegator_balance = 0
for rec in temp_delegators_list:
    if rec["address"] == "tz1ZLEhySrTcVhMvwJH5KPpSb6qmgmz5hVwY":
        print(rec)
        delegator_balance = rec["balance"]
print(delegator_balance - (staking_balance - active_stake))
exit(0)
resp = requests.get("https://api.tzkt.io/v1/delegates?active=true&limit=10000&sort=activationLevel")
data = resp.json()
for rec in data:
    if "alias" in rec:
        alias = rec["alias"]
    else:
        alias = "none"

    print("{0},alias: {1},activation time: {2}, {3}".format(rec["address"],alias,rec["activationTime"],rec["balance"]))

exit(0)


base_url = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&status=missed&limit=500&cycle="
starting_cycle = 634

while starting_cycle <= 640:
    resp = requests.get(base_url + str(starting_cycle))
    data = resp.json()
    for rec in data:
        previous_level = rec["level"] - 1
        block_resp = requests.get("https://api.tzkt.io/v1/blocks/" + str(previous_level))
        block_info = block_resp.json()
        print("Missed endorsement at level {0},{1}, prior level proposer {2}, producer {3}".format(rec["level"],rec["timestamp"],block_info["proposer"]["address"],block_info["producer"]["address"]))
    starting_cycle += 1

exit(0)



base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = {}
resp = requests.get(base_url + "641" + "?limit=500")
split = resp.json()
print(split)
temp_delegators_list = split["delegators"]
active_stake = split["activeStake"]
staking_balance = split["stakingBalance"]
delegator_balance = 0
for rec in temp_delegators_list:
    if rec["address"] == "tz1ZLEhySrTcVhMvwJH5KPpSb6qmgmz5hVwY":
        print(rec)
        delegator_balance = rec["balance"]
print(delegator_balance - (staking_balance - active_stake))
exit(0)
resp = requests.get("https://api.tzkt.io/v1/delegates?active=true&limit=10000&sort=activationLevel")
data = resp.json()
for rec in data:
    if "alias" in rec:
        alias = rec["alias"]
    else:
        alias = "none"

    print("{0},alias: {1},activation time: {2}, {3}".format(rec["address"],alias,rec["activationTime"],rec["balance"]))

exit(0)
resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/delegators?limit=300")
data = resp.json()
total_balances = 0
total_above_balances = 0
total_below_balances = 0
total_above = 0
total_below = 0
total_delegators = 0

for delegator in data:
    if delegator["balance"] < 10000000:
        total_below_balances += delegator["balance"]
        total_below += 1
    else:
        total_above_balances += delegator["balance"]
        total_above += 1
    total_balances += delegator["balance"]
    total_delegators += 1

print(total_balances)
print(total_above_balances)
print(total_below_balances)
print(total_above)
print(total_below)
print(total_delegators)
print("Percent balances at or above min: {0}".format(total_above_balances / total_balances))
print("Percent balances below min: {0}".format(total_below_balances / total_balances))
print("Percent at or above min: {0}".format(total_above / total_delegators))
print("Percent below min: {0}".format(total_below / total_delegators))

exit(0)
base_URL = "https://api.tzkt.io/v1/quotes?level="
starting_level = 1
while starting_level < 3514368:
    resp = requests.get(base_URL + str(starting_level))
    rec = resp.json()
    if rec[0]["usd"] < .90:
        print("{0}, {1}".format(rec[0]["usd"],rec[0]["timestamp"]))
    starting_level += 16000
exit(0)
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/" + str(cycle) + "?limit=500"
resp = requests.get(base_url)
split = resp.json()
if future_slots == 0 and split["endorsements"] / split["expectedEndorsements"] < .68:
    print("\n*****Endorsements less than two thirds expected, might lose all endorsement rewards*****")


resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/delegators?limit=300")
delegators = resp.json()
for rec in delegators:
    resp2 = requests.get("https://api.tzkt.io/v1/accounts/" + rec["address"] + "/operations?type=transaction&limit=1000")
    trx = resp2.json()
    for trx_rec in trx:
        #print(trx_rec)
        if trx_rec["level"] > 3493550 and trx_rec["amount"] > 100000000:
            print(trx_rec)

exit(0)
current_cycle = 525
base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
max_cycle = 605
while current_cycle <= max_cycle:
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
    split = resp.json()
    if split["endorsements"] / split["expectedEndorsements"] < .8:
        print(split)
    current_cycle += 1