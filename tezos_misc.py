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
cycle="608"
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