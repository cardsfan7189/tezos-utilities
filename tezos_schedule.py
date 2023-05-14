import requests
import math
from datetime import datetime

#URL = "https://prater.beaconcha.in/api/v1/validator/stats/66536"
#URL = "https://prater.beaconcha.in/api/v1/validator/stats/284747"
#URL = "https://api.tzkt.io/v1/cycles/200"
#URL = "https://api.tzkt.io/v1/statistics"
URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1fh2hMTTQKYejt4F4wCxyQAD1VenCpMEg3&limit=8000&status=realized&cycle=97"
URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1cwwjwgQJSvAq8ZMqerJVxU6qZGuYD4357&limit=8000&status=missed&cycle=97"

#URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1fh2hMTTQKYejt4F4wCxyQAD1VenCpMEg3&limit=8000&cycle=94"
#URL = "https://api.ithacanet.tzkt.io/v1/accounts/tz1fh2hMTTQKYejt4F4wCxyQAD1VenCpMEg3"
#687567698853889
#687960287985538
#693319188891231
#682314220224283
#685,911,936.003519

def calc_total_rewards(address,activationTime):
    total = 0
    resp = requests.get("https://api.tzkt.io/v1/rewards/bakers/" + address + "?limit=500")
    data = resp.json()
    for rec in data:
        if int(rec["stakingBalance"]) < 9001000000:
            total = total + int(rec["endorsementRewards"]) + int(rec["ownBlockRewards"])
        else:
            return
    print("{0}: {1}, {2}".format(address,total / 1000000, activationTime))

def check_for_baking(data,base_URL,starting_cycle):
    while data:
        #print(data)
        for rec in data:

            if rec["type"] == "baking":
                print(rec)
        starting_cycle = starting_cycle + 1
        resp = requests.get(base_URL + str(starting_cycle))
        data = resp.json()

def check_future_schedule(data,base_URL,starting_cycle):
    max_cycle = starting_cycle + 19;
    while (starting_cycle < max_cycle):
        for rec in data:
            if rec["type"] == "baking" and rec["round"] < 3:
                print(rec)
        starting_cycle = starting_cycle + 1
        resp = requests.get(base_URL + str(starting_cycle) + "&limit=5000")
        data = resp.json()

starting_cycle = 504
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
#starting_cycle = int(data["cycle"])
base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&limit=8000&cycle="
#base_URL = "https://api.ithacanet.tzkt.io/v1/rights?baker=tz1cwwjwgQJSvAq8ZMqerJVxU6qZGuYD4357&limit=8000&cycle="
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1fbumHTEhLMBmtT1GjagbMnhnTD5YZAJh2&limit=8000&status=realized&cycle="
resp = requests.get(base_URL + str(starting_cycle))
#resp = requests.get(base_URL)
data = resp.json()
missed_slots = 0
endorsed_slots = 0
endorsing_realized = 0
endorsing_missed = 0
blocks_baked = 0
blocks_missed = 0
for rec in data:
    #print(rec)
    if rec["type"] == "baking" and rec["status"] == "realized":
        blocks_baked = blocks_baked + 1
    elif rec["type"] == "baking" and rec["status"] == "missed":
        blocks_missed = blocks_missed + 1
    elif rec["status"] == "realized":
        endorsed_slots = endorsed_slots + int(rec["slots"])
        endorsing_realized = endorsing_realized + 1
    elif rec["status"] == "missed":
        missed_slots = missed_slots + int(rec["slots"])
        endorsing_missed = endorsing_missed + 1
    elif rec["status"] == "future":
        print(rec)

print("endorsed slots: {0}, endorsing realized: {1}".format(endorsed_slots,endorsing_realized))
print("missed slots: {0}, endorsing missed: {1}".format(missed_slots,endorsing_missed))
print("baked blocks {0}".format(blocks_baked))
print("missed blocks: {0}".format(blocks_missed))
#check_for_baking(data,base_URL,starting_cycle)
check_future_schedule(data,base_URL,starting_cycle)
