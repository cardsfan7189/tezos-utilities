import requests
import math
from datetime import datetime

payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_cycle = int(data["cycle"])
base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"

overdelegators = ['tz1TGXxNCximgsvk7T88AHLWigBkLWsTdtn9'
                ,'tz1XowM9gCxYxbKW8SiUWSBXmwv7mnorQkFy'
                ,'tz1eexKABYjDYhidDBGcDaQv6uzHRpMBuJiy'
                  ,'tz1gW4pi34zmJDkbYtSgCk9VHMbbMjUh7qdm']

overdelegator_dict = {}

for overdelegator in overdelegators:
    resp = requests.get("https://api.tzkt.io/v1/accounts/" + overdelegator + "/operations?type=delegation&newDelegate=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")
    delegation = resp.json()
    #print(delegation)
    delegation_time = delegation[0]["timestamp"]
    overdelegator_dict[overdelegator] = delegation_time

base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
delegators_list = {}
resp = requests.get(base_url + str(current_cycle) + "?limit=500")

while resp.status_code == 200:
    split = resp.json()
    cycle_resp = requests.get("https://api.tzkt.io/v1/cycles/" + str(current_cycle))
    cycle_data = cycle_resp.json()
    indicator = "Not over-delegated"
    if split["stakingBalance"] > split["activeStake"]:
        indicator = "*** Over-delegated"
    print("Cycle {0}, stakingBalance {1}, activeStake {2}, cycle started {3}, {4}".format(current_cycle,split["stakingBalance"],split["activeStake"],cycle_data["startTime"],indicator))
    temp_delegators_list = split["delegators"]
    for rec in temp_delegators_list:
        if rec["address"] in overdelegator_dict.keys():
            print(rec["address"] + "," + overdelegator_dict[rec["address"]])

    #print(temp_delegators_list)
    current_cycle += 1
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")

