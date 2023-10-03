import requests
import json
import boto3

def load_overdelegations(s3):
    file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('monitor-overdelegators', 'overdelegations.json', file_name)
    fo = open(file_name,"r+")
    overdelegations = json.load(fo)
    fo.close()
    return overdelegations

def load_overdelegators(overdelegations):
    overdelegator_list = []

    length = len(overdelegations)
    for overdelegation in overdelegations:
        if overdelegation["endDate"] == None:
            #print(overdelegation)
            for rec in overdelegation["overDelegators"]:
                overdelegator_list.append(rec["delegator"])

    if len(overdelegator_list) == 0:
        for rec in overdelegations[length-1]["overDelegators"]:
            overdelegator_list.append(rec["delegator"])

    return overdelegator_list

s3 = boto3.client('s3')
payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_cycle = int(data["cycle"])
#current_cycle = 634
base_rewards_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"

overdelegations = load_overdelegations(s3)
#print(overdelegations)
overdelegators = load_overdelegators(overdelegations)

#overdelegators = ['tz1VF7ZigN29oQMF5qbmTEVMrrd9dAUcCQha',
 #                 'tz1WfnAasgiPtmg5Vbt8MyhutxsjrvJXuXa3']

overdelegator_dict = {}

for overdelegator in overdelegators:
    print(overdelegator)
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
            print(rec)
            print(rec["address"] + "," + overdelegator_dict[rec["address"]])

    #print(temp_delegators_list)
    current_cycle += 1
    resp = requests.get(base_url + str(current_cycle) + "?limit=500")
exit(0)
file_name = "C:\\users\\drewa\\downloads\\temp.txt"
fo = open(file_name,"r+")
line = fo.readline()
fo.close()
over_delegations = json.loads(line)
print(over_delegations)
for over_delegation in over_delegations:
    for over_delegator in over_delegation["overDelegators"]:
        address = over_delegator["delegator"]
        resp = requests.get("https://api.tzkt.io/v1/accounts/" + address + "/operations?type=transaction&limit=50")
        operations = resp.json()
        for oper in operations:
            print("{0}: {1}, {2}".format(address,int(oper["amount"]) / 1000000,oper["timestamp"]))
