import requests
import math
from datetime import datetime

#URL = "https://prater.beaconcha.in/api/v1/validator/stats/66536"
#URL = "https://prater.beaconcha.in/api/v1/validator/stats/284747"
#URL = "https://api.tzkt.io/v1/cycles/200"
#URL = "https://api.tzkt.io/v1/statistics"
URL = "https://api.tzkt.io/v1/delegates?active=true&limit=700"
#687567698853889
#687960287985538
#693319188891231
#682314220224283
#685,911,936.003519

def calc_total_rewards(address,activationTime):
    total = 0
    cycle = 482
    base_URL = "https://api.tzkt.io/v1/rewards/bakers/" + address
    resp = requests.get(base_URL + "/" + str(cycle))
    if resp.status_code != 200:
        return 0
    data = resp.json()
    cycle_count = 0;
    while cycle < 521:
        if int(data["stakingBalance"]) < 80001000000:
            total = total + int(data["endorsementRewards"]) + int(data["blockRewards"])
                    #+ int(data["ownBlockRewards"])
            cycle_count = cycle_count + 1
        cycle = cycle + 1
        resp = requests.get(base_URL + "/" + str(cycle))
        data = resp.json()

    if cycle_count > 0:
        print("{0}: {1}, {2}, {3}".format(address,total / 1000000,(total / 1000000) / cycle_count, activationTime))
        return (total / 1000000) / cycle_count
    else:
        return 0

def missed_endorsements(address):
    resp = requests.get("https://api.tzkt.io/v1/rights?baker="+ address + "&limit=8000")
    data = resp.json()

URL = "https://api.tzkt.io/v1/delegates?limit=8000&active=true"
resp = requests.get(URL)
data = resp.json()
count = 0
total_rewards = 0
for rec in data:
    if int(rec["stakingBalance"]) > 70000000000 and int(rec["stakingBalance"]) < 74001000000:
    #if rec["address"] == "tz1WApynBLihFFfDpaDdFerYvQE4dsZyfocn":
        #print(rec)
        #if rec["activationLevel"] < 2315470:
         rewards = calc_total_rewards(rec["address"],rec["activationTime"])
         if rewards:
            count = count + 1
            total_rewards = total_rewards + int(rewards)

print(total_rewards / count)

