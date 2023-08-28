import requests
import math

#filename_path = "/home/arbest/cycle_rewards.txt"
filename_path = "C:\\users\\drewa\\downloads\\cycle_rewards.txt"

fo = open(filename_path,"r+")
line = fo.readline()
fo.close()
fields = line.split(",")
print(fields)
prev_cycle = int(fields[0])

base_url = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6?cycle="
resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
starting_cycle = data["cycle"]

if starting_cycle == prev_cycle:
    exit(0)

resp = requests.get(base_url+str(starting_cycle))
rewards_data = resp.json()
#print(rewards_data)
total_est_rewards= (rewards_data[0]["futureBlockRewards"] + rewards_data[0]["blockRewards"] + rewards_data[0]["missedBlockRewards"] + rewards_data[0]["blockFees"] + rewards_data[0]["futureEndorsementRewards"]) / 1000000
resp = requests.get("https://api.tzkt.io/v1/accounts/tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j")
data = resp.json()
total_est_rewards = total_est_rewards - (data["balance"] / 1000000) + .51
out_rec = "{0},{1}".format(starting_cycle,math.ceil(total_est_rewards))
print(out_rec)
with open(filename_path,"w") as outfile:
    outfile.write(out_rec)
    outfile.flush()
    outfile.close()
