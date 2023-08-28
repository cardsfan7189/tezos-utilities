import requests

filename_path = "C:\\users\\drewa\\downloads\\cycle_for_tezpay.txt"

fo = open(filename_path,"r+")
line = fo.readline()
fo.close()
#fields = line.split(",")
prev_cycle = int(line)

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
current_cycle = data["cycle"]

if current_cycle == prev_cycle:
    exit(0)
else:
    with open(filename_path,"w") as outfile:
        outfile.write(str(current_cycle))
        outfile.flush()
        outfile.close()
    exit(current_cycle)
