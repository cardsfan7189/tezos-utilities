import requests
import boto3
import math
from datetime import datetime


def send_email(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="Baking Alert")

def get_last_cycle(s3,file_name):
    s3.download_file('last-cycle', 'last_cycle.txt', file_name)
    fo = open(file_name,"r+")
    line = fo.read(8)
    fo.close()
    return int(line)


def update_last_cycle(s3,file_name,latest_cycle):
    with open(file_name,mode="wt") as f:
        f.write(latest_cycle)
        f.flush()
        f.close()
    response = s3.upload_file(file_name, "last-cycle", "last_cycle.txt")

def lambda_handler(event,context):
    file_name = "/tmp/last_cycle.txt"
    return 0

def check_round_0_baker(level,last_cycle):
    resp = requests.get("https://api.tzkt.io/v1/rights?type=baking&round=0&level=" + str(level))
    data = resp.json()
    baker_address = data[0]["baker"]["address"]
    starting_cycle = last_cycle
    counter = 10
    alert = ""
    expected_endorsements = 0
    actual_endorsements = 0

    while counter > 0:
        resp = requests.get("https://api.tzkt.io/v1/rewards/bakers/" + baker_address + "?cycle=" + str(starting_cycle))
        data = resp.json()
        expected_endorsements += data[0]["expectedEndorsements"]
        actual_endorsements += data[0]["endorsements"]
        counter = counter - 1
        starting_cycle = starting_cycle - 1

    reliability = actual_endorsements / expected_endorsements
    if reliability < .95:
        alert = "Potential missed block 0 with baker {0}, reliability {1} at level {2}".format(baker_address,reliability,level)

    return alert
def process(starting_cycle,save_starting_cycle,file_name,last_cycle):
    round1_alert = ""
    report = ""
    rights_list = []
    base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&limit=10000&cycle="
    resp = requests.get(base_URL + str(starting_cycle))
    data = resp.json()
    for item in data:
        rights_list.append(item)
    resp = requests.get(base_URL + str(starting_cycle) + "&offset=10000")
    data = resp.json()
    for item in data:
        rights_list.append(item)
    for rec in rights_list:
        if rec["type"] == "baking" and rec["round"] < 4 and rec["status"] == "future":
                #print(rec)
            level_url = "https://tzkt.io/" + str(rec["level"])
            report = report + "Baking at cycle {3}, round {0}, level {1}, at {2}\n".format(rec["round"],level_url,rec["timestamp"],rec["cycle"])
            if rec["round"] == 1:
                round1_alert = check_round_0_baker(rec["level"],last_cycle)
                if round1_alert != "":
                    report = report + round1_alert + "\n"

    if len(report) > 0:
        print(report)
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    send_email(report,topic_arn)
    update_last_cycle(s3,file_name,str(save_starting_cycle))

file_name = "/tmp/last_cycle.txt"
#file_name = "C:\\Users\\DREWA\\Downloads\\last_cycle_baking_alert.txt"
s3 = boto3.client('s3')
last_cycle = int(get_last_cycle(s3,file_name))
print(last_cycle)

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()
starting_cycle = int(data["cycle"])
save_starting_cycle = starting_cycle
if starting_cycle > last_cycle:
    process(starting_cycle,save_starting_cycle,file_name,last_cycle)
