import requests
import boto3
import json
from datetime import datetime

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Subject="[URGENT] Action needed today - Missed payment",
                Message=message)

def load_cycle_info(s3,file_name,file_path):
    #file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('teznebraska', file_name, file_path + file_name)
    fo = open(file_path + file_name,"r+")
    cycle = fo.read()
    fo.close()
    return cycle

def upload_cycle_info(s3,file_name,file_path,cycle):
    #print("path: " + file_path + " " + file_name)
    with open(file_path + file_name, 'w') as f:
        f.write(cycle)

    s3.upload_file(file_path + file_name, "teznebraska", file_name)
def lambda_handler(event,context):
    print("Lambda handler")
    main(   )

def main():
    s3 = boto3.client('s3')
    topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"

    prev_cycle = 0
    file_name = "monitor_rewards_cycle.txt"
    file_path = "/tmp/"
    #file_path = "C:\\Users\\DREWA\\Downloads"
    block_len = 8  # seconds
    curr_tezpay_delay = 1256 # cycle pos  966 + 14
    event_sched_rate = 1800 # seconds
    buffer = 960 / 8  # number of seconds in 15 minute interval divided by seconds per level
    curr_tezpay_delay_with_padding = curr_tezpay_delay + (event_sched_rate / block_len ) + buffer
    lower_limit = curr_tezpay_delay
    #print(upper_limit,lower_limit)
    payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
    resp = requests.get("https://rpc.tzkt.io/mainnet/chains/main/blocks/head")
    data = resp.json()
    cycle_pos = data["metadata"]["level_info"]["cycle_position"]
    cycle = data["metadata"]["level_info"]["cycle"]
    last_cycle_processed = load_cycle_info(s3,file_name,file_path)
    print("Last cycle processed {0}, this cycle {1}".format(last_cycle_processed,cycle))
    print("Cycle position: {0}, current tezpay delay with padding {1}".format(cycle_pos,curr_tezpay_delay_with_padding))

    if last_cycle_processed == str(cycle) or cycle_pos < curr_tezpay_delay_with_padding:
        return 0

    resp = requests.get("https://api.tzkt.io/v1/cycles/" + str(cycle))
    cycle_info = resp.json()
    firstLevel = cycle_info["firstLevel"]
    #level = data["metadata"]["level_info"]["level"]
    #prev_cycle = cycle - 1
    #if cycle_pos < lower_limit or cycle_pos > upper_limit:
    #  return
    starting_level = firstLevel + curr_tezpay_delay_with_padding
    #overdelegators = load_overdelegators(s3,"/tmp/temp.txt")
    base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
    resp = requests.get(base_url + str(last_cycle_processed) + "?limit=500")
    first_delegator = ""
    last_delegator = None
    data = resp.json()
    for delegator in data["delegators"]:
        if first_delegator == "":
            first_delegator = delegator["address"]
        elif delegator["delegatedBalance"] > 9999999:
            last_delegator = delegator["address"]

    print("{0}, {1}".format(first_delegator,last_delegator))

    resp = requests.get("https://api.tzkt.io/v1/accounts/" + payor_address + "/operations?type=transaction&limit=1&level.gt=" + str(starting_level) + "&target=" + first_delegator)
    data = resp.json()
    count_1 = len(data)
    resp = requests.get("https://api.tzkt.io/v1/accounts/" + payor_address + "/operations?type=transaction&limit=1&level.gt=" + str(starting_level) + "&target=" + last_delegator)
    data = resp.json()
    count_2 = len(data)
    if count_1 == 0 or count_2 == 0:
        #if True:
        send_msg("Rewards payments may have been missed", topic_arn)
    upload_cycle_info(s3,file_name,file_path,str(cycle))

#main()