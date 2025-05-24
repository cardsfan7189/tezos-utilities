import requests
import boto3
import json
from datetime import datetime

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message)

def load_overdelegators(s3,file_name):
    #file_name = "C:\\users\\drewa\\downloads\\temp.txt"
    s3.download_file('teznebraska', 'overdelegators.json', file_name)
    fo = open(file_name,"r+")
    overdelegations = json.load(fo)
    fo.close()

    return overdelegations["overdelegators"]

def lambda_handler(event,context):
    print("Lambda handler")
    main(   )

def main():
    s3 = boto3.client('s3')
    topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
    block_len = 10  # seconds
    curr_tezpay_delay = 1433 # cycle pos
    event_sched_rate = 1800 # seconds
    buffer = 6 # levels
    upper_limit = curr_tezpay_delay + (event_sched_rate / block_len ) + buffer
    lower_limit = curr_tezpay_delay - (event_sched_rate / block_len ) - buffer
    print(upper_limit,lower_limit)
    payor_address = "tz1fnU3mjTn8aH2tJ5TcnS5HnfP4wUEhjE7j"
    resp = requests.get("https://rpc.tzkt.io/mainnet/chains/main/blocks/head")
    data = resp.json()
    cycle_pos = data["metadata"]["level_info"]["cycle_position"]
    cycle = data["metadata"]["level_info"]["cycle"]
    level = data["metadata"]["level_info"]["level"]
    prev_cycle = cycle - 1
    if cycle_pos < lower_limit or cycle_pos > upper_limit:
        return
    starting_level = level - cycle_pos
    overdelegators = load_overdelegators(s3,"/tmp/temp.txt")
    base_url = "https://api.tzkt.io/v1/rewards/split/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
    resp = requests.get(base_url + str(prev_cycle) + "?limit=500")
    first_delegator = ""
    last_delegator = None
    data = resp.json()
    for delegator in data["delegators"]:
        if delegator["address"] in overdelegators:
            continue
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
        send_msg("Rewards payments may have been missed", topic_arn)


#main()