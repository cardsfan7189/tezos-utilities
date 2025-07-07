import time

import requests
import boto3
import json
from decimal import Decimal
import os
import math
from datetime import datetime
def load_cycle_info(file_name):
    fo = open(file_name,"r+")
    cycle = fo.readline()
    fo.close()
    return cycle.rstrip()

def save_cycle_info(file_name,cycle_info):
    with open(file_name,"w") as outfile:
        outfile.write(cycle_info)
        outfile.flush()
        outfile.close()

def get_tezpay_summary(file_path):
    summary_json = None
    if os.path.exists(file_path):
        fo = open(file_path,"r+")
        summary_json = json.load(fo)
        fo.close()
    return summary_json
def main():
    aws_api = "https://8aji15i999.execute-api.us-east-1.amazonaws.com/production/adjust_rewards"
    cycle_file_name = "/home/arbest/cycle_info.txt"
    #cycle_file_name = "C:\\Users\\DREWA\\Downloads\\cycle_info.txt"
    saved_cycle = load_cycle_info(cycle_file_name)
    tezpay_reports_dir = "/home/arbest/tezpay/reports/" + saved_cycle + "/summary.json"
    #tezpay_reports_dir = "C:\\Users\\DREWA\\Downloads\\" + saved_cycle + "\\summary.json"
    result = requests.get("https://rpc.tzkt.io/mainnet/chains/main/blocks/head")
    head = result.json()
    curr_cycle = int(head["metadata"]["level_info"]["cycle"])
    if int(saved_cycle) < curr_cycle:
        summary_json = get_tezpay_summary(tezpay_reports_dir)
        if summary_json is not None:
            resp = requests.put(aws_api,json = summary_json)
            if resp.status_code == 200:
                save_cycle_info(cycle_file_name,str(curr_cycle))



event = {"event_type": "end_of_cycle", "cycle": 922}
main()
#lambda_handler(event,None)