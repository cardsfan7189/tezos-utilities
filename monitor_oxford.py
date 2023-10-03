import boto3
import sys
import json
import time
import subprocess

def load_baking_rights(cycle):
    rights_list = []
    file_name = str(cycle) + "_baking_rights.txt"
    fo = open(file_name,"r+")
    rights = json.load(fo)
    fo.close()
    for rec in rights:
        rights_list.append(rec["level"])
    return rights_list

def load_attest_rights(cycle):
    rights_list = {}

    return rights_list

current_cycle = 25

while True:
    result = subprocess.run(["octez-client", "rpc","get", "/chains/main/blocks/head"], capture_output=True, text=True)
    block = json.loads(result.stdout)
    block_cycle =  block["metadata"]["level_info"]["cycle"]
    block_cycle += 1
    if current_cycle != block_cycle:
        current_cycle = block_cycle
        baking_rights = load_baking_rights(current_cycle)
        print(baking_rights)
        attest_rights = load_attest_rights(current_cycle)
        print(current_cycle)
#fo.close()
#if line and line != "{}":
    print("{0},{1},{2}".format(block["metadata"]["baker"],block["header"]["level"],block["header"]["timestamp"]))
    print()
    time.sleep(8)