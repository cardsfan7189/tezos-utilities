import requests
import math
from datetime import datetime
import boto3

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message)

def lambda_handler(event,context):
    print("Lambda handler")

resp = requests.get("https://api.tzkt.io/v1/head")
data = resp.json()

cycle = data["cycle"]
level = int(data["level"]) - 30

topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
resp = requests.get("https://api.tzkt.io/v1/rights?type=baking&baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=" + str(cycle) + "&status=realized")
data = resp.json()
for rec in data:
    if rec["round"] > 0 and rec["level"] > level:
        send_msg("Round 1 or later block was baked. May need to send more to payout address", topic_arn)
