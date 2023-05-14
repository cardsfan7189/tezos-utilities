import requests
import boto3

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message)

def lambda_handler(event,context):
    try:
        URL = "http://68.13.59.242:12798/index.html"
        topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
        resp = requests.get(URL, timeout=10)
        print(resp.status_code)
    except requests.ConnectionError:
        send_msg("ip may need updating",topic_arn)

URL = "http://68.13.59.242:12798/index.html"
topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
try:
    resp = requests.get(URL, timeout=10)
    print(resp.status_code)
except requests.ConnectionError:
    send_msg("ip may need updating",topic_arn)
