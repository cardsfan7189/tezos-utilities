import boto3
import time

def send_msg(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message)

topic_arn = "arn:aws:sns:us-east-1:917965627285:dell_check"
while True:
    with open("C:\\Users\\DREWA\\Downloads\\core temp.txt") as f:
        record = f.readline()
        if int(record) > 70000:
            send_msg("Dell may be running hot at {0}".format(int(record) / 1000),topic_arn)
            return 0
    time.sleep(30)

