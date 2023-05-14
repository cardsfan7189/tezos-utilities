import os

import boto3
from blockfrost import BlockFrostApi, ApiError, ApiUrls
import time

api = BlockFrostApi(
    project_id='jR3jHGaMMWNszERjQUu6x1MO0eyLRGqy',  # or export environment variable BLOCKFROST_PROJECT_ID
    # optional: pass base_url or export BLOCKFROST_API_URL to use testnet, defaults to ApiUrls.mainnet.value
    base_url=ApiUrls.mainnet.value
)

def send_email(message,topic_arn):
    sns = boto3.client('sns')
    topic_arn = topic_arn
    sns.publish(TopicArn=topic_arn,
                Message=message,
                Subject="TOSHZ Rewards check")
def lambda_handler(event,context):
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    response = api.accounts(stake_address)
    if int(response.rewards_sum) > 8920531:
        send_email("More rewards from TOSHZ",topic_arn)

try:
    topic_arn = "arn:aws:sns:us-east-1:917965627285:faso_toshz_check"
    stake_address = 'stake1uxr42vh57y8l6znevf4m8h4lnuae2pr27j4cv2k4ksgwsdgjud0g9' # toshz
    #stake_address = 'stake1uxyv072jmtk3prhekekg57qtwd89qw4sjnj73m6z6n3nmzqvm4neu' #faso
    #if (os.getenv("local_test") == "true"):
    response = api.accounts(stake_address)
    print(response)
    if int(response.rewards_sum) - int(response.withdrawals_sum) > 0:
        send_email("More rewards from TOSHZ",topic_arn)

except ApiError as e:
    print(e)
