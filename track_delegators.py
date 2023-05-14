import requests
import math
from datetime import datetime
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6&cycle=525&limit=8000"
base_URL = "https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/delegators"
#base_URL = "https://api.tzkt.io/v1/rights?baker=tz1S8MNvuFEUsWgjHvi3AxibRBf388NhT1q2&limit=8000&cycle="
#base_URL = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/"
resp = requests.get("https://api.tzkt.io/v1/accounts/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6")
data = resp.json()
bakerBalance = data["balance"]
resp = requests.get(base_URL)
data = resp.json()
totalStake = bakerBalance
for rec in data:
    totalStake = totalStake + rec["balance"]
    print(rec)
print(totalStake)
bakerAndDelegators = {"tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6" : bakerBalance / totalStake}
for rec in data:
    percentage = rec["balance"] / totalStake
    bakerAndDelegators.update({rec["address"] : percentage})
print(bakerAndDelegators)
base_URL = "https://api.tzkt.io/v1/rewards/bakers/tz1ffYUjwjduZkoquw8ryKRQaUjoWJviFVK6/520"
resp = requests.get(base_URL)
data = resp.json()
print(data)


