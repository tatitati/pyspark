#!/usr/local/bin/python3

import json
import pandas as pd
from json_flatten import flatten

with open('dataset.json') as f:
    d = json.load(f)

flat_json_dict = [flatten(d) for i in d]
print(flat_json_dict)
# [
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   },
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   },
#   {
#     "ProductNum": "6000078",
#     "Properties.[0].key": "invoice_id",
#     "Properties.[0].value": "923659",
#     "Properties.[1].key": "job_id",
#     "Properties.[1].value": "296160",
#     "Properties.[2].key": "sku_id",
#     "Properties.[2].value": "312002",
#     "unitCount": "3"
#   }
# ]

df = pd.DataFrame(flat_json_dict)
print(df.head(3))
#   ProductNum Properties.[0].key  ... Properties.[2].value unitCount
# 0    6000078         invoice_id  ...               312002         3
# 1    6000078         invoice_id  ...               312002         3
# 2    6000078         invoice_id  ...               312002         3

print(df.columns)
# Index([
#     'ProductNum',
#     'Properties.[0].key',
#     'Properties.[0].value',
#     'Properties.[1].key',
#     'Properties.[1].value',
#     'Properties.[2].key',
#     'Properties.[2].value',
#     'unitCount'],
#     dtype='object'
# )