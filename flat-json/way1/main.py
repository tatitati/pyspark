#!/usr/local/bin/python3

import json
import pandas as pd
from pandas.io.json import json_normalize

with open('dataset.json') as f:
    d = json.load(f)

print(d)
# {'programs': [{'season': '1842-43', 'orchestra': 'New York Philharmonic', 'concerts': [{'Date': '1842-12-07T05:00:00Z', 'eventType': 'Subscription Season',

#lets put the data into a pandas df
#clicking on raw_nyc_phil.json under "Input Files"
#tells us parent node is 'programs'
df = json_normalize(d['programs'])
print(df.head(3))
#     season                                 orchestra  ...                                              works                                    id
# 0  1842-43                     New York Philharmonic  ...  [{'workTitle': 'SYMPHONY NO. 5 IN C MINOR, OP....  38e072a7-8fc9-4f9a-8eac-3957905c0002
# 1  1842-43                     New York Philharmonic  ...  [{'workTitle': 'SYMPHONY NO. 3 IN E FLAT MAJOR...  c7b2b95c-5e0b-431c-a340-5b37fc860b34
# 2  1842-43  Musicians from the New York Philharmonic  ...  [{'workTitle': 'EGMONT, OP.84', 'composerName'...  894e1a52-1ae5-4fa7-aec0-b99997555a37


works_data = json_normalize(
    data=d['programs'],
    record_path='works',
    meta=['id', 'orchestra','programID', 'season']
)
print(works_data.head(3))
#                           workTitle        conductorName      ID  ...              orchestra programID   season
# 0  SYMPHONY NO. 5 IN C MINOR, OP.67  Hill, Ureli Corelli  52446*  ...  New York Philharmonic      3853  1842-43
# 1                            OBERON       Timm, Henry C.  8834*4  ...  New York Philharmonic      3853  1842-43
# 2   QUINTET, PIANO, D MINOR, OP. 74                  NaN   3642*  ...  New York Philharmonic      3853  1842-43