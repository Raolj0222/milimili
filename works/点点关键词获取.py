import pandas as pd

import json

import requests

url = 'https://api.diandian.com/pc/app/v3/word/app/aso_words'
header = {
             'Content-Type': 'application/json',
            'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.36'
         }
json_data={"app_id": "kxupur8o87gpoaw",
"base_time": 1701326204,
"change_type": -1,
"compare_time": 1701239804,
"corrected_type": 0,
"country_id": 26,
"device_id": 1,
"follow_words": "",
"k": "c11DBAhAd0kCXW1VSwUNQHdJAlxsWkEFDUN2TVgXKxwGRwEeeE5UHzENBB0fV2lIA1trWEcMDUF0URVJdwgXHR9XaVZFXHEbHEddXiEJQ0A/HxxqTh4yHUA=",
"market_id": 1,
"system_type": 4}
r=requests.post(url,headers=header,json=json_data)
print(r.text)