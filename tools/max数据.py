import requests
import json
import time
import base64
import datetime
import pandas as pd
import argparse
from itertools import product
import multiprocessing
start_date = datetime.date.today() - datetime.timedelta(days=2)
end_date = datetime.date.today() - datetime.timedelta(days=1)
class applovin_group(object):
    def __init__(self, api_key, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key
        self.url = 'http://r.applovin.com/maxReport'
        self.params = {
            'api_key': '{0}'.format(self.api_key),
            'start': '{0}'.format(self.start_date),
            'end': '{0}'.format(self.end_date),
            'format': 'json',
            'columns': 'day,application,ad_format,country,network,network_placement,attempts,impressions,estimated_revenue,ecpm,responses'
        }

    def get_data(self):
        data = json.loads(requests.get(self.url, params=self.params).content)
        return data['results']


api_key_list = [
    # 'Dk_e4il2vEoKZacIKkWWdTN9SQt3lEhtX4HsEVeoz0wWJQV_my6nR742GTOcE7lDscTySHcyn2k66S1vTHAAG6',
                # 'nHqgpkyXTyeot-vvB65wQmQnH3A-XFh5T903CbT6IQNZpCekoQJ0HlDfHUCyBaboZkI_XecrsJpqfq-cgpgEKy',
                # '3NeR-dGmSFxal-9OZCWsuxgvTdTe0J1Zp3UIfepCcnQC-CGyWl9grPkpb7GWfCzJKDf5QEDc_OAdil-zD6Tou0',
                # 'xyfyfBanPfEcl1RltX-BtH1pBO_OQUAJdECr597KNSZfwq4Eo_4O0WK6l3zhyhgCVe0Kn1hyfmLpJ2y8dno99o',
                # 'y3U_ha5Ox3eZd49eBb2KBFxM42hbztgnvJBDKlUUOiaIo32o5XGJUD7DCW0aQ51PTBxZJbO8u_dFZTHlfseDet'
                'AUsu4ZOKZbNQgQoKMDsUTn7RTpL3qy5CshI05aDq6BDeck0swg23vlr1ugS6RGZTeTPOnZrWiBaFvPQIOcTpSb'
                ]

def get_group_data():
    res = pd.DataFrame()
    for api_key in api_key_list:
        appl = applovin_group(api_key, start_date, end_date)
        data_json = appl.get_data()
        # print(data_json)
        dataframe = pd.DataFrame(data_json)
        if res.shape[0]==0:
            res=dataframe
        else:
            res=pd.concat((res,dataframe))


    res.columns = ['date', 'application', 'ad_format', 'country', 'network',
                       'network_placement', '请求', '展示', 'estimated_revenue',
                       'ecpm', '填充']
    res1 = res[['date','application','country', 'estimated_revenue']]
    # 剔除请求数为0的数据
    # res['请求']=res['请求'].astype('int')
    # res = res[res['请求']>0]
    # res.sort_values(by=['date','请求'],ascending=[True,False], inplace=True)
    # res['country']=res['country'].str.upper()
    return res1
res = get_group_data()
print()
# 写入飞书表格




