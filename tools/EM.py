import pandas as pd
import numpy as np
import requests
import datetime

class EM_api():
    def __init__(self):
        self.herders={
    'Content-Type': 'application/json;charset=UTF-8'
}
    def get_event(self,url,body):

        df= requests.post(url,self.herders,json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print(df.json()['msg'])
            print('未获取到数据')
            return pd.DataFrame()
    def get_liucun(self,url,body):
        df = requests.post(url, self.herders, json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print('未获取到数据')
            return pd.DataFrame()
    def get_ltv(self,url,body):
        df = requests.post(url, self.herders, json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print('未获取到数据')
            return pd.DataFrame()
# if __name__ == '__main__':
#     em = EM_api()
#     res=em.get_event(url='',body={})
#     print(res.head())