import requests
import datetime
import json
import time
import pymysql
import pandas as pd
import json
import datetime
import requests
import os


def get_data(apikey, organizationId, start, end):
    startdate = start + 'T00:00:00.000Z'
    enddate = end + 'T00:00:00.000Z'
    print(startdate, enddate)

    url1 = 'https://stats.unityads.unity3d.com/organizations/%s/reports/acquisitions?start=%s&end=%s&scale=day&splitBy=campaignSet,campaign,platform,country&fields=timestamp,campaignSet,campaign,platform,country,installs,spend,views,clicks&apikey=%s' % (
    organizationId, startdate, enddate, apikey)
    re1 = requests.get(url1).text
    relist1 = re1.splitlines()[1:]
    print('data:', relist1)

    datalist1 = []
    for re in relist1:
        re = re.split(',')
        datadict = {
            'date': re[0][0:10],
            'campaignset_id': re[1].replace('"', ''),
            'appname': re[2].replace('"', ''),
            'push_channel': 'unity',
            'campaign_id': re[-7].replace('"', ''),
            'campaign_name': re[-6].replace('"', ''),
            'push_country': re[-5].replace('"', ''),
            'install': re[-4],
            'spend_money': re[-3],
            'views': re[-2],
            'clicks': re[-1]
        }
        datalist1.append(datadict)
    print(datalist1)
    return datalist1


apikey = '025b14649d86a980f0fbcdd66c845cde5e79a6d724c934ceccd896a4c88ad5c3'
organizationId = '5f9787dee5c05f11b2c52f56'
