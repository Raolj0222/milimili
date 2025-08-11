import json

import pandas  as pd
import requests
import datetime



headers = {
    'Authorization': 'Bearer EYPb4pZvh6pLPWL7y_Bk',
}

# 通过事件服务获取事件id
def events():
    params = {
    'event__contains': 'adjust_purchase',
    }

    response = requests.get('https://dash.adjust.com/control-center/reports-service/events',
                            params=params,
                            headers=headers)
    df = pd.DataFrame(response.json())
    df.to_excel('adjust事件.xlsx')
# 通过过滤器服务获取指标列表
def filter(headers):
    response = requests.get(
        'https://dash.adjust.com/control-center/reports-service/filters_data?required_filters=event_metrics',
        headers=headers)
    df_filters = pd.DataFrame(response.json()['event_metrics'])
    df_filters.to_excel('adjust_filter.xlsx', index=False)

# 通过报告api获取内购收入,无法获取人数
def get_neigou(headers):
    response = requests.get(
        'https://dash.adjust.com/control-center/reports-service/report?app_token__in=3dle7bbjgwhs&date_period={date}:{date}&dimensions=app,country_code&metrics=adjust_purchases_revenue',
        headers=headers)
    df = pd.DataFrame(response.json()['rows'])
    df.to_excel('neigou.xlsx')

def get_time(x):
    t=int(float(x))
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    s=str("%02d:%02d:%02d" % (h, m, s))
    return s
# 通过报告api获取每日新增，活跃，推广支出，人均活跃时长
def get_installs(app_token,date):
    # 新增使用安装数代替 指标名称 installs,活跃使用一天的DAU代替
    # 人均活跃时长使用当天同期群内的平均活跃时长

    install_response = requests.get(
        f'https://dash.adjust.com/control-center/reports-service/report?app_token__in={app_token}&date_period={date}:{date}&dimensions=app,country_code,network&metrics=installs,daus,time_spent_per_active_user_D0,network_cost',
        headers=headers)
    df = pd.DataFrame(install_response.json()['rows'])
    df['date']=date
    # 将国家地区代码转化为大写
    df['country_code']=df['country_code'].str.upper()
    # 将获取到的活跃时长秒转化为时分秒格式
    df['averageTime_PerDevice']=df['time_spent_per_active_user_d0'].apply(lambda x:get_time(x))
    return df[['date','app','country_code','network','installs','daus','averageTime_PerDevice','network_cost']]

def get_cost_campaign_network(app_token,date):
    cost_response = requests.get(
    f'https://dash.adjust.com/control-center/reports-service/report?app_token__in={app_token}&date_period={date}:{date}&dimensions=app,country_code,network,campaign_network&metrics=installs,network_cost,network_ecpi',
        headers=headers
    )
    df = pd.DataFrame(cost_response.json()['rows'])
    df['date'] = date
    if df.shape[0]>0:
    # 将国家地区代码转化为大写
        df.fillna(value='',inplace=True)
        df['country_code'] = df['country_code'].str.upper()
        return df[['date', 'app', 'country_code','network', 'campaign_network', 'installs',  'network_cost','network_ecpi']]
    else:
        return pd.DataFrame()

date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')

# app list 新项目数据需要更新
app_token_list={
    # 'Daub Farm':['g4mozkbn05c0',['US','BR','PH']],
    'Doomsday Vanguard':['3dle7bbjgwhs',['TW','KR','JP','MO','HK']],
    'Doomsday Vanguard ios':['h40ytipbjkzk',['US','JP','TW']],
    'Tales of Brave':['ex9bq7r3kcg0']
    # 'Bingo Stars':['lv2y844vjo5c',['US']],
    # 'Keno Farm':['3ycwnrahi1ds',['US','BR']],
    # 'Bingo Party':['1bqgxy43lvs0',['US']]
}
# 获取安装数据
df_install =pd.DataFrame()
for app_neme,app_token in app_token_list.items():
    re_df=get_installs(app_token[0],date)
    # re_df=re_df[re_df['country_code'].isin(app_token[1])]
    print(df_install.head())
    re_df=re_df[['date','app','country_code','network','installs','daus','averageTime_PerDevice']]
    if df_install.shape[0]==0:
        df_install=re_df
    else:
        df_install = pd.concat((df_install,re_df))


# 获取付费数据
df_cost=pd.DataFrame()
## 处理特殊渠道的支出，只能用安装和cpi计算
def func(x):
    if x['network']=='backgadon':
        cost=float(x['installs']) * 1.2
        return cost
    elif x['network']=='Okspin-wz'  and x['country_code']=='US':
        cost = float(x['installs']) * 0.75
        return cost
    elif x['network']=='Okspin-wz'  and x['country_code']=='PH':
        cost = float(x['installs']) * 0.125
        return cost
    elif x['network']=='zhizhen-cpi'  and x['country_code']=='US':
        cost = float(x['installs']) * 1.2
        return cost
    else:
        return x['network_cost']
for app_neme,app_token in app_token_list.items():
    re_df=get_installs(app_token[0],date)
    # re_df=re_df[re_df['country_code'].isin(app_token[1])]
    re_df['network_cost']=re_df.apply(lambda x:func(x),axis=1)
    re_df=re_df[['date','app','country_code','network','network_cost']]
    # re_df = re_df.query('network_cost>0')
    if df_cost.shape[0]==0:
        df_cost=re_df
    else:
        df_cost = pd.concat((df_cost,re_df))

# 入库
url_update = 'http://150.230.206.248:5000/update'
values = df_install.to_json(orient='records')
payload = {'table': 'wangzhuan_adjust_installs', 'values': values}
headers = {}
response = requests.request("post", url=url_update, headers=headers, data=payload)
print('installs_data:',response)


url_update = 'http://150.230.206.248:5000/update'
values = df_cost.to_json(orient='records')
payload = {'table': 'adjust_network_cost', 'values': values}
headers = {}
response = requests.request("post", url=url_update, headers=headers, data=payload)
print('cost_data:',response)
# if __name__ == '__main__':
#     date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
#     re_df = get_installs('3dle7bbjgwhs', date)
#     print(re_df.head())
#     pass



