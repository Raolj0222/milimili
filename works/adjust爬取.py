import pandas  as pd
import requests
import datetime
from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql

# 设置获取的数据时间
# date= (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
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


if __name__ == '__main__':
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    re_df = get_installs('3dle7bbjgwhs', date)
    print(re_df.head())
    pass



