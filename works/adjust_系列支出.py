import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from adjust爬取 import get_cost_campaign_network
from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql
Mysql=Mysql_handle(mili_mysql,'data_analysis')
import datetime
import pandas  as pd


## 脚本需在下午执行，adjust支出下午三点更新！！！
## 获取数据
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
# start_date = (datetime.date.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
app_token_list={
    'Doomsday Vanguard':['3dle7bbjgwhs',['TW','KR','JP','MO','HK','VN','TH','DE','FR']],
    'Doomsday Vanguard ios':['h40ytipbjkzk',['JP']],
    'Tales of Brave':['ex9bq7r3kcg0',['JP','TW']]
    # 'Bingo Stars':['lv2y844vjo5c',['US']]
}


def func(x):
    if x['network']=='backgadon':
        cost=float(x['installs']) * 1.2
        return cost
    elif x['network']=='Okspin-wz' and x['country_code']=='US':
        cost = float(x['installs']) * 0.75
        return cost
    elif x['network']=='Okspin-wz' and x['country_code']=='PH':
        cost = float(x['installs']) * 0.125
        return cost
    elif x['network']=='zhizhen-cpi'  and x['country_code']=='US':
        cost = float(x['installs']) * 1.2
        return cost
    else:
        return x['network_cost']
# 读取近 (n -1) 天的系列支出数据

def get_cost(day):
    df = pd.DataFrame()
    for i in range(1,day):
        start_date=(datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        for app_neme,app_token in app_token_list.items():
            re_df=get_cost_campaign_network(app_token[0],start_date)
            if re_df.shape[0]>0:
                # re_df=re_df[re_df['country_code'].isin(app_token[1])]
                re_df['network_cost'] = re_df.apply(lambda x: func(x), axis=1)
                re_df=re_df[['date','app','country_code','network','campaign_network','installs','network_cost','network_ecpi']]
            else:
                pass

            if df.shape[0]==0:
                df=re_df
            else:
                df = pd.concat((df,re_df))
    # 单独处理fb渠道 Facebook
    df['network']=df['network'].apply(lambda x: 'FB' if 'Facebook' in x else x)
    df['network']=df['network'].apply(lambda x: 'FB' if 'Instagram Installs' in x else x)
    df['network_cost']=df['network_cost'].astype('float')
    df['installs']=df['installs'].astype('int')

    def get_nerwork(x):
        if 'weads' in str(x['campaign_network']):
            network='weads'
        elif 'youdao' in str(x['campaign_network']):
            network ='youdaozhixuan'
        elif 'apolloads' in str(x['campaign_network']):
            network = 'apolloads'
        elif 'runbridge'  in str(x['campaign_network']):
            network = 'runbridge'
        elif 'palmax'  in str(x['campaign_network']).lower():
            network = 'Palmax'
        elif 'wezonet'  in str(x['campaign_network']).lower():
            network = 'wezonet'
        elif 'GATHERONE' in str(x['campaign_network']).upper():
            network = 'GatherOne'
        elif 'HBY' in str(x['campaign_network']).upper():
            network = 'HBY'
        elif 'SR' in str(x['campaign_network']).upper():
            network = '3YData'
        elif 'madhouse' in str(x['campaign_network']).lower():
            network = 'madhouse'
        elif 'SINOINTERACTIVE' in str(x['campaign_network']).upper():
            network='SINOINTERACTIVE'
        elif 'himobi' in str(x['campaign_network']).lower():
            network = 'Himobi'
        elif 'admax' in str(x['campaign_network']).lower():
            network = 'Admax'
        elif 'DH' in str(x['network']).upper() or '3D AD' in str(x['network']).upper() or str(x['network']) in ['Persona.ly','Presco','Fukuroulabo','ZucksAF','Nend','Unicorn']:
            network='DH'
        else :
            network = x['network']
        return network
    df['渠道']=df.apply(lambda x:get_nerwork(x),axis=1)

    def get_pingtai(x):
        if 'FB' in str(x['campaign_network']).upper():
            pingtai='FB'
        elif 'GG' in str(x['campaign_network']).upper() or 'Google Ads ACI'==str(x['network']) or 'Google' in str(x['campaign_network']):
            pingtai ='GG'
        elif 'TT' in str(x['campaign_network']).upper():
            pingtai = 'TT'

        elif 'HBY' in str(x['campaign_network']).upper() or 'SR' in str(x['campaign_network']).upper():
            pingtai = 'Apple Search Ads'
        else :
            pingtai = x['渠道']
        if 'TWITTER' in str(x['campaign_network']).upper():
            pingtai = 'Twitter'
        if 'TWITTER' in str(x['渠道']).upper():
            pingtai = 'Twitter'
        if 'AMOAD' in str(x['network']).upper():
            pingtai = 'Amoad'
        if 'LINE' in str(x['network']).upper():
            pingtai = 'line'
        if 'SEEDAPP' in str(x['network']).upper():
            pingtai = 'Seedapp'
        if '3D AD' in str(x['network']).upper():
            pingtai = '3D AD'
        return pingtai

    def get_country(x):
        country = x['country_code']
        if 'GLOBAL' in str(x['campaign_network']).upper() and 'CHINESE' in str(x['campaign_network']).upper():
            country = 'global-cn'
        if 'GLOBAL' in str(x['campaign_network']).upper() and 'CHINESE' not in str(x['campaign_network']).upper():
            country = 'global-us'
        if 'zizi-jp-0124-1.0' in str(x['campaign_network']).lower() or 'zizi-jp-ac1.0' in str(x['campaign_network']).lower():
            country = 'JP1.0'
        if 'RBO_MS' in str(x['campaign_network']).upper():
            country = 'global-test'
        if '-tv' in str(x['campaign_network']).lower():
            country = 'JP-TV'
        if '-img' in str(x['campaign_network']).lower():
            country = 'JP-img'
        return country


    df['平台']=df.apply(lambda x:get_pingtai(x),axis=1)
    # df['country_code']=df.apply(lambda x:get_country(x),axis=1)
    # 根据系列汇总
    df=df.groupby(by=['date','app','country_code','平台','渠道','campaign_network'])[['installs','network_cost']].agg({
        'installs':'sum',
        'network_cost':'sum'
    }).reset_index()
    # 排除自然量渠道
    # df=df.query('渠道!="Organic"&渠道!="Google Organic Search"')
    # 将系列转化为大写
    df['campaign_network']=df['campaign_network'].str.upper()
    df.columns=['date','app','country_code','平台','network','campaign_network','installs','network_cost']
    return df

test=get_cost(3)
print(test.head())
# test.to_excel('系列支出数据.xlsx')

