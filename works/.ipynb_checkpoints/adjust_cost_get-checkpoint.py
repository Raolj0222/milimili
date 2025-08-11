import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from adjust爬取 import get_installs
from tools.mysqlhandle import Mysql_handle
import datetime
import pandas  as pd
from configs import mili_mysql

## 脚本需在下午执行，adjust支出下午三点更新！！！
## 获取数据
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
app_token_list={
    'Doomsday Vanguard':['3dle7bbjgwhs',['TW','KR','JP','MO','HK','DE','FR','TH','VN']],
    'Doomsday Vanguard ios':['h40ytipbjkzk',['US','JP','TW']],
    'Tales of Brave':['ex9bq7r3kcg0',['JP']]
}
df =pd.DataFrame()
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
    if df.shape[0]==0:
        df=re_df
    else:
        df = pd.concat((df,re_df))

#  处理MB渠道数据

def get_pingtai(x):
    if x['network']=='Google Ads ACI':
        pingtai='GG'
    elif x['network']=='Facebook (Ad Spend)':
        pingtai='FB'
    else :
        pingtai=x['network']
    return pingtai
# df['平台']=df.apply(lambda x:get_pingtai(x),axis=1)



## 写入数据库
if __name__ == '__main__':
    Mysql=Mysql_handle(mili_mysql,'data_analysis')
    data = df.to_json(orient='records')
    print(data)
    Mysql.savedata('adjust_network_cost', data)
    pass