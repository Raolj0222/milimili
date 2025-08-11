import sys
import requests
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
import pandas as pd
import datetime
tab_path='DPpOsWNQ9h4xOitKjDicrWwBn3h'
sheet_path='szakJY'
range_list='A1:K1000'
df=pd.DataFrame()
for i in range(4):
    date = (datetime.date.today() - datetime.timedelta(days=i+1)).strftime('%Y-%m-%d')
    df1=feishu.read_tab(tab_path,sheet_path,range_list)
    df1=df1.query(f'日期=="{date}"')
    df1['appname']='Keno Farm'
    df1['country']='BR'
    if df.shape[0]==0:
        df=df1
    else:
        df=pd.concat((df,df1))
# 广告类型
def func(x):
    if x['广告类型']=='激励':
        return 'Rewarded Video'
    elif x['广告类型']=='插屏':
        return 'Interstitial'
    else:
        return x['广告类型']
df['ad_type']=df.apply(lambda x:func(x),axis=1)
df.columns=['date','产品','广告类型','广告位','requestCount','imprCount','clickCount','填充率','展示率','ecpm','revenue','appname','country','ad_type']
df['appid']='1'
df['placementid']='1'
df['placementname']='1'
res1=df[['date', 'appid', 'appname', 'country', 'ad_type', 'placementid', 'placementname', 'requestCount', 'imprCount', 'clickCount', 'revenue']]
res2=df[['date', 'appid', 'appname', 'country', 'ad_type', 'placementid', 'placementname', 'requestCount', 'imprCount', 'clickCount', 'revenue']]
res2['country']='all'
res=pd.concat((res1,res2))
#存入 sr_topon表中
print(res)
# mysql.insert_data(res,'sr_topon')


# 将数据保存到数据库
def savedata(table, savedata_list):
    values = savedata_list
    url_update = 'http://150.230.221.78:5000/update'
    payload = {'table': table, 'values': values}
    headers = {}
    response = requests.request("post", url=url_update, headers=headers, data=payload)
    print(response)
# 转换DataFrame为JSON格式并保存到数据库
tt = res.to_json(orient='records')  # 将数据框转换为 JSON 格式
table = "sr_topon"  # 数据库表名
savedata(table, tt)  # 保存数据到数据库
