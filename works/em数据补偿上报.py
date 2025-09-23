import pandas as pd
import paramiko, socket, select, threading, pymysql
import requests
import datetime
# 获取数据-飞书
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
tab_path = 'A97NsY9GiheYVEtYSZMc3KWLnZg'
sheetname = 'SoTi1b'
df=feishu.read_tab(tab_path,sheetname,'A1:D10000')
df = df.query('消耗>0')
# 获取最后7行数据
df = df.iloc[-7:]
df['sts'] = pd.to_datetime(df['日期']).astype('int64') // 10**6
df['user_id'] = df['日期'].astype('string')
df['更新记录'] = df['更新记录'].apply(lambda x : x if x else '暂无')
# 组装数据
body_list =[]
for index,row in df.iterrows():
    dic = {
    "ts": row["sts"],
    "appk":'UVxs42Ho3FDdQ1wLQxIkk1J4r00HUwwX',
    "uid": row['user_id'],
    "os":1,
    "country":'',
    "events": [
      {
        "ts": row['sts'],
        "cdid": row['user_id'],
        "eid": "#user_set",
        "props": {
            'cpi':row['消耗'],
            'log':row['更新记录']
        }}
    ]
    }
    body_list.append(dic)
headers = {
    'Content-Type': 'application/json;charset=UTF-8'
}
url = 'https://a.engageminds.ai/s2s/es'
response=requests.post(url, headers=headers, json=body_list)
print(response)


