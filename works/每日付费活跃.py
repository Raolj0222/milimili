import datetime
import sys
import requests
import pandas as pd
# 勇者付费活跃
url = 'https://dash.engageminds.ai/api/oka/analysis/v1/event/report'
headers = {
        'Content-Type': 'application/json;charset=UTF-8'
}
body1 = {"appKey":"P3ewIzs9gigHVSRLXqvF4xjubPwDz5MZ","pubAppId":175680,
        "fomulars":"[]",
        "groups":"[{\"id\":\"5f310ac6-1aa3-4761-81b9-63b55f2edd16\",\"display\":\"国家/地区\",\"displayEn\":\"Country/Region\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2,\"customBucket\":null},{\"id\":\"c2a0d6f7-5474-4ad2-ade3-406c0e2a66aa\",\"display\":\"操作系统\",\"displayEn\":\"Operating System\",\"value\":\"$os\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":1}]","tz":0,"dateType":2,"lang":"cn",
        "dateValue":"[\"Last 14 Days\",\"\"]",
        "events":"[{\"id\":\"01e93006-0a77-4e81-a7d1-a05d595df71e\",\"f\":[],\"eid\":\"login\",\"display\":\"登录\",\"displayEn\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\",\"nameEn\":\"Unique\"}}]",
        "filters":"[{\"id\":\"49889051-a47f-4535-95d9-b4a3877026ee\",\"eid\":\"\",\"f\":[{\"id\":\"1b8b6871-21fb-4d9c-b174-674f7c5f29df\",\"f\":{},\"columnName\":\"total_pay_num\",\"display\":\"total_pay_num\",\"displayEn\":\"total_pay_num\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":0,\"category\":1,\"calcuSymbol\":\"Is numeric\",\"symbolId\":\"A09\",\"ftv\":[],\"showCondition\":false,\"d\":1}]}]"}

data1 = requests.post(url, headers=headers, json=body1)
data_df=data1.json()['data']['data']
res_tales = pd.DataFrame()
for data in data_df:
    country = data['country']
    metric = data['metric']
    os =data['os']
    # Filter out date entries and create a DataFrame
    date_entries = {k: v for k, v in data.items() if '-' in k}
    df = pd.DataFrame(date_entries.items(), columns=['date', 'users'])
    # Add the country and metric information to each row
    df['country'] = country
    df['os'] = os
    if res_tales.shape[0]==0:
        res_tales = df
    else:
        res_tales=pd.concat((res_tales,df))
res_tales['appname'] = res_tales.apply(lambda x: 'Tales of Brave' if x['os'] !='iOS' else 'Tales of Brave ios',axis=1)
print(res_tales.head())

# 滋滋付费活跃
body2 ={"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD",
        "pubAppId":25624,"fomulars":"[]",
        "groups":"[{\"id\":\"ed192f09-ca46-4d4d-9460-649b325a04fd\",\"display\":\"国家/地区\",\"displayEn\":\"Country/Region\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"0f9f6e69-8a32-41e7-a4da-8ebee2b206e6\",\"display\":\"操作系统\",\"displayEn\":\"Operating System\",\"value\":\"$os\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":1}]","tz":0,"dateType":2,"lang":"cn",
        "dateValue":"[\"Last 14 Days\",\"\"]",
        "events":"[{\"id\":\"0ace8059-b927-4305-a36c-5294a72bbae1\",\"f\":[],\"eid\":\"login\",\"display\":\"登录\",\"displayEn\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\",\"nameEn\":\"Unique\"}}]",
        "filters":"[{\"id\":\"40968edc-0334-4d6b-a8bc-be8fbb2c792a\",\"eid\":\"\",\"f\":[{\"id\":\"056797ac-4242-4d25-9c2a-48ae7ca9631f\",\"f\":{},\"columnName\":\"total_pay_amount_server\",\"display\":\"用户累计付费金额（可用）\",\"displayEn\":\"用户累计付费金额（可用）\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":1,\"calcuSymbol\":\"Is set\",\"symbolId\":\"B05\",\"ftv\":[],\"showCondition\":false,\"d\":1}]}]"}
data2 = requests.post(url, headers=headers, json=body2)
data_df2=data2.json()['data']['data']
res_dooms = pd.DataFrame()
for data in data_df2:
    country = data['country']
    metric = data['metric']
    os =data['os']
    # Filter out date entries and create a DataFrame
    date_entries = {k: v for k, v in data.items() if '-' in k}
    df = pd.DataFrame(date_entries.items(), columns=['date', 'users'])
    # Add the country and metric information to each row
    df['country'] = country
    df['os'] = os
    if res_dooms.shape[0]==0:
        res_dooms = df
    else:
        res_dooms=pd.concat((res_dooms,df))
# 去除其他操作系统
res_dooms= res_dooms[res_dooms['os'].isin(['iOS','Android'])]

res_dooms['appname'] = res_dooms.apply(lambda x: 'Doomsday Vanguard' if x['os'] !='iOS' else 'Doomsday Vanguard ios',axis=1)
# 合并入库
data = pd.concat((res_tales,res_dooms))
# print(data)
# 入库
data_res = data[['date','appname','country','users']].to_json(orient='records')
url_update = 'http://150.230.206.248:5000/update'
payload = {'table': 'ng_user_active', 'values': data_res}
headers = {}
response = requests.request("post", url=url_update, headers=headers, data=payload)
print(response)
