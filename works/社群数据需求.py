# coding=utf-8
import pandas as pd
import numpy as np
import requests
import datetime
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
start = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
# 读取飞书数据，获取需要处理的用户ID
df=feishu.read_tab('F2VHsUMQahF6d3tR9AUcKDD6nxh','c5961b','A1:G10000')

df.columns = ['时间','discordID','游戏id','操作系统','国家','判断值','是否发放']

# df['时间'] = df['时间'].str.slice(0,10)

df.to_excel('社群特斯拉数据.xlsx')
# 读取前7天的数据
df = df[df['时间']>=start]

user_id_list = df['游戏id'].tolist()
# 组装用户列表
users = ['"' + str(i) + '"' for i in user_id_list]

users_str = str(users).replace("'",'')
users_str = str(users_str).replace('"\'','"')
users_str = str(users_str).replace(' ','')
print(users_str)
# filters = "[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":%s,\"showCondition\":false,\"d\":1}]}]"% users_str
# filters=filters.replace(' ','')
# print(filters)
# 获取em数据
headers = {
    'Content-Type': 'application/json;charset=UTF-8'
}
url = 'https://dash.engageminds.ai/api/oka/analysis/v1/event/report'
body = {"appKey":"P3ewIzs9gigHVSRLXqvF4xjubPwDz5MZ","pubAppId":175680,
        "fomulars":"[]","groups":"[{\"id\":\"571cf898-1ca1-4684-b501-866c294141a9\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"value\":\"$cdid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]",
        "tz":0,"dateType":4,"lang":"cn",
        "dateValue":"[\"2025-01-01\",\"Today\"]",
        "events":"[{\"id\":\"470cae11-d778-4e75-a840-d7a0fdb8edf8\",\"f\":[{\"id\":\"f6c3dd9f-8518-4422-b263-c8f1d1428baa\",\"f\":[{\"id\":\"f38258c0-520c-4705-9f46-c9b1f2a0c87e\",\"columnName\":\"level_name\",\"display\":\"level_name\",\"displayEn\":\"level_name\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"ChapterName_10\"],\"showCondition\":false,\"noPropsPop\":true},{\"id\":\"18af722e-ff2b-4671-aadb-76cae837f2b3\",\"columnName\":\"if_clearance\",\"display\":\"是否通关\",\"displayEn\":\"是否通关\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"1\"],\"showCondition\":false}],\"type\":0,\"relation\":\"and\"}],\"eid\":\"Game_Level_Finish\",\"display\":\"Game_Level_Finish\",\"displayEn\":\"Game_Level_Finish\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\",\"nameEn\":\"Unique\"}}]",
        "filters":"[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":%s,\"showCondition\":false,\"d\":1}]}]"% users_str}

data = requests.post(url, headers=headers, json=body)
print(data.json())
res = pd.DataFrame(data.json()['data']['data'])
print(res)
res['是否通关'] = res['A:Game_Level_Finish-总人数']
res_cdid = res.query('是否通关==1')[['event.cdid']]
res_cdid['通关'] = '是'

# 覆盖写入飞书表
value_range = res_cdid.shape[0]+1
df_list = np.array(res_cdid).tolist()
feishu.insert_tab('F2VHsUMQahF6d3tR9AUcKDD6nxh','NtCep1',f'A2:B{value_range}',df_list)
#

