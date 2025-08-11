# coding=utf-8
import pandas as pd
import numpy as np
import requests
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()

# 读取飞书数据，获取需要处理的用户ID
df=feishu.read_tab('F2VHsUMQahF6d3tR9AUcKDD6nxh','c5961b','A1:D10000')
# 删除null值
df=df[df['时间'].notnull()]
user_id_list = df['游戏id'].tolist()

# 组装用户列表
users = ['"' + i + '"' for i in user_id_list]

users_str = str(users).replace("'",'')
users_str = str(users_str).replace('"\'','"')
users_str = str(users_str).replace(' ','')

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
        "dateValue":"[\"2024-05-01\",\"Today\"]",
        "events":"[{\"id\":\"470cae11-d778-4e75-a840-d7a0fdb8edf8\",\"f\":[{\"id\":\"f6c3dd9f-8518-4422-b263-c8f1d1428baa\",\"f\":[{\"id\":\"f38258c0-520c-4705-9f46-c9b1f2a0c87e\",\"columnName\":\"level_name\",\"display\":\"level_name\",\"displayEn\":\"level_name\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"ChapterName_10\"],\"showCondition\":false,\"noPropsPop\":true},{\"id\":\"18af722e-ff2b-4671-aadb-76cae837f2b3\",\"columnName\":\"if_clearance\",\"display\":\"是否通关\",\"displayEn\":\"是否通关\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"1\"],\"showCondition\":false}],\"type\":0,\"relation\":\"and\"}],\"eid\":\"Game_Level_Finish\",\"display\":\"Game_Level_Finish\",\"displayEn\":\"Game_Level_Finish\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\",\"nameEn\":\"Unique\"}}]",
        "filters":"[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":%s,\"showCondition\":false,\"d\":1}]}]"% users_str}
# print(body)
body1 = {"appKey":"P3ewIzs9gigHVSRLXqvF4xjubPwDz5MZ","pubAppId":175680,
        "fomulars":"[]","groups":"[{\"id\":\"571cf898-1ca1-4684-b501-866c294141a9\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"value\":\"$cdid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]",
        "tz":0,"dateType":4,"lang":"cn",
        "dateValue":"[\"2024-05-01\",\"Today\"]",
        "events":"[{\"id\":\"470cae11-d778-4e75-a840-d7a0fdb8edf8\",\"f\":[{\"id\":\"f6c3dd9f-8518-4422-b263-c8f1d1428baa\",\"f\":[{\"id\":\"f38258c0-520c-4705-9f46-c9b1f2a0c87e\",\"columnName\":\"level_name\",\"display\":\"level_name\",\"displayEn\":\"level_name\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"ChapterName_10\"],\"showCondition\":false,\"noPropsPop\":true},{\"id\":\"18af722e-ff2b-4671-aadb-76cae837f2b3\",\"columnName\":\"if_clearance\",\"display\":\"是否通关\",\"displayEn\":\"是否通关\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":0,\"type\":0,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"1\"],\"showCondition\":false}],\"type\":0,\"relation\":\"and\"}],\"eid\":\"Game_Level_Finish\",\"display\":\"Game_Level_Finish\",\"displayEn\":\"Game_Level_Finish\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\",\"nameEn\":\"Unique\"}}]",
        "filters":"[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"e11cd179-b0b9-45fa-b9d3-97e33062a433\",\"b85a3cb4-5bd5-48cf-8d7e-a0056a7f3ea9\",\"436ee3f3-7cc3-494f-a14d-0a1e2237cd77\",\"9ab9f653-6682-4e9d-897b-8b2c13266a74\",\"3eecb82e-7b24-4a1e-8220-b535fc952dfe\",\"7612ab41-c147-45f2-9b89-7254293650be\",\"f00f6b0d-3c92-4db1-a437-dd309e79d421\",\"a27454c6-edc5-4f3c-90a7-39586d2fffea\",\"f8cff149-de3a-4b47-b20c-ddb52e1a993d\",\"4de58d21-9567-47be-8314-e97410f60c6d\",\"c9c14580-4fcf-4a2f-8943-8559cb7bf65b\",\"719d6dc4-689a-4fb6-8723-ee2d899d9113\",\"58d11790-091b-467b-acd8-2e96c8a65429\",\"0cc0f7fa-d122-483b-bc9c-9a8dcc9cc2ff\",\"4a5daa3f-57dd-4dc6-917a-88feb1b444b7\"],\"showCondition\":false,\"d\":1}]}]"}
# str1= "[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":%s,\"showCondition\":false,\"d\":1}]}]"% users_str
# str2= "[{\"id\":\"cf1e502b-d9bb-42e2-990a-356483f97791\",\"eid\":\"\",\"f\":[{\"id\":\"94c0decc-b6ab-4ed6-9972-0306174eb2bc\",\"f\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is\",\"symbolId\":\"B01\",\"ftv\":[\"e11cd179-b0b9-45fa-b9d3-97e33062a433\",\"b85a3cb4-5bd5-48cf-8d7e-a0056a7f3ea9\",\"436ee3f3-7cc3-494f-a14d-0a1e2237cd77\",\"9ab9f653-6682-4e9d-897b-8b2c13266a74\",\"3eecb82e-7b24-4a1e-8220-b535fc952dfe\",\"7612ab41-c147-45f2-9b89-7254293650be\",\"f00f6b0d-3c92-4db1-a437-dd309e79d421\",\"a27454c6-edc5-4f3c-90a7-39586d2fffea\",\"f8cff149-de3a-4b47-b20c-ddb52e1a993d\",\"4de58d21-9567-47be-8314-e97410f60c6d\",\"c9c14580-4fcf-4a2f-8943-8559cb7bf65b\",\"719d6dc4-689a-4fb6-8723-ee2d899d9113\",\"58d11790-091b-467b-acd8-2e96c8a65429\",\"0cc0f7fa-d122-483b-bc9c-9a8dcc9cc2ff\",\"4a5daa3f-57dd-4dc6-917a-88feb1b444b7\"],\"showCondition\":false,\"d\":1}]}]"
# print(str1)
# print(str2)
data = requests.post(url, headers=headers, json=body)
res = pd.DataFrame(data.json()['data']['data'])
res_cdid = res[['cdid']]
res_cdid['通关'] = '是'

# 覆盖写入飞书表
value_range = res_cdid.shape[0]+1
df_list = np.array(res_cdid).tolist()
feishu.insert_tab('F2VHsUMQahF6d3tR9AUcKDD6nxh','NtCep1',f'A2:B{value_range}',df_list)


