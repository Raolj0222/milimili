import pandas as pd
import numpy as np
import requests
import datetime

# url ="""https://dash.engageminds.ai/api/oka/analysis/v1/event/report"""
# url1 = """https://dash.engageminds.ai/api/oka/analysis/v1/event/report"""
# headers = {
#     'Content-Type': 'application/json;charset=UTF-8'
# }
# body= {"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"fomulars":"[]","groups":"[{\"id\":\"551396ba-1752-4c9c-9d5a-7eb3821c5e38\",\"display\":\"用户唯一id\",\"value\":\"$ssid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"1d848c14-78b3-4a5c-8e40-dcfa10cb5243\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"93e13231-5e04-46d9-915e-d87e7f13a9cd\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"id\":\"da08c690-0fde-4332-ab1f-943c59b1dab8\",\"f\":[],\"eid\":\"IAP\",\"display\":\"内购成功_new\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换收入(App币种收入))\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]","filters":"[{\"id\":\"8376c952-de3d-4281-8e2f-9f7f0927d38d\",\"eid\":\"\",\"f\":[{\"id\":\"35be43b8-7a23-43e2-9550-9671b2740df8\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false,\"d\":1}]}]"}
# # # body = {"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"fomulars":"[]","groups":"[]","tz":0,"dateType":2,"dateValue":"[\"Last 7 Days\",\"\"]","events":"[{\"id\":\"e0d574e7-7805-4f44-9cb1-62dcbb77c972\",\"f\":[],\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"total\",\"name\":\"总次数\"}}]","filters":"[]"}
# # # body1 = {"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"groups":"[]","tz":0,"metric":0,"dateValue":"[\"2024-03-15\",\"2024-03-31\"]","events":"[{\"id\":\"bcb6f420-b1b0-4e09-89fb-43d9f16412b1\",\"rType\":0,\"f\":[{\"id\":\"b3d3f76e-7e7c-424f-a975-d6074902f21f\",\"f\":{},\"columnName\":\"$sts\",\"display\":\"服务器时间\",\"selectType\":\"Date\",\"symbolType\":3,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Between\",\"symbolId\":\"C01\",\"ftv\":[\"2024-03-23\",\"2024-03-24\"],\"showCondition\":false}],\"eid\":\"#app_install\",\"display\":\"App 安装后首次启动\",\"type\":0},{\"id\":\"9bb35180-2ef8-4dcb-a8d9-90e294035366\",\"rType\":1,\"f\":[],\"eid\":\"In_appPurchases_BuySuccess\",\"display\":\"内购成功\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(pay_amount)\",\"props\":\"pay_amount\",\"propType\":0,\"type\":0}}]","filters":"[]"}
# body1 = {"appKey": "wLj6pADCXbszRclanbaKReskz61DK6YD", "pubAppId": 25624, "fomulars": "[]",
#          "groups": "[{\"id\":\"b8207880-5e3c-4a33-9887-4499e45b4403\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2},{\"id\":\"0b51c889-287c-48d2-b345-9ef9b05fe30d\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]",
#          "tz": 0, "dateType": 2, "dateValue": "[\"Yesterday\",\"\"]",
#          "events": "[{\"id\":\"f4e7ba43-d019-4874-8065-258fcbf2ef0d\",\"f\":[],\"eid\":\"#app_install\",\"display\":\"App 安装后首次启动\",\"type\":0,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"2c496060-39cc-4a83-bcb3-cf14b3a41677\",\"f\":[],\"num\":0,\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"8b275f4a-0d2f-4223-88a8-9a267e535c4a\",\"f\":[],\"num\":0,\"eid\":\"#app_exit\",\"display\":\"App关闭\",\"type\":0,\"countType\":{\"value\":\"avgPrpo\",\"name\":\"按...求人均值(事件时长（分钟）)\",\"props\":\"#vp@duration_m\",\"propType\":0,\"type\":2,\"virtualSql\":\"event.#duration/60\"}}]",
#          "filters": "[]"}
#
# df = requests.post(url1,headers=headers,json=body1)
# print(pd.DataFrame(df.json()['data']['data']))

class EM_api():
    def __init__(self):
        self.herders={
    'Content-Type': 'application/json;charset=UTF-8'
}
    def get_event(self,url,body):

        df= requests.post(url,self.herders,json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print(df.json()['msg'])
            print('未获取到数据')
            return pd.DataFrame()
    def get_liucun(self,url,body):
        df = requests.post(url, self.herders, json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print('未获取到数据')
            return pd.DataFrame()
    def get_ltv(self,url,body):
        df = requests.post(url, self.herders, json=body)
        if df.json()['msg'] == 'OK':
            return df.json()['data']['data']
        else:
            print('未获取到数据')
            return pd.DataFrame()
# if __name__ == '__main__':
#     em = EM_api()
#     res=em.get_event(url='',body={})
#     print(res.head())