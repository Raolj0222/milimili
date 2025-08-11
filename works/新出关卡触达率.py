import pandas as pd
import numpy as np
import requests

def get_chapter_enter_rate(chapter_id):
    url ='https://dash.engageminds.ai/api/em/analysis/funnel//v1/report'
    herders = {
        'Content-Type': 'application/json;charset=UTF-8'
    }
    body ={"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"tz":0,
           "countType":1,"timeWindow":"[7,2]","lang":"cn",
           "dateValue":"['This Month','']",
           "events":"[{'id':'0ced3c68-c631-4334-9861-2d364efd26ad','f':[{'id':'dd14b219-167e-41df-97b5-280c74f42fb0','f':{},'columnName':'level_id','display':'level_id','displayEn':'level_id','selectType':'String','symbolType':2,'propType':0,'type':0,'category':0,'calcuSymbol':'Is','symbolId':'B01','ftv':[%s],'showCondition':false}],'eid':'Game_Level_enter','display':'开始闯关','displayEn':'开始闯关','type':1,'relation':null},"
                    "{'id':'b78bfb7e-c9b9-4978-8f06-b487f56e996a','f':[{'id':'7e0e16bd-550c-4a74-9fb2-9c0d83b19b15','f':{},'columnName':'level_id','display':'level_id','displayEn':'level_id','selectType':'String','symbolType':2,'propType':0,'type':0,'category':0,'calcuSymbol':'Is','symbolId':'B01','ftv':[%s],'showCondition':false}],'num':0,'eid':'Game_Level_enter','display':'开始闯关','displayEn':'开始闯关','type':1}]"%(str(chapter_id),str(chapter_id+1)),
           "filters":"[{'id':'f96bd733-5ef3-4b59-b5a1-96528382d93b','eid':'','f':[{'id':'865a22e3-2f2f-4070-944f-343d896d72c6','f':{},'columnName':'$country','display':'国家/地区','displayEn':'Country/Region','selectType':'String','symbolType':2,'propType':1,'type':1,'category':0,'calcuSymbol':'Is not','symbolId':'B02','ftv':['CN'],'showCondition':false,'d':1},{'id':'3319f252-a8cb-40c7-93de-652d37b4e998','f':{},'columnName':'$os','display':'操作系统','displayEn':'Operating System','selectType':'Number','symbolType':1,'propType':1,'type':1,'category':0,'calcuSymbol':'Equal','symbolId':'A01','ftv':['0','1'],'showCondition':false,'d':1}],'relation':'and'}]"}
    data = requests.post(url, headers=herders, json=body)

    if data.json()['code']==0:
        # print(data.json()['data']['data'])
        # 提取数据
        login = data.json()['data']['data'][0]['Game_Level_enter']
        not_passed = data.json()['data']['data'][0]['Game_Level_enter_loss_1']
        passed = data.json()['data']['data'][0]['Game_Level_enter_conv_1']

        # 创建 DataFrame
        res = pd.DataFrame({
            '关卡ID':[chapter_id],
            '登录': [login],
            '未闯关': [not_passed],
            '闯关': [passed]
        })

        print(res)
    else:
        print('未获取到数据', data.json()['code'])
        res = pd.DataFrame()

    return res

res = pd.DataFrame()
for i in range(160,190):
    data=get_chapter_enter_rate(i)
    if res.shape[0]==0:
        res= data
    else:
        res = pd.concat((res,data))
res.to_excel('3日闯关率.xlsx',index=False)

