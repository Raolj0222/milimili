import pandas as pd
import re
import requests
import datetime
endtime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
starttime =(datetime.date.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
nowtime=datetime.date.today().strftime('%Y-%m-%d')
headers = {
    'Content-Type': 'application/json'
}
params = {
    'token': 'bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0',
}
# body={"eventView":{"collectFirstDay":1,"endTime":"2023-10-18 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":"2023-10-19","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":"2023-10-19","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-19","subTableType":"","tableType":"user"},{"columnDesc":"adjust_campaign_name","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-19","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":"2023-10-11 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"first_device_add","eventNameDisplay":"","simultaneous_display":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"ad_impEndRevenue","eventNameDisplay":"","filts":[],"quota":"#vp@af_revenue","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":25}
# body = {"eventView":{"collectFirstDay":1,"endTime":"2023-10-19 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":"2023-10-20","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":"2023-10-20","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-20","subTableType":"","tableType":"user"},{"columnDesc":"adjust_campaign_name","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-20","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":"2023-10-12 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"first_device_add","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"offerwall_success","eventNameDisplay":"","filts":[],"quota":"offerwall_money","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":25}
body = {"eventView":{"collectFirstDay":1,"endTime":"2023-10-22 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":"2023-10-23","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":"2023-10-23","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-23","subTableType":"","tableType":"user"},{"columnDesc":"adjust_campaign_name","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":"2023-10-23","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":"2023-10-15 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"first_device_add","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"redeem_success","eventNameDisplay":"","filts":[{"columnDesc":"status","columnName":"status","comparator":"equal","filterType":"SIMPLE","ftv":["1"],"specifiedClusterDate":"2023-10-23","subTableType":"","tableType":"event","timeUnit":""}],"quota":"num","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":25}

# 网赚
def liucun_download(headers,params,body,colmuns_num):
    # 调用下载api，返回为text格式
    response = requests.post('http://13.214.112.1:8992/open/streaming-download/retention-analyze',  headers=headers,params=params,json=body)
    # 设置encoding 防止中文乱码
    response.encoding = "utf-8"

    #写入文件
    # 指定文件名
    filename = "example.txt"

    # 使用'with'语句和'open'函数打开文件。模式参数'w'表示写入模式，如果文件已存在，将被覆盖。
    with open(filename, 'w', encoding='utf-8') as file:
        # 将文本信息写入到文件中
        file.write(response.text)



    #替换双引号，并按照 , 换行符进行分割
    response = re.sub('",','"{',response.text)
    response = re.sub('"', '', response)
    response = re.split('[\r\t\n{]', response)
    # 根据字段控制二维列表长度,并生成df
    response_list=[response[i:i+colmuns_num] for i in range(0,len(response),colmuns_num)]
    df=pd.DataFrame(data=response_list[1:-1], columns=response_list[0])
    return df
# df=liucun_download(headers,params,body)
# print(df)
# df.to_excel('基础数据.xlsx')



# 休闲
headers = {
    'Content-Type': 'application/json'
}
# params = {
#     'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
# }
# body = {"eventView": {"collectFirstDay": 1, "endTime": f"{endtime} 23:59:59", "filts": [], "groupBy": [
#     {"columnDesc": "adjust渠道名称", "columnName": "#adjust_network_name", "propertyRange": "",
#      "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
#     {"columnDesc": "adjust推广活动名称", "columnName": "#adjust_campaign_name", "propertyRange": "",
#      "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
#     {"columnDesc": "adjust推广活动名称_fb", "columnName": "adjust_campaign_name", "propertyRange": "",
#      "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"}], "recentDay": "1-8",
#                       "relation": "and", "startTime": f"{starttime} 00:00:00", "statType": "retention",
#                       "taIdMeasureVo": {"columnDesc": "用户唯一ID", "columnName": "#user_id", "tableType": "event"},
#                       "timeParticleSize": "day", "unitNum": 7}, "events": [
#     {"eventName": "firs_device_add", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
#      "type": "first"},
#     {"eventName": "login", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
#      "type": "second"},
#     {"analysis": "STAGE_ACC", "analysisDesc": "阶段累计总和", "eventName": "ad_impEnd", "eventNameDisplay": "",
#      "filts": [], "quota": "af_revenue", "relation": "and", "relationUser": "and", "type": "simultaneous_display"}],
#         "projectId": 33}
#
# response = requests.post('http://13.214.112.1:8992/open/streaming-download/retention-analyze', headers=headers,
#                          params=params, json=body)
#
# response.encoding = "utf-8"
# print(response.text)
if __name__ == '__main__':
    liucun_download(headers,params,body,10)