import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from tools.数数api import liucun_download
from works.adjust_系列支出 import  get_cost
from BI_robot.feishu import FeiShu
import numpy as np
feishu=FeiShu()
import re
import pandas as pd
import datetime
import warnings
warnings.filterwarnings("ignore")
nowtime=datetime.date.today().strftime('%Y-%m-%d')
endtime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
starttime =(datetime.date.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')



## 广告回收情况
def guanggao():

    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0',
    }
    body={"eventView":{"collectFirstDay":1,"endTime":f"{endtime} 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust_campaign_name","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":f"{starttime} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"first_device_add","eventNameDisplay":"","simultaneous_display":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"ad_impEndRevenue","eventNameDisplay":"","filts":[],"quota":"#vp@af_revenue","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":25}


    df=liucun_download(headers,params,body,15)

    df['系列']=df.apply(lambda x: x['adjust推广活动名称'] if x['adjust_campaign_name']=='(null)' else x['adjust_campaign_name'],axis=1)
    df.columns=['时间', '应用包名', 'adjust渠道名称', 'adjust推广活动名称',
           'adjust_campaign_name', '新增设备用户数', '指标', '当日广告回收', '1日广告回收', '2日广告回收', '3日广告回收', '4日广告回收',
           '5日广告回收', '6日广告回收', '7日广告回收', '系列']

    # 获取收入 广告收益.此次广告收益阶段累计总和
    df_income=df.query('指标=="广告收益.此次广告收益阶段累计总和"&时间!="阶段值"')
    res=df_income[['时间', '系列', '当日广告回收', '1日广告回收', '2日广告回收', '3日广告回收', '4日广告回收',
           '5日广告回收', '6日广告回收', '7日广告回收']]
    res['时间']=res['时间'].str.slice(0,10)
    for i in ['当日广告回收', '1日广告回收', '2日广告回收', '3日广告回收', '4日广告回收',
           '5日广告回收', '6日广告回收', '7日广告回收']:
        res[i]=res[i].str.replace('-','0').astype('float')
    def func(x):
        s=re.sub('\(.*\)','',x['系列'])
        return s
    res['系列']=res.apply(lambda x:func(x),axis=1)
    res['系列']=res['系列'].str.strip()
    res=res.groupby(['时间', '系列'])[['当日广告回收', '1日广告回收',
                                   '2日广告回收', '3日广告回收', '4日广告回收',
                                    '5日广告回收', '6日广告回收', '7日广告回收']].agg({
        '当日广告回收':'max', '1日广告回收':'max',
        '2日广告回收':'max', '3日广告回收':'max', '4日广告回收':'max',
        '5日广告回收':'max', '6日广告回收':'max', '7日广告回收':'max'}).reset_index()

    return res


## 积分墙回收情况
def adjoe():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0',
    }
    body = {"eventView": {"collectFirstDay": 1, "endTime": f"{endtime} 23:59:59", "filts": [], "groupBy": [
        {"columnDesc": "应用包名", "columnName": "#bundle_id", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "event"},
        {"columnDesc": "adjust渠道名称", "columnName": "#adjust_network_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
        {"columnDesc": "adjust推广活动名称", "columnName": "#adjust_campaign_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
        {"columnDesc": "adjust_campaign_name", "columnName": "adjust_campaign_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"}], "recentDay": "1-8",
                          "relation": "and", "startTime": f"{starttime} 00:00:00", "statType": "retention",
                          "taIdMeasureVo": {"columnDesc": "用户唯一ID", "columnName": "#user_id", "tableType": "event"},
                          "timeParticleSize": "day", "unitNum": 7}, "events": [
        {"eventName": "first_device_add", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
         "type": "first"},
        {"eventName": "login", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
         "type": "second"}, {"analysis": "STAGE_ACC", "analysisDesc": "阶段累计总和", "eventName": "offerwall_success",
                             "eventNameDisplay": "", "filts": [], "quota": "offerwall_money", "relation": "and",
                             "relationUser": "and", "type": "simultaneous_display"}], "projectId": 25}
    df = liucun_download(headers, params, body,15)
    # print(df.head())

    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust_campaign_name'] == '(null)' else x['adjust_campaign_name'],
        axis=1)
    df.columns = ['时间', '应用包名', 'adjust渠道名称', 'adjust推广活动名称',
                  'adjust_campaign_name', '新增设备用户数', '指标', '当日积分墙回收', '1日积分墙回收', '2日积分墙回收', '3日积分墙回收', '4日积分墙回收',
                  '5日积分墙回收', '6日积分墙回收', '7日积分墙回收', '系列']

    # 获取收入 广告收益.此次广告收益阶段累计总和
    df_income = df.query('指标=="积分墙完成任务.带来美元数阶段累计总和"&时间!="阶段值"')
    res = df_income[
        ['时间', '系列', '当日积分墙回收', '1日积分墙回收', '2日积分墙回收', '3日积分墙回收', '4日积分墙回收',
                  '5日积分墙回收', '6日积分墙回收', '7日积分墙回收']]
    res['时间'] = res['时间'].str.slice(0, 10)
    for i in ['当日积分墙回收', '1日积分墙回收', '2日积分墙回收', '3日积分墙回收', '4日积分墙回收',
                  '5日积分墙回收', '6日积分墙回收', '7日积分墙回收']:
        res[i]=res[i].str.replace('-','0').astype('float')
    def func(x):
        s = re.sub('\(.*\)', '', x['系列'])
        return s

    res['系列'] = res.apply(lambda x: func(x), axis=1)
    res['系列'] = res['系列'].str.strip()
    res=res.groupby(['时间', '系列'])[['当日积分墙回收', '1日积分墙回收',
                                   '2日积分墙回收', '3日积分墙回收', '4日积分墙回收',
                                    '5日积分墙回收', '6日积分墙回收', '7日积分墙回收']].agg({

        '当日积分墙回收':'max', '1日积分墙回收':'max',
        '2日积分墙回收':'max', '3日积分墙回收':'max', '4日积分墙回收':'max',
        '5日积分墙回收':'max', '6日积分墙回收':'max', '7日积分墙回收':'max'
    }).reset_index()
    return res


# # print(adjoe.head(10)[['时间','应用包名','系列','当日']])
# ## 提现支出情况
def cost_payout():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0',
    }
    body = {"eventView": {"collectFirstDay": 1, "endTime": f"{endtime} 23:59:59", "filts": [], "groupBy": [
        {"columnDesc": "应用包名", "columnName": "#bundle_id", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "event"},
        {"columnDesc": "adjust渠道名称", "columnName": "#adjust_network_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
        {"columnDesc": "adjust推广活动名称", "columnName": "#adjust_campaign_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"},
        {"columnDesc": "adjust_campaign_name", "columnName": "adjust_campaign_name", "propertyRange": "",
         "specifiedClusterDate": f"{nowtime}", "subTableType": "", "tableType": "user"}], "recentDay": "1-8",
                          "relation": "and", "startTime": f"{starttime} 00:00:00", "statType": "retention",
                          "taIdMeasureVo": {"columnDesc": "用户唯一ID", "columnName": "#user_id", "tableType": "event"},
                          "timeParticleSize": "day", "unitNum": 7}, "events": [
        {"eventName": "first_device_add", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
         "type": "first"},
        {"eventName": "login", "eventNameDisplay": "", "filts": [], "relation": "and", "relationUser": "and",
         "type": "second"},
        {"analysis": "STAGE_ACC", "analysisDesc": "阶段累计总和", "eventName": "redeem_success", "eventNameDisplay": "",
         "filts": [{"columnDesc": "status", "columnName": "status", "comparator": "equal", "filterType": "SIMPLE",
                    "ftv": ["1"], "specifiedClusterDate": "2023-10-23", "subTableType": "", "tableType": "event",
                    "timeUnit": ""}], "quota": "num", "relation": "and", "relationUser": "and",
         "type": "simultaneous_display"}], "projectId": 25}
    df = liucun_download(headers, params, body,15)
    # print(df)
    # print(df.head())

    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust_campaign_name'] == '(null)' else x['adjust_campaign_name'],
        axis=1)
    df.columns = ['时间', '应用包名', 'adjust渠道名称', 'adjust推广活动名称',
                  'adjust_campaign_name', '新增设备用户数', '指标', '当日提现支出', '1日提现支出', '2日提现支出',
                  '3日提现支出', '4日提现支出',
                  '5日提现支出', '6日提现支出', '7日提现支出', '系列']



    # 获取收入 广告收益.此次广告收益阶段累计总和
    df_income = df.query('指标=="提现状态事件.次数\金额\数字阶段累计总和"&时间!="阶段值"')
    res = df_income[
        ['时间', '系列', '当日提现支出', '1日提现支出', '2日提现支出',
                  '3日提现支出', '4日提现支出',
                  '5日提现支出', '6日提现支出', '7日提现支出']]
    res['时间'] = res['时间'].str.slice(0, 10)
    for i in ['当日提现支出', '1日提现支出', '2日提现支出',
                  '3日提现支出', '4日提现支出',
                  '5日提现支出', '6日提现支出', '7日提现支出']:
        res[i]=res[i].str.replace('-','0').astype('float')
    def func(x):
        s = re.sub('\(.*\)', '', x['系列'])
        return s

    res['系列'] = res.apply(lambda x: func(x), axis=1)
    res['系列'] = res['系列'].str.strip()
    # 留存数据
    df_active = df.query('指标=="留存率"&时间!="阶段值"')
    df_active['时间'] = df_active['时间'].str.slice(0, 10)
    df_active['系列'] = df_active.apply(lambda x: func(x), axis=1)
    df_active['系列'] = df_active['系列'].str.strip()
    df_active['次留']=df_active['1日提现支出']
    res=res.merge(df_active[['时间','系列','次留']],how='left',on=['时间','系列'])
    res = res.groupby(['时间', '系列'])[['当日提现支出', '1日提现支出', '2日提现支出',
                  '3日提现支出', '4日提现支出',
                  '5日提现支出', '6日提现支出', '7日提现支出','次留']].agg({

        '当日提现支出': 'max', '1日提现支出': 'max',
        '2日提现支出': 'max', '3日提现支出': 'max', '4日提现支出': 'max',
        '5日提现支出': 'max', '6日提现支出': 'max', '7日提现支出': 'max',
        '次留':'max'
    }).reset_index()
    return res


## 魔王 广告回收和 内购收入
#魔王密钥： hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ
def mowang_guanggao():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    }
    body={"eventView":{"collectFirstDay":1,"endTime":f"{endtime} 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称_fb","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":f"{starttime} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"firs_device_add","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"ad_impEnd","eventNameDisplay":"","filts":[],"quota":"af_revenue","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":33}
    df = liucun_download(headers, params, body,15)
    # print(df)
    # print(df.head())
    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust推广活动名称_fb'] == '(null)' else x['adjust推广活动名称_fb'],
        axis=1)
    df.columns = ['时间', '应用包名','adjust渠道名称', 'adjust推广活动名称',
                  'adjust推广活动名称_fb', '新增设备用户数', '指标', '当日广告收入', '1日广告收入', '2日广告收入',
                  '3日广告收入', '4日广告收入',
                  '5日广告收入', '6日广告收入', '7日广告收入', '系列']

    # 获取收入 广告收益.此次广告收益阶段累计总和
    df_income = df.query('指标=="广告展示完毕（sdk上报）.此次广告收益阶段累计总和"&时间!="阶段值"')
    res = df_income[
        ['时间', '系列', '当日广告收入', '1日广告收入', '2日广告收入',
                  '3日广告收入', '4日广告收入',
                  '5日广告收入', '6日广告收入', '7日广告收入']]
    res['时间'] = res['时间'].str.slice(0, 10)
    for i in ['当日广告收入','1日广告收入', '2日广告收入',
                  '3日广告收入', '4日广告收入',
                  '5日广告收入', '6日广告收入', '7日广告收入']:
        res[i]=res[i].str.replace('-','0').astype('float')
    def func(x):
        s = re.sub('\(.*\)', '', x['系列'])
        return s

    res['系列'] = res.apply(lambda x: func(x), axis=1)
    res['系列'] = res['系列'].str.strip()
    # 留存数据
    df_active = df.query('指标=="留存率"&时间!="阶段值"')
    df_active['时间'] = df_active['时间'].str.slice(0, 10)
    df_active['系列'] = df_active.apply(lambda x: func(x), axis=1)
    df_active['系列'] = df_active['系列'].str.strip()
    df_active['次留'] = df_active['1日广告收入']
    res = res.merge(df_active[['时间', '系列', '次留']], how='left', on=['时间', '系列'])
    res = res.groupby(['时间', '系列'])[['当日广告收入', '1日广告收入', '2日广告收入',
                  '3日广告收入', '4日广告收入',
                  '5日广告收入', '6日广告收入', '7日广告收入', '次留']].agg({

        '当日广告收入': 'max', '1日广告收入': 'max',
        '2日广告收入': 'max', '3日广告收入': 'max', '4日广告收入': 'max',
        '5日广告收入': 'max', '6日广告收入': 'max', '7日广告收入': 'max',
        '次留': 'max'
    }).reset_index()
    return res
def mowang_neigou():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    }
    body={"eventView":{"collectFirstDay":1,"endTime":f"{endtime} 23:59:59","filts":[],"groupBy":[{"columnDesc":"应用包名","columnName":"#bundle_id","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称_fb","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"}],"recentDay":"1-8","relation":"and","startTime":f"{starttime} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":7},"events":[{"eventName":"firs_device_add","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"In_appPurchases_BuySuccess","eventNameDisplay":"","filts":[],"quota":"#vp@currency_type__pay_amount__USD","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":33}
    df = liucun_download(headers, params, body,15)
    # print(df)
    # print(df.head())
    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust推广活动名称_fb'] == '(null)' else x['adjust推广活动名称_fb'],
        axis=1)
    df.columns = ['时间', '应用包名', 'adjust渠道名称', 'adjust推广活动名称',
                  'adjust推广活动名称_fb', '新增设备用户数', '指标', '当日内购收入', '1日内购收入', '2日内购收入',
                  '3日内购收入', '4日内购收入',
                  '5日内购收入', '6日内购收入', '7日内购收入', '系列']

    # 获取收入 广告收益.此次广告收益阶段累计总和
    df_income = df.query('指标=="内购商品购买成功.支付金额 - USD阶段累计总和"&时间!="阶段值"')
    res = df_income[
        ['时间', '系列', '当日内购收入', '1日内购收入', '2日内购收入',
                  '3日内购收入', '4日内购收入',
                  '5日内购收入', '6日内购收入', '7日内购收入']]
    res['时间'] = res['时间'].str.slice(0, 10)
    for i in ['当日内购收入', '1日内购收入', '2日内购收入',
                  '3日内购收入', '4日内购收入',
                  '5日内购收入', '6日内购收入', '7日内购收入']:
        res[i] = res[i].str.replace('-', '0').astype('float')

    def func(x):
        s = re.sub('\(.*\)', '', x['系列'])
        return s
    res['系列'] = res.apply(lambda x: func(x), axis=1)
    res['系列'] = res['系列'].str.strip()
    res = res.groupby(['时间', '系列'])[['当日内购收入', '1日内购收入', '2日内购收入',
                                         '3日内购收入', '4日内购收入',
                                         '5日内购收入', '6日内购收入', '7日内购收入']].agg({

        '当日内购收入': 'max', '1日内购收入': 'max',
        '2日内购收入': 'max', '3日内购收入': 'max', '4日内购收入': 'max',
        '5日内购收入': 'max', '6日内购收入': 'max', '7日内购收入': 'max'
    }).reset_index()
    return res



# 魔王数据 包含ios数据
mowang_gg,mowang_ng=mowang_guanggao(),mowang_neigou()
res_mowang=mowang_gg.merge(mowang_ng,how='left',on=['时间','系列'])
res_mowang['留存']=res_mowang['次留']
res_mowang.drop(columns=['次留'],inplace=True)
res_mowang['系列']=res_mowang['系列'].str.upper()
# #
# #
# #
# # 网赚数据
admon,adjoe,cost=guanggao(),adjoe(),cost_payout()
res=admon.merge(adjoe,how='left',on=['时间','系列']).merge(cost,how='left',on=['时间','系列'])
res['系列']=res['系列'].str.upper()
#
# # 系列支出
net_cost=get_cost(9)

res_wz=pd.merge(net_cost,res,how='left',left_on=['date','campaign_network'],right_on=['时间','系列'])
res_wz=res_wz.query('app=="Daub Farm"')
res_wz.drop(columns=['时间','系列','平台'],inplace=True)
#本地存档
# res_wz=pd.read_excel('D:\Desktop\网赚每日回收情况.xlsx')
res_wz.fillna(value=0,inplace=True)
res_wz.to_excel('D:\Desktop\网赚每日回收情况.xlsx',index=False)




# 休闲数据
res_cp=pd.merge(net_cost,res_mowang,how='left',left_on=['date','campaign_network'],right_on=['时间','系列'])
res_cp.drop(columns=['时间','系列'],inplace=True)
res_cp['network']=res_cp['network'].apply(lambda x: 'MB' if x in ['FB','Google Ads ACI'] else x)
res_cp.to_excel('D:\Desktop\休闲每日回收情况.xlsx',index=False)




# # 读取飞书表格
# tab_path = 'NPPTsN1HmhVigrtKj6Uc95Eyn8M'
# sheet_path = 'wwoL0p'
# range_list = 'A:A'
# df = feishu.read_tab(tab_path, sheet_path, range_list)
# if df.shape[0] <= 1:
#     new_range = 2
# else:
#     new_range = list(df.query(f'日期=="{starttime}"').index)[0] + 1
# # 写入飞书表格
#
# values = np.array(res_wz).tolist()
# # for i  in range(0,len(values),10):
# #     if i < len(values)-10:
# #         val=values[i:i+10]
# #         num = 10
# #     else:
# #         val =values[i:]
# #         num = len(values) - i
# data_len = new_range + len(values)  -1
# print(f'A{new_range}:AF{data_len}')
# feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:AF{data_len}', values)
#     # new_range = new_range +  10
# print('--- 网赚系列回收更新完成')



# res_cp= pd.read_excel('D:\Desktop\休闲每日回收情况.xlsx')
res_cp.fillna(value=0,inplace=True)
res_cp1=res_cp.query('app=="Doomsday Vanguard"')

# 读取飞书表格
tab_path = 'XVYDsydUFhpNOPtRJurcPgCCnyf'
sheet_path = 'aM4FQj'
range_list = 'A:A'
df = feishu.read_tab(tab_path, sheet_path, range_list)
if df.shape[0] <= 1:
    new_range = 2
else:
    new_range = list(df.query(f'时间=="{starttime}"').index)[0] + 1
# 写入飞书表格
values = np.array(res_cp1).tolist()
data_len = new_range  + len(values)  -1
print(f'A{new_range}:AF{data_len}')
feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:Y{data_len}', values)
    # new_range = new_range +  10
print('-- 休闲系列回收更新完成')
