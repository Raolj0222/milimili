#网赚分国家数据
import datetime
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import TE_app_token
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
import requests
import datetime
import json
import pandas as pd
accountlist = [
    'zhangjie20209@gmail.com',
]
tokendict = {
      'zhangjie20209@gmail.com':'eyJhbGciOiJIUzI1NiIsImtpZCI6ImZsdXJyeS56dXVsLnByb2Qua2V5c3RvcmUua2V5LjIifQ.eyJpc3MiOiJodHRwczovL3p1dWwuZmx1cnJ5LmNvbTo0NDMvdG9rZW4iLCJpYXQiOjE2MjgxNTc3OTksImV4cCI6MzMxODUwNjY1OTksInN1YiI6IjQ3ODc3MiIsImF1ZCI6IjQiLCJ0eXBlIjo0LCJqdGkiOiIxMjUzMyJ9._7GfcD-s1Qh_yiV0uCU-ghp2ySpZaHfgJJF4g5w-vmc',
}
url_endpoint="https://api-metrics.flurry.com/public/v1/data/"
today=datetime.datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).date().strftime('%Y-%m-%d')
#yesterday1 = (datetime.datetime.now() + datetime.timedelta(days=-1)).date().strftime('%Y-%m-%d')
#weekday=(datetime.datetime.now() + datetime.timedelta(days=-8)).date().strftime('%Y-%m-%d')
print(today,yesterday)

#读取数据库信息
def get_info(sql):
    url_sql="http://150.230.221.78:5000?sql="+sql
    response_sql = requests.request("get", url=url_sql)
    sql_datalist=json.loads(response_sql.text)
    return sql_datalist
def savedata(table,savedata_list):
    values=savedata_list
    values=str(savedata_list).replace("'","\"").replace(r"\n","")
    #print(values)
    url_update='http://150.230.221.78:5000/update'
    payload={'table': table,
    'values': values}
    headers = {}
    response = requests.request("post", url=url_update, headers=headers, data=payload)
    print(response.text)

def parse_user(row_list):
    tt = []
    appnamelist = []
    for u in row_list:
        averageTimePerDevice=u['averageTimePerDevice']
        averageTime_PerDevice=pd.to_datetime(averageTimePerDevice, unit = 's').strftime('%H:%M:%S')
        appname = u['app|name']
        ttdict={
            "date":u['dateTime'][0:10],
            "app_id":u['app|id'],
            "app_name":appname,
            "app_country":u['country|iso'],
            "newDevices":u['newDevices'],
            "activeDevices":u['activeDevices'],
            "averageTime_PerDevice":averageTime_PerDevice
        }
        appnamelist.append(appname)
        tt.append(ttdict)
    return appnamelist,tt

def parse_userall(row_list):
    tt = []
    for u in row_list:
        averageTimePerDevice=u['averageTimePerDevice']
        averageTime_PerDevice=pd.to_datetime(averageTimePerDevice, unit = 's').strftime('%H:%M:%S')
        appname = u['app|name']
        ttdict={
            "date":u['dateTime'][0:10],
            "app_id":u['app|id'],
            "app_name":appname,
            "app_country":'all',
            "newDevices":u['newDevices'],
            "activeDevices":u['activeDevices'],
            "averageTime_PerDevice":averageTime_PerDevice
        }
        tt.append(ttdict)
    return tt

def parse_retention(app_name,row_list):
    tt=[]
    app_name=app_name
    for t in row_list:
            date =t['dateTime'][0:10]
            cohort=t['rollingRate']['cohort']
            day_list=t['rollingRate']['measurement']
            d1=None
            d2=None
            d3=None
            d4=None
            d5=None
            d6=None
            d7=None
            for d in day_list:
                period_tuple=d['period'],
                str1 = ''
                period=str1.join(period_tuple)
                if period=="day1":
                    d1=d['ratio']
                elif period=="day2":
                    d2=d['ratio']
                elif period=="day3":
                    d3=d['ratio']
                elif period=="day4":
                    d4=d['ratio']
                elif period=="day5":
                    d5=d['ratio']
                elif period=="day6":
                    d6=d['ratio']
                elif period=="day7":
                    d7=d['ratio']
                else:
                    print('没有数据')
            dict={
            "app_name":app_name,
            "date":date,
            "cohort":cohort,
            "d1":d1,
            "d2":d2,
            "d3":d3,
            "d4":d4,
            "d5":d5,
            "d6":d6,
            "d7":d7
            }
            tt.append(dict)
    return tt

#获取各app新增活跃数据
for account in accountlist:
    token = tokendict[account]
    user_url="https://api-metrics.flurry.com/public/v1/data/appUsage/day/app;show=id,name/country;show=iso?metrics=newDevices,activeDevices,averageTimePerDevice&dateTime=%s/%s&format=json&token=%s&timeZone=UTC"%(yesterday,today,token)
    user_res=requests.request('get',url=user_url)
    rr=json.loads(user_res.text)
    #print(rr)
    row_list = rr['rows']
    datatuple = parse_user(row_list)
    appnamelist = datatuple[0]
    appnamelist = list(set(appnamelist))#去重列表元素
    #print(appnamelist)
    tt = datatuple[1]
    # tt=json.dumps(tt)
    tt=pd.DataFrame(tt)

    print(tt)
    table='wangzuan_flurry'
    mysql.insert_data(tt,table)
    user_url1="https://api-metrics.flurry.com/public/v1/data/appUsage/day/app;show=id,name,company|id?metrics=newDevices,activeDevices,averageTimePerDevice&dateTime=%s/%s&format=json&token=%s&timeZone=UTC"%(yesterday,today,token)
    user_res1=requests.request('get',url=user_url1)
    rr1=json.loads(user_res1.text)
    #print(rr1)
    row_list1 = rr1['rows']
    print(row_list1)
    data = parse_userall(row_list1)

    df=pd.DataFrame(data)
    print(df)
    print('--------')
    # data=json.dumps(data)
    # print(data)
    mysql.insert_data(df,table)
    print('新增活跃获取完成')
'''
    #获取留存
    for app_name in appnamelist:
        rentention_url="https://api-metrics.flurry.com/public/v1/data/retention/day?metrics=returnRate,rollingRate&dateTime=%s/%s&filters=app|name-in[%s]&token=%s"%(weekday,yesterday1,app_name,token)
        res=requests.request('get',url=rentention_url)
        rr=json.loads(res.text)
        #print(rr)
        row_list=rr['rows']
        tt=parse_retention(app_name,row_list)
        #print(tt)
        savedata_list=json.dumps(tt)
        table='wangzuan_retention'
        savedata(table,savedata_list)
        print(app_name,'留存获取完成')
print('已全部更新')
'''