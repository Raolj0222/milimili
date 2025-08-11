import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')

from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql

from 滋滋渠道回收情况 import get_pingtai,get_nerwork,get_country
mysql=Mysql_handle(mili_mysql,'data_analysis')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
import pandas as pd
import numpy as np

import requests
import datetime
import warnings
warnings.filterwarnings("ignore")
nowtime=datetime.date.today().strftime('%Y-%m-%d')
endtime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
starttime =(datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
# # 特殊处理更新数据时间
def get_total_revenue():
    # 获取用户累计30日总收入数据
    url = 'https://dash.engageminds.ai/api/oka/analysis/v1/ltv/report'
    herders = {
        'Content-Type': 'application/json;charset=UTF-8'
    }
    body = {"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD",
            "pubAppId":25624,
            "groups":"[{\"id\":\"2d930815-217a-44a4-a02a-42d3b0a7e649\",\"display\":\"操作系统\",\"displayEn\":\"Operating System\",\"value\":\"$os\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":1},"
                     "{\"id\":\"002a96f6-eb28-4cb5-970a-5a40a5acc05c\",\"display\":\"国家/地区\",\"displayEn\":\"Country/Region\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},"
                     "{\"id\":\"336b174d-cc26-485a-a205-726529ae4a95\",\"display\":\"首次广告来源\",\"displayEn\":\"First Ad Source/Network\",\"value\":\"#utm_source\",\"propType\":1,\"type\":1,\"category\":1,\"dataType\":2},"
                     "{\"id\":\"54bf7284-79ed-4fdb-9c0b-ad9010f92234\",\"display\":\"首次广告活动ID\",\"displayEn\":\"First Ad Campaign\",\"value\":\"#utm_campaign\",\"propType\":1,\"type\":1,\"category\":1,\"dataType\":2}]",
            "tz":0,"metric":1,"lang":"cn","retentionDayn":30,
            "dateValue":"[\"Last 30 Days\",\"\"]",
            "events":"[{\"id\":\"45d46887-de04-4dd6-906e-b368a9c92f25\",\"rType\":0,\"f\":[{\"id\":\"1ac8688c-40e4-4f33-abc9-c33209d6eb90\",\"f\":[{\"id\":\"ab4dc5f0-f0ba-4f8f-813f-420056d3c74e\",\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"displayEn\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false,\"noPropsPop\":true},"
                     "{\"id\":\"d50cd081-bd38-451e-9435-059c2f3bc14c\",\"s\":{},\"columnName\":\"$cdid\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":1,\"calcuSymbol\":\"Is set\",\"symbolId\":\"B05\",\"ftv\":[],\"showCondition\":false},"
                     "{\"id\":\"e48ba45b-8f3a-402d-8223-4362061c35ee\",\"columnName\":\"$redirect\",\"display\":\"用户重定向id\",\"displayEn\":\"用户重定向id\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":1,\"calcuSymbol\":\"Is not set\",\"symbolId\":\"B06\",\"ftv\":[],\"showCondition\":false}],\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"relation\":\"and\"}],"
                     "\"eid\":\"#app_install\",\"display\":\"App 安装后首次启动\",\"displayEn\":\"app intall\",\"type\":0},"
                     "{\"id\":\"cd126032-e971-4459-a34b-f2d2b378d09f\",\"rType\":1,\"f\":[],\"mid\":352,\"display\":\"总收入\",\"displayEn\":\"总收入\",\"type\":1}]",
            "filters":"[]"}
    data = requests.post(url,headers=herders,json=body)

    # 解析数据
    df_list = []
    if data.json()['code'] ==0:
        res= data.json()['data']['data']
    else:
        return

    # 获取详细日期数据，在返回数据中的，用名为‘list’的键存放
    for i in res:
        data_detail = i['list']
        # print(data_detail)
        for value  in data_detail:
            df_list.append(value)

    df = pd.DataFrame(df_list)
    # 根据日期排序
    df = df.sort_values(by='date')

    # 国家、渠道、系列
    df['国家'] = df['country']
    df['系列'] = df['#utm_campaign']
    df['adjust渠道名称'] = df['#utm_source']

    # 处理 缺失日期数据 7、14、21、30日数据
    # 首先处理正确的列顺序

    col_list = ['date','国家','new_user','os','adjust渠道名称','系列']
    day_list = []
    for i in range(31):
        if f'Day{i}' in df.columns.tolist():
            day_list.append(f'Day{i}')
        else:
            pass
    col_list=col_list+day_list
    df = df[col_list]
    # 填充nan，使用左侧数据进行填充
    def fill_nan_with_left(row):
        # 遍历行中的每个元素，如果该元素是NaN，则用左侧的非NaN值填充
        for i in range(6, len(row)):
            if pd.isna(row[i]) and i==6:
                row[i] = 0
            elif pd.isna(row[i]):
                row[i] = row[i - 1]
        return row

    df = df.apply(fill_nan_with_left, axis=1)

    # 使用前n天最大值来填充缺少日期
    def max_of_row(row):
        # 将字符串转换为数值，无法转换的值变为NaN
        row_numeric = pd.to_numeric(row, errors='coerce')
        return row_numeric.max()
    def day_n(n):
        day_list = []
        for i in range(n+1):
            if f'Day{i}' in df.columns.tolist():
                day_list.append(f'Day{i}')
            else:
                pass
        return day_list
    for i in [7,14,21,30]:
        df[f'day{i}'] = df[day_n(i)].apply(lambda row: max_of_row(row), axis=1)



    # 根据系列、渠道数据，判断代理、平台、国家等信息

    df['渠道'] = df.apply(lambda x: get_nerwork(x), axis=1)
    df['渠道'] = df['渠道'].str.replace('Unattributed', 'FB')

    df['平台']=df.apply(lambda x:get_pingtai(x),axis=1)
    df['国家'] = df.apply(lambda x: get_country(x), axis=1)

    # 修改渠道数据 fb和gg为mb
    df['渠道']=df['渠道'].apply(lambda x: 'MB' if x in ['FB','Google Ads ACI','TTMB','Unity ads_MB','Mintegral','Twitter Installs','Line-MB','Applovin'] else x)
    df['支出'] = 0


    # 返回关键数据
    col_list =['date','国家','平台','渠道','支出','new_user','os','Day0','Day3','day7','day14','day21','day30']
    res = df[col_list].groupby(by=['date','国家','平台','渠道','os'])[
        ['支出','new_user','Day0','Day3','day7','day14','day21','day30']
    ].agg({'支出':'sum',
           'new_user':'sum',
           'Day0':'sum',
           'Day3':'sum',
           'day7':'sum',
           'day14':'sum',
           'day21':'sum',
           'day30':'sum'}).reset_index()


    return res
if __name__ == '__main__':
    res= get_total_revenue()
    res=res.query('平台!="Organic"')
    res = res.query('平台!=""')
    res.to_excel('D:\Desktop\em渠道回收.xlsx',index=False)


    # 飞书写入
    # 处理异常值
    res.dropna(inplace=True)
    res.replace(np.inf, 0, inplace=True)

    # 安卓写入飞书表格
    tab_path = 'XVYDsydUFhpNOPtRJurcPgCCnyf'
    sheet_path = 'PnTiIk'


    range_list = 'A:A'
    df = feishu.read_tab(tab_path, sheet_path, range_list)
    if df.shape[0] <= 1:
        new_range = 2
    else:
        new_range = list(df.query(f'时间=="{starttime}"').index)[0] + 1
    # 写入飞书表格
    res_an = res.query('os=="Android"')
    res_an.drop(columns=['os'], inplace=True)
    values = np.array(res_an).tolist()

    data_len = new_range + len(values) - 1
    feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:L{data_len}', values)
    print('-- 安卓回收更新完成')
    #
    # # ios写入飞书表格
    # tab_path = 'XVYDsydUFhpNOPtRJurcPgCCnyf'
    # sheet_path = 'vYt6LB'
    #
    # range_list = 'A:A'
    # df = feishu.read_tab(tab_path, sheet_path, range_list)
    # if df.shape[0] <= 1:
    #     new_range = 2
    # else:
    #     new_range = list(df.query(f'时间=="{starttime}"').index)[0] + 1
    # # 写入飞书表格
    # res_ios = res.query('os=="iOS"')
    # res_ios.drop(columns=['os'], inplace=True)
    # values = np.array(res_an).tolist()
    #
    # data_len = new_range + len(values) - 1
    # feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:L{data_len}', values)
    # print('-- ios回收更新完成')