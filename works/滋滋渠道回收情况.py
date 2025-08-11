import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from tools.数数api import liucun_download
from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql
mysql=Mysql_handle(mili_mysql,'data_analysis')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
import re
import numpy as np
import pandas as pd
import datetime
import warnings
warnings.filterwarnings("ignore")
nowtime=datetime.date.today().strftime('%Y-%m-%d')
endtime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
starttime =(datetime.date.today() - datetime.timedelta(days=46)).strftime('%Y-%m-%d')
# starttime='2023-10-25'
# endtime='2023-11-17'


##  通过系列前缀判断外部渠道
def get_nerwork(x):
    if 'weads' in str(x['系列']):
        network='weads'
    elif 'youdao' in str(x['系列']):
        network ='youdaozhixuan'
    elif 'apolloads' in str(x['系列']):
        network = 'apolloads'
    elif 'runbridge'  in str(x['系列']):
        network = 'runbridge'
    elif 'palmax'  in str(x['系列']).lower():
        network = 'Palmax'
    elif 'wezonet'  in str(x['系列']).lower():
        network = 'wezonet'
    elif 'GATHERONE' in str(x['系列']).upper():
        network = 'GatherOne'
    elif 'HBY' in str(x['系列']).upper():
        network = 'HBY'
    elif 'SR' in str(x['系列']).upper():
        network = '3YData'
    elif 'madhouse' in str(x['系列']).lower():
        network = 'madhouse'
    elif 'SINOINTERACTIVE' in str(x['系列']).upper():
        network='SINOINTERACTIVE'
    elif 'himobi' in str(x['系列']).lower():
        network = 'Himobi'
    elif 'admax' in str(x['系列']).lower():
        network = 'Admax'
    elif 'DH' in str(x['adjust渠道名称']) or '3D AD' in str(x['adjust渠道名称']) or str(x['adjust渠道名称']) in ['Persona.ly','Presco','Fukuroulabo','ZucksAF','Nend','Unicorn','i-mobile','SeedApp']:
        network = 'DH'
    else :
        network = x['adjust渠道名称']
    return network

# 获取平台信息
def get_pingtai(x):
    if 'FB' in str(x['系列']).upper() or 'FACEBOOK' in str(x['系列']).upper() or x['渠道'] in ['instagram','facebook','audience_network','messenger']:
        pingtai='FB'
    elif 'GG' in str(x['系列']).upper() or 'Google Ads ACI'==str(x['渠道']) or 'Google' in str(x['系列']) :
        pingtai ='GG'
    elif 'TT' in str(x['系列']).upper():
        pingtai = 'TT'
    elif 'HBY' in str(x['系列']).upper() or 'SR' in str(x['系列']).upper():
        pingtai = 'Apple Search Ads'
    else :
        pingtai = x['渠道']
    if 'TWITTER' in str(x['系列']).upper():
        pingtai = 'Twitter'
    if 'TWITTER' in str(x['渠道']).upper():
        pingtai = 'Twitter'
    if 'AMOAD' in str(x['adjust渠道名称']).upper():
        pingtai = 'Amoad'
    if 'LINE' in str(x['adjust渠道名称']).upper():
        pingtai = 'line'
    if 'SEEDAPP' in str(x['adjust渠道名称']).upper():
        pingtai = 'Seedapp'
    if '3D AD' in str(x['adjust渠道名称']).upper():
        pingtai = '3D AD'
    if 'presco' in str(x['adjust渠道名称']).lower():
        pingtai = 'Presco'
    if 'i-mobile' in str(x['adjust渠道名称']).lower():
        pingtai = 'i-mobile'
    if 'CREAM' in str(x['adjust渠道名称']).upper():
        pingtai = 'Cream'
    return pingtai

# 获取国家信息

def get_country(x):
    country = x['国家']
    if 'GLOBAL' in str(x['系列']).upper() and  'CHINESE' in str(x['系列']).upper():
        country = 'global-cn'
    if 'GLOBAL' in str(x['系列']).upper() and  'CHINESE' not  in str(x['系列']).upper():
        country = 'global-us'
    if 'zizi-jp-0124-1.0' in str(x['系列']).lower() or 'zizi-jp-ac1.0' in str(x['系列']).lower():
        country = 'JP1.0'
    if 'RBO_MS' in str(x['系列']).upper():
        country = 'global-test'
    if '-TV' in str(x['系列']).upper():
        country = 'JP-TV'
    if '-IMG' in str(x['系列']).upper():
        country = 'JP-img'
    if '中东' in str(x['系列']).upper() or 'middleeast' in str(x['系列']).lower():
        country = 'ZD'
    return country





## 魔王 广告回收和 内购收入
#魔王密钥： hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ
def mowang_guanggao():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    }
    body={"eventView":{"collectFirstDay":1,"endTime":f"{endtime} 23:59:59","filts":[{"columnDesc":"操作系统","columnName":"#os","comparator":"equal","filterType":"SIMPLE","ftv":["Android","iOS"],"specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event","timeUnit":""},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","comparator":"notEqual","filterType":"SIMPLE","ftv":["Organic","Google Organic Search"],"specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user","timeUnit":""}],"firstDayOfWeek":1,"groupBy":[{"columnDesc":"操作系统","columnName":"#os","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"国家地区代码","columnName":"#country_code","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称_fb","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"}],"recentDay":"1-46","relation":"and","startTime":f"{starttime} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":45},"events":[{"eventName":"ta_app_install","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"ad_impEnd","eventNameDisplay":"","filts":[],"quota":"af_revenue","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":33}

    df = liucun_download(headers, params, body, 54)
    print(df)
    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust推广活动名称_fb'] == '(null)' else x['adjust推广活动名称_fb'],
        axis=1)
    # df=df.query('系列!="(null)"')
    gg_list=[f'{i}日广告收入' for i in range(1,46)]
    gg_list.append('系列')
    collist=['时间','OS', '国家', 'adjust渠道名称', 'adjust推广活动名称',
                  'adjust推广活动名称_fb', '新增设备用户数', '指标', '当日广告收入',
                  ]
    collist.extend(gg_list)
    df.columns = collist
    # 获取次日留存人数和当日新增人数
    df_nowng = df.query('指标=="留存人数"&时间!="阶段值"')

    df_nowng = df_nowng[['时间','OS', '国家', 'adjust渠道名称', '系列', '新增设备用户数','1日广告收入']]
    df_nowng.columns = ['时间','OS','国家', 'adjust渠道名称', '系列', '新增设备用户数','1日留存人数']

    # 获取收入 广告展示完毕（sdk上报）.此次广告收益阶段累计总和
    df_income = df.query('指标=="广告展示完毕（sdk上报）.此次广告收益阶段累计总和"&时间!="阶段值"')
    datalist = ['时间', 'OS','国家', 'adjust渠道名称', '当日广告收入']
    datalist.extend(gg_list)
    res = df_income[datalist].merge(df_nowng, on=['时间', 'OS','国家', 'adjust渠道名称', '系列'])
    res['时间'] = res['时间'].str.slice(0, 10)
    col=['新增设备用户数','1日留存人数', '当日广告收入']
    col.extend(gg_list)
    for i in col:
        if i !='系列':
            res[i] = res[i].str.replace('-', '0').astype('float')
        else:
            pass

    def func(x):
        s = re.sub('\(.*\)', '', x)
        return s



    res['系列'] = res['系列'].apply(lambda x: func(x))
    res['系列'] = res['系列'].str.strip()

    res['渠道'] = res.apply(lambda x: get_nerwork(x), axis=1)
    res['渠道'] = res['渠道'].str.replace('Unattributed', 'FB')

    res['平台']=res.apply(lambda x:get_pingtai(x),axis=1)
    res['国家'] = res.apply(lambda x: get_country(x), axis=1)
    res.to_excel('广告数据.xlsx')
    list1=['新增设备用户数','1日留存人数', '当日广告收入']
    list1.extend(gg_list[0:-1])
    res = res.groupby(['时间', 'OS','国家','平台', '渠道'])[
        list1].agg({i:'sum' for i in list1}).reset_index()
    res['留存率']=res['1日留存人数']/res['新增设备用户数']
    res = res[res['国家'].isin(['TW', 'HK', 'MO', 'JP','KR','FR','DE','TH','VN','global-cn','global-us','JP1.0','global-test','JP-TV','JP-img'])]
    return res
def mowang_neigou():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    }
    body={"eventView":{"collectFirstDay":1,"endTime":f"{endtime} 23:59:59","filts":[{"columnDesc":"操作系统","columnName":"#os","comparator":"equal","filterType":"SIMPLE","ftv":["Android","iOS"],"specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event","timeUnit":""},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","comparator":"notEqual","filterType":"SIMPLE","ftv":["Organic","Google Organic Search"],"specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user","timeUnit":""}],"firstDayOfWeek":1,"groupBy":[{"columnDesc":"操作系统","columnName":"#os","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"国家地区代码","columnName":"#country_code","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"},{"columnDesc":"adjust渠道名称","columnName":"#adjust_network_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称","columnName":"#adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"},{"columnDesc":"adjust推广活动名称_fb","columnName":"adjust_campaign_name","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"user"}],"recentDay":"1-46","relation":"and","startTime":f"{starttime} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":45},"events":[{"eventName":"ta_app_install","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"In_appPurchases_BuySuccess","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"STAGE_ACC","analysisDesc":"阶段累计总和","eventName":"In_appPurchases_BuySuccess","eventNameDisplay":"","filts":[],"quota":"#vp@currency_type__pay_amount__USD","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":33}

    df = liucun_download(headers, params, body,54)
    df['系列'] = df.apply(
        lambda x: x['adjust推广活动名称'] if x['adjust推广活动名称_fb'] == '(null)' else x['adjust推广活动名称_fb'],
        axis=1)
    ng_list=[f'{i}日内购收入' for i in range(1,46)]
    ng_list.append('系列')
    collist=['时间','OS', '国家', 'adjust渠道名称', 'adjust推广活动名称',
                  'adjust推广活动名称_fb', '新增设备用户数', '指标', '当日内购收入',
                  ]
    collist.extend(ng_list)
    df.columns = collist
    # 获取当日内购用户数
    df_nowng=df.query('指标=="留存人数"&时间!="阶段值"')
    df_nowng=df_nowng[['时间','OS','国家','adjust渠道名称','系列','当日内购收入']]
    df_nowng.columns=['时间','OS','国家','adjust渠道名称','系列','当日内购用户数']
    # 获取收入 内购 内购商品购买成功.支付金额 - USD阶段累计总和
    df_income = df.query('指标=="内购商品购买成功.支付金额 - USD阶段累计总和"&时间!="阶段值"')
    datalist=['时间', 'OS','国家','adjust渠道名称','系列', '当日内购收入',]
    datalist.extend(ng_list[0:-1])
    res = df_income[datalist
    ].merge(df_nowng,on=['时间','OS','国家','adjust渠道名称','系列'])
    res['时间'] = res['时间'].str.slice(0, 10)
    col=['当日内购用户数','当日内购收入']
    col.extend(ng_list[0:-1])
    for i in col:
        res[i] = res[i].str.replace('-', '0').astype('float')

    def func(x):
        s = re.sub('\(.*\)', '', x['系列'])
        return s
    res['系列'] = res.apply(lambda x: func(x), axis=1)
    res['系列'] = res['系列'].str.strip()

    ##  通过系列前缀判断外部渠道
    res['渠道']=res.apply(lambda x:get_nerwork(x),axis=1)
    res['渠道'] = res['渠道'].str.replace('Unattributed', 'FB')

    # 根据系列查看平台

    res['平台']=res.apply(lambda x:get_pingtai(x),axis=1)


    list1=['当日内购用户数', '当日内购收入']
    list1.extend(ng_list[0:-1])
    res = res.groupby(['时间','OS','国家', '平台','渠道','系列'])[
        list1].agg({
        i:'max' for i in list1 }).reset_index()
    res['国家'] = res.apply(lambda x: get_country(x), axis=1)
    res = res.groupby(['时间','OS','国家','平台','渠道'])[list1].agg({
        i:'sum' for i in list1
    }).reset_index()
    res = res[res['国家'].isin(['TW', 'HK', 'MO', 'JP','KR','FR','DE','TH','VN','global-cn','global-us','JP1.0','global-test'])]
    return res

def updata_data():
    # 获取渠道支出安装数据
    cost_sql=f"""
    select * from adjust_campaign_cost 
    where date between '{starttime}' and '{endtime}'
    and app in ('Doomsday Vanguard ios','Doomsday Vanguard')
    """
    cost=mysql.select_data(cost_sql)
    # print(cost_sql)

    cost=cost.groupby(by=['date','app','country_code','平台','network'])[['installs','network_cost']].agg({
            'installs':'sum',
            'network_cost':'sum'
        }).reset_index()
    cost['OS']=cost['app'].apply(lambda x:'iOS' if 'ios' in x else 'Android')
    cost.columns=['时间','app','国家','平台','渠道','安装','支出','OS']
    cost['时间']=cost['时间'].astype('str')
    # print(cost)


    # 魔王数据
    mowang_gg,mowang_ng=mowang_guanggao(),mowang_neigou()
    res_mowang=mowang_gg.merge(mowang_ng,how='left',on=['时间','OS','国家','平台','渠道'])
    res_mowang.to_excel('魔王收入数据.xlsx',index=False)



    df=cost[['时间','OS','国家','平台','渠道','安装','支出']].merge(res_mowang,how='right',on=['时间','OS','国家','平台','渠道'])

    df.fillna(value=0,inplace=True)
    # 修改渠道数据 fb和gg为mb
    df['渠道']=df['渠道'].apply(lambda x: 'MB' if x in ['FB','Google Ads ACI','TTMB','Unity ads_MB','Mintegral','Twitter Installs','Line-MB','Applovin'] else x)
    df['渠道新增']=df['新增设备用户数']
    df['cpi']=df['支出']/df['渠道新增']
    res=df[['时间','OS','国家','平台','渠道','支出','渠道新增','cpi','留存率','当日内购用户数',
                '当日广告收入', '1日广告收入', '2日广告收入', '3日广告收入', '4日广告收入',
                '5日广告收入','6日广告收入', '7日广告收入','14日广告收入','21日广告收入','28日广告收入','35日广告收入','45日广告收入',
                '当日内购收入','1日内购收入', '2日内购收入', '3日内购收入', '4日内购收入',
                '5日内购收入', '6日内购收入', '7日内购收入','14日内购收入', '21日内购收入','28日内购收入','35日内购收入', '45日内购收入'
                    ]]
    res.fillna(value=0,inplace=True)
    res.replace(np.inf,0,inplace=True)

    # 将时间转化为日期格式
    # res['时间']=pd.to_datetime(res['时间'])
    res.sort_values(by='时间',ascending=True,inplace=True)

    # 本地覆盖存档
    res_an=res.query('OS!="iOS"')
    res_an.drop(columns=['OS'],inplace=True)
    res_an.to_excel('D:\Desktop\滋滋安卓渠道每日回收情况.xlsx',index=False)
    print('安卓本地存档完成')
    # res_an=pd.read_excel('D:\Desktop\滋滋安卓渠道每日回收情况.xlsx')
    res_ios=res.query('OS=="iOS"')
    res_ios.drop(columns=['OS'],inplace=True)
    res_ios.to_excel('D:\Desktop\滋滋iOS渠道每日回收情况.xlsx',index=False)
    print('iOS本地存档完成')
    # res_ios=pd.read_excel('D:\Desktop\滋滋iOS渠道每日回收情况.xlsx')
    # return df



    #安卓写入飞书表格
    tab_path = 'XVYDsydUFhpNOPtRJurcPgCCnyf'
    sheet_path = '9CtuQD'

    # 测试表格
    # tab_path = 'YlkGsdOYeh2XcQtr9YWcUiTwnKd'
    # sheet_path = 'Lw605V'

    range_list = 'A:A'
    df = feishu.read_tab(tab_path, sheet_path, range_list)
    if df.shape[0] <= 1:
        new_range = 2
    else:
        new_range = list(df.query(f'时间=="{starttime}"').index)[0] + 1
    # 写入飞书表格
    values = np.array(res_an).tolist()
    # for i  in range(0,len(values),5):
    #     if i < len(values)-5:
    #         val=values[i:i+5]
    #         num = 5
    #     else:
    #         val =values[i:]
    #         num = len(values) - i
    data_len = new_range  + len(values)  -1
    print(f'A{new_range}:AC{data_len}')
    feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:AI{data_len}', values)
        # new_range = new_range +  5
    print('-- 安卓回收更新完成')


    # ios写入飞书表格
    tab_path = 'XVYDsydUFhpNOPtRJurcPgCCnyf'
    sheet_path = 'TRzFy9'

    # 测试表格
    # tab_path = 'YlkGsdOYeh2XcQtr9YWcUiTwnKd'
    # sheet_path = 'GfJQNT'


    range_list = 'A:A'
    df = feishu.read_tab(tab_path, sheet_path, range_list)
    if df.shape[0] <= 1:
        new_range = 2
    else:
        new_range = list(df.query(f'时间=="{starttime}"').index)[0] + 1
    # 写入飞书表格
    values = np.array(res_ios).tolist()

    data_len = new_range  + len(values)  -1
    print(f'A{new_range}:AC{data_len}')
    feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:AI{data_len}', values)
        # new_range = new_range +  10
    print('-- ios回收更新完成')

if __name__ == '__main__':
    updata_data()
    pass

