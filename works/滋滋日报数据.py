import pandas as pd
import datetime
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import TE_app_token
from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql
mysql=Mysql_handle(mili_mysql,'data_analysis')
from tools.数数api import  liucun_download
from tools.数数查询 import get_sqldata
from BI_robot.feishu import FeiShu
from works.滋滋渠道回收情况 import mowang_guanggao,mowang_neigou
feishu=FeiShu()
import numpy as np

nowtime =datetime.date.today().strftime('%Y-%m-%d')
time1 =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
time2 =(datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
time3 =(datetime.date.today() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
time6 = (datetime.date.today() - datetime.timedelta(days=6)).strftime('%Y-%m-%d')
time7 = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
time8 = (datetime.date.today() - datetime.timedelta(days=8)).strftime('%Y-%m-%d')
time15 = (datetime.date.today() - datetime.timedelta(days=15)).strftime('%Y-%m-%d')
token=TE_app_token['滋滋先遣队']


tab_path='NGw1s6YdChF7vYtJ6K6ckclwnug'
sheet_path='vHH6hF'

# 获取今日昨日新增和前日新增
def get_install():
    sql1=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 1) data_map_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 2) data_compare_map_0_0,sum(amount_0) filter (where is_finite(amount_0) ) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,"$__Date_Time",date_mark,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1 from (SELECT *, timestamp '1981-01-01' "$__Date_Time" from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)) join (values 1,2) b(date_mark) on if(("$part_date" between '{time2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')), 1, 0)=b.date_mark or if(("$part_date" between '{time3}' and '{time1}') and ("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')), 2, 0)=b.date_mark)) ta_ev inner join (select *, "#vp@network_name" group_2 from (select *, try_cast(try(CASE when "#adjust_network_name"='Organic' or "#adjust_network_name"='Google Organic Search' then '自然量' else '买量' END) as varchar) "#vp@network_name" from (select * from (select "#update_time","#event_date","#user_id","#adjust_network_name" from v_user_33) where "#event_date" > 20231127))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'firs_device_add' ) ) )) and (((("$part_date" between '{time3}' and '{time1}') or ("$part_date" between '{time2}' and '{nowtime}')) and (("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')) or ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')))) and (ta_ev."#country_code" NOT IN ('CN'))) group by group_0,group_1,group_2,"$__Date_Time",date_mark)) group by group_0,group_1,group_2)) ORDER BY total_amount DESC limit 1000
    """
    df=get_sqldata(token,sql1)
    print(df.head())
    df.columns=['OS','国家','渠道','今日','昨日','col1','col2']
    df_installs=df[['OS','国家','渠道','今日','昨日']]


    df_installs=df_installs.query('OS!="HarmonyOS"')
    # 提取新增用户字段
    df_installs['今日']=df_installs['今日'].str.replace('{1981-01-01 00:00:00:','').str.replace('}','')
    df_installs['昨日']=df_installs['昨日'].str.replace('{1981-01-01 00:00:00:','').str.replace('}','')
    df_installs=df_installs.replace('null', 0)
    df_installs['今日'] = df_installs['今日'].astype('float')
    df_installs['昨日'] = df_installs['昨日'].astype('float')
    # 投放地区  JP、TW、KR、
    country_list=['JP','TW','KR','VN','TH','HK']
    # 每日进量前5的地区
    df1=df_installs.groupby(by=['国家'])['今日'].sum().reset_index()
    df1.sort_values(by='今日',ascending=False,inplace=True)
    country_list_5=df1.head(5)['国家'].tolist()

    df_spend=df_installs[df_installs['国家'].isin(country_list)]
    df_spend.sort_values(by=['OS','国家','渠道'],inplace=True)
    df_Organic=df_installs[~df_installs['国家'].isin(country_list)]
    df_Organic=df_Organic.query('渠道=="自然量"')
    df_Organic.sort_values(by='今日',ascending=False,inplace=True)
    df_Organic=df_Organic.head(10)

    # 飞书写入
    values = np.array(df_spend).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'B3:F{len(values)+2}', values)

    values = np.array(df_Organic).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'H3:L{len(values) + 2}', values)
    return country_list_5


country_list=get_install()
# 新增用户的游戏情况数据
# 新增用户首日时长
def get_new_time():
    sql=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0) as row(internal_amount_0 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_1 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/60 internal_amount_1 from (select "$__Date_Time",cast(coalesce(try(try_cast(SUM(ta_ev."#duration") AS DOUBLE )/COUNT(DISTINCT ta_ev."#user_id")), 0) as double) internal_amount_0 from (select *, ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#duration","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231122) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by "$__Date_Time"))))) ORDER BY total_amount DESC limit 1000
    """
    sql2=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0) as row(internal_amount_0 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_1 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/60 internal_amount_1 from (select group_0,"$__Date_Time",cast(coalesce(try(try_cast(SUM(ta_ev."#duration") AS DOUBLE )/COUNT(DISTINCT ta_ev."#user_id")), 0) as double) internal_amount_0 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#duration","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231201) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ((( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    df=get_sqldata(token,sql2)
    df=df[df.iloc[:,0].isin(country_list)]

    # 获取每日时长数据
    # 转置数据
    df1=df.iloc[:,0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns=date_list
    for i in date_list[1:]:
        df1[i]=df1[i].str.replace('{','').str.slice(24,29)
    df1=df1.T
    df1.reset_index(inplace=True)

    df1.sort_values(by='index',ascending=True,inplace=True)
    # 写入飞书

    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'A37:F{len(values) + 36}', values)
    return df

def get_new_fight():
    sql=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select "$__Date_Time",cast(coalesce(COUNT(if((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0)),1)), 0) as double) internal_amount_0,cast(coalesce(COUNT(DISTINCT if((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0)),ta_ev."#user_id")), 0) as double) internal_amount_1 from (select *, ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231122) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ("$part_event" in ('Game_Level_enter')) and ((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by "$__Date_Time"))))) ORDER BY total_amount DESC limit 1000
    """
    sql1=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(if((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0)),1)), 0) as double) internal_amount_0,cast(coalesce(COUNT(DISTINCT if((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0)),ta_ev."#user_id")), 0) as double) internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231201) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ("$part_event" in ('Game_Level_enter')) and ((( ( "$part_event" IN ( 'Game_Level_enter' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000    
    """
    df = get_sqldata(token, sql1)
    # 转置数据
    df = df[df.iloc[:, 0].isin(country_list)]

    # 获取每日时长数据
    # 转置数据
    df1 = df.iloc[:, 0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns = date_list
    for i in date_list[1:]:
        df1[i] = df1[i].str.replace('{', '').str.slice(24, 27)
    df1 = df1.T
    df1.reset_index(inplace=True)
    df1.sort_values(by='index', ascending=True, inplace=True)

    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'A47:F{len(values) + 46}', values)
    return df

def get_liucun():
    headers = {
        'Content-Type': 'application/json'
    }
    params = {
        'token': 'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    }
    body = {"eventView":{"collectFirstDay":1,"endTime":f"{time2} 23:59:59","filts":[],"firstDayOfWeek":1,"groupBy":[{"columnDesc":"国家地区代码","columnName":"#country_code","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"}],"recentDay":"2-15","relation":"and","startTime":f"{time15} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":1},"events":[{"eventName":"ta_app_install","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"PER_CAPITA_TIMES","analysisDesc":"人均次数","eventName":"Game_Level_enter","eventNameDisplay":"","filts":[],"quota":"","relation":"and","relationUser":"and","type":"simultaneous_display"}],"projectId":33}
    df = liucun_download(headers, params, body, 6)
    df.columns=['初始事件发生时间','国家','安装','指标','当日','次留']
    df1=df.query('初始事件发生时间!="阶段值"&指标=="留存率"')
    df2=df.query('初始事件发生时间!="阶段值"&指标=="开始闯关.null人均次数"')
    df2.columns=['初始事件发生时间','国家','安装','指标','当日','次日闯关人均']

    body2={"eventView":{"collectFirstDay":1,"endTime":f"{time2} 23:59:59","filts":[],"firstDayOfWeek":1,"groupBy":[{"columnDesc":"国家地区代码","columnName":"#country_code","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"}],"recentDay":"2-15","relation":"and","startTime":f"{time15} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":1},"events":[{"eventName":"ta_app_install","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysisDesc":"","customEvent":"ta_app_end.#duration.PER_CAPITA_NUM/60","customFilters":[],"eventName":"人均时长","eventNameDisplay":"人均时长","filts":[],"format":"float","quota":"","type":"simultaneous_display"}],"projectId":33}
    df3 = liucun_download(headers, params, body2, 6)
    df3.columns = ['初始事件发生时间','国家', '安装', '指标', '当日', '次日人均时长']
    df3 = df3.query('初始事件发生时间!="阶段值"&指标=="人均时长"')

    body3={"eventView":{"collectFirstDay":1,"endTime":f"{time2} 23:59:59","filts":[],"firstDayOfWeek":1,"groupBy":[{"columnDesc":"国家地区代码","columnName":"#country_code","propertyRange":"","specifiedClusterDate":f"{nowtime}","subTableType":"","tableType":"event"}],"recentDay":"2-15","relation":"and","startTime":f"{time15} 00:00:00","statType":"retention","taIdMeasureVo":{"columnDesc":"用户唯一ID","columnName":"#user_id","tableType":"event"},"timeParticleSize":"day","unitNum":1},"events":[{"eventName":"ta_app_install","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"first"},{"eventName":"login","eventNameDisplay":"","filts":[],"relation":"and","relationUser":"and","type":"second"},{"analysis":"PER_CAPITA_NUM","analysisDesc":"人均值","eventName":"Game_Level_Finish","eventNameDisplay":"人均复活次数","filts":[],"quota":"level_revive_num","relation":"and","type":"simultaneous_display"}],"projectId":33}
    df4=liucun_download(headers, params, body3, 6)
    df4.columns = ['初始事件发生时间','国家', '安装', '指标', '当日', '次日人均复活']
    df4 = df4.query('初始事件发生时间!="阶段值"&指标=="人均复活次数"')
    # print(df1.head(),df2.head(),df3.head(),df4.head())

    df1=df1[['初始事件发生时间','国家','次留']]
    df1['次留']=df1['次留'].str.replace('%','').astype('float')/100
    df2=df2[['初始事件发生时间','国家','次日闯关人均']]
    df2['次日闯关人均']=df2['次日闯关人均'].astype('float')
    df3=df3[['初始事件发生时间','国家','次日人均时长']]
    df3['次日人均时长']=df3['次日人均时长'].astype('float')
    df4=df4[['初始事件发生时间','国家','次日人均复活']]
    df4['次日人均复活']=df4['次日人均复活'].astype('float')

    res = df1.merge(df2, on=['初始事件发生时间','国家']).merge(df3,on=['初始事件发生时间','国家']).merge(df4,on=['初始事件发生时间','国家'])
    res.sort_values(by=['国家','初始事件发生时间'],ascending=[True,True],inplace=True)
    res=res[res['国家'].isin(country_list)]
    # 写入飞书
    values = np.array(res).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'H28:M{len(values) + 27}', values)
    return res


def get_ng():

    # 内购用户数和金额
    sql1=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 1) data_map_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 2) data_compare_map_0_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",date_mark,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@user_type" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF((date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time") <= 0), U&'\65B0\7528\6237', U&'\8001\7528\6237')) as varchar) "#vp@user_type" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231128) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) join (values 1,2) b(date_mark) on if(("$part_date" between '{time2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')), 1, 0)=b.date_mark or if(("$part_date" between '{time3}' and '{time1}') and ("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')), 2, 0)=b.date_mark)) ta_ev inner join (select *, "#vp@network_name" group_2 from (select *, try_cast(try(CASE when "#adjust_network_name"='Organic' or "#adjust_network_name"='Google Organic Search' then '自然量' else '买量' END) as varchar) "#vp@network_name" from (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id","#adjust_network_name" from v_user_33) where "#event_date" > 20231128) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id"))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (((("$part_date" between '{time3}' and '{time1}') or ("$part_date" between '{time2}' and '{nowtime}')) and (("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')) or ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by group_0,group_1,group_2,group_3,"$__Date_Time",date_mark)) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC limit 1000    """
    df1 = get_sqldata(token, sql1)
    df1.columns=['OS','国家','渠道','新老用户','昨日','前日','col1','col2']
    df1['昨日'] = df1['昨日'].str.replace(r'(.*)(00:00:00.000:)', '',regex=True).str.replace('}', '')
    df1['前日'] = df1['前日'].str.replace(r'(.*)(00:00:00.000:)', '',regex=True).str.replace('}', '')
    df1 = df1.replace('null', 0)
    df1['前日'] = df1['前日'].astype('float')
    df1['昨日'] = df1['昨日'].astype('float')
    df1['新老用户']=df1['新老用户'].apply(lambda x: '老用户' if '老' in str(x) else '新用户')
    df1=df1[['OS','国家','渠道','新老用户','昨日','前日']]

    sql2=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 1) data_map_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 2) data_compare_map_0_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",date_mark,cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@user_type" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF((date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time") <= 0), U&'\65B0\7528\6237', U&'\8001\7528\6237')) as varchar) "#vp@user_type" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231128) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '{nowtime}')) ex_currency_agg FROM (( SELECT sequence(date '{nowtime}', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))) join (values 1,2) b(date_mark) on if(("$part_date" between '{time2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')), 1, 0)=b.date_mark or if(("$part_date" between '{time3}' and '{time1}') and ("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')), 2, 0)=b.date_mark)) ta_ev inner join (select *, "#vp@network_name" group_2 from (select *, try_cast(try(CASE when "#adjust_network_name"='Organic' or "#adjust_network_name"='Google Organic Search' then '自然量' else '买量' END) as varchar) "#vp@network_name" from (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id","#adjust_network_name" from v_user_33) where "#event_date" > 20231128) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id"))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (((("$part_date" between '{time3}' and '{time1}') or ("$part_date" between '{time2}' and '{nowtime}')) and (("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')) or ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by group_0,group_1,group_2,group_3,"$__Date_Time",date_mark)) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC limit 1000
    """
    df2 = get_sqldata(token, sql2)
    df2.columns = ['OS', '国家', '渠道', '新老用户', '昨日金额', '前日金额', 'col1', 'col2']
    df2['昨日金额'] = df2['昨日金额'].str.replace(r'(.*)(00:00:00.000:)', '',regex=True).str.replace('}', '')
    df2['前日金额'] = df2['前日金额'].str.replace(r'(.*)(00:00:00.000:)', '',regex=True).str.replace('}', '')
    df2 = df2.replace('null', 0)
    df2['前日金额'] = df2['前日金额'].astype('float')
    df2['昨日金额'] = df2['昨日金额'].astype('float')
    df2['新老用户'] = df2['新老用户'].apply(lambda x: '老用户' if '老' in str(x) else '新用户')
    df2 = df2[['OS', '国家', '渠道', '新老用户', '昨日金额', '前日金额']]

    sql3=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 1) data_map_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) and date_mark = 2) data_compare_map_0_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",date_mark,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@user_type" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(IF((date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time") <= 0), U&'\65B0\7528\6237', U&'\8001\7528\6237')) as varchar) "#vp@user_type" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231128) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) join (values 1,2) b(date_mark) on if(("$part_date" between '{time2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')), 1, 0)=b.date_mark or if(("$part_date" between '{time3}' and '{time1}') and ("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')), 2, 0)=b.date_mark)) ta_ev inner join (select *, "#vp@network_name" group_2 from (select *, try_cast(try(CASE when "#adjust_network_name"='Organic' or "#adjust_network_name"='Google Organic Search' then '自然量' else '买量' END) as varchar) "#vp@network_name" from (select * from (select "#update_time","#event_date","#user_id","#adjust_network_name" from v_user_33) where "#event_date" > 20231128))) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'login' ) ) )) and ((("$part_date" between '{time3}' and '{time1}') or ("$part_date" between '{time2}' and '{nowtime}')) and (("@vpc_tz_#event_time" >= timestamp '{time2}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time2}')) or ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}')))) group by group_0,group_1,group_2,group_3,"$__Date_Time",date_mark)) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC limit 1000
    """
    df3 = get_sqldata(token, sql3)
    df3.columns = ['OS', '国家', '渠道', '新老用户', '昨日活跃', '前日活跃', 'col1', 'col2']
    df3['昨日活跃'] = df3['昨日活跃'].str.replace(r'(.*)(00:00:00.000:)', '', regex=True).str.replace('}', '')
    df3['前日活跃'] = df3['前日活跃'].str.replace(r'(.*)(00:00:00.000:)', '', regex=True).str.replace('}', '')
    df3 = df3.replace('null', 0)
    df3['前日活跃'] = df3['前日活跃'].astype('float')
    df3['昨日活跃'] = df3['昨日活跃'].astype('float')
    df3['新老用户'] = df3['新老用户'].apply(lambda x: '老用户' if '老' in str(x) else '新用户')
    df3 = df3[['OS', '国家', '渠道', '新老用户', '昨日活跃', '前日活跃']]


    res=df1.merge(df2,on=['OS', '国家', '渠道', '新老用户']).merge(df3,how='left',on=['OS', '国家', '渠道', '新老用户'])

    res.fillna(value='-',inplace=True)
    values = np.array(res).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'O3:X{len(values) + 2}', values)


    # 商品购买情况
    sql4=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(1), 0) as double) internal_amount_0 from (select *, "#vp@ggshopid_new@name" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select * from (select *, try_cast(try(replace("googleshopid", 'vanguard_', '')) as varchar) "#vp@ggshopid_new" from (select "#event_name","googleshopid","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33)) logic_table left join ta_dim."dim_33_0_vprop_sql_249" on logic_table."#vp@ggshopid_new" = "dim_33_0_vprop_sql_249"."#vp@ggshopid_new@ggid")))) ta_ev inner join (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231123) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id") ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by group_0,"$__Date_Time")) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    # print(time7,time6)
    df4=get_sqldata(token,sql4)
    print(df4.shape)
    df4.columns=['name','col1','col2','col3','col4','col5','col6','col7','合计','行数']
    df4.to_excel('test.xlsx')
    df4.fillna(value=0,inplace=True)
    df4['合计']=df4['合计'].astype('float')
    df4['行数'] = df4['行数'].astype('float')

    df4=df4.query('行数>0&合计>28')

    # 最近7日数据
    df5=df4[['name','合计']]

    # 每日详细数据
    date_list=['name']
    for i in df4.iloc[0,:].tolist()[1:]:
        if len(str(i))>10:
            col=str(i).replace('{','')[0:10]
        else:
            col=i
        date_list.append(col)

    df4.columns=date_list
    df6=df4[['name',f'{time1}',f'{time2}']]
    df6[f'{time1}'] = df6[f'{time1}'].str.replace(r'(.*)(00:00:00.000:)', '',regex=True).str.replace('}', '')
    df6[f'{time2}'] = df6[f'{time2}'].str.replace(r'(.*)(00:00:00.000:)', '', regex=True).str.replace('}', '')

    df6[f'{time1}'] = df6[f'{time1}'].astype('float')
    df6[f'{time2}'] = df6[f'{time2}'].astype('float')

    res=df5.merge(df6,on='name')

    values = np.array(res).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'AE3:AH{len(values) + 2}', values)

    # 新老用户每日复购率
    sql5 =f"""
    /* SQL for Events: 2 */
    select * from (select *,count(data_map_0) over () group_num_0 from (select map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select "$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select "$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0,null internal_amount_1 from (select *, ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#event_time","#zone_offset","is_first_pay","#user_id","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id") ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."is_first_pay" = FALSE) and (ta_ev."#vp@life_time" NOT IN (0))) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by "$__Date_Time" union all select "$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_1 from (select *, ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#zone_offset","#user_id","#event_time","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id","total_pay_num" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id") ta_u on ta_ev."#user_id" = ta_u."#user_id" where ((( ( "$part_event" IN ( 'login' ) ) )) and (ta_u."total_pay_num" > 0) and (ta_ev."#vp@life_time" NOT IN (0))) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by "$__Date_Time") group by "$__Date_Time"))))) ORDER BY total_amount DESC limit 1000
    """
    sql6=f"""
    /* SQL for Events: 1 */
    select * from (select *,count(data_map_0) over () group_num_0 from (select map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select "$__Date_Time",cast(coalesce(COUNT(DISTINCT if((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."is_first_pay" = FALSE) and (ta_ev."#vp@life_time" IN (0)),ta_ev."#user_id")), 0) as double) internal_amount_0,cast(coalesce(COUNT(DISTINCT if((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."#vp@life_time" IN (0)),ta_ev."#user_id")), 0) as double) internal_amount_1 from (select *, ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#event_time","#zone_offset","is_first_pay","#user_id","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev inner join (select a.*,if(coalesce("@vpc_cluster_a20230831_105039",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_a20230831_105039" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231124) a left join (select "#user_id" "#user_id",true "@vpc_cluster_a20230831_105039" from user_result_cluster_33 where cluster_name = 'a20230831_105039') b0 on a."#user_id"=b0."#user_id") ta_u on ta_ev."#user_id" = ta_u."#user_id" where ("$part_event" in ('In_appPurchases_BuySuccess')) and (((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."is_first_pay" = FALSE) and (ta_ev."#vp@life_time" IN (0))) or ((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."#vp@life_time" IN (0)))) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_u."@vpc_cluster_a20230831_105039"='不属于 国区用户')) group by "$__Date_Time"))))) ORDER BY total_amount DESC limit 1000
    """
    df7=get_sqldata(token,sql5).T
    df7.columns=['数据','新用户复购率']
    df7 = df7.head(7)
    # print(df7)
    df7['日期']=df7['数据'].str.replace('{','').str.slice(0,10)
    df7['新用户复购率']=df7['数据'].str.replace('{','').str.slice(24,30)



    df8=get_sqldata(token,sql6).T
    df8.columns=['数据','老用户复购率']
    df8 = df8.head(7)
    df8['日期']=df8['数据'].str.replace('{','').str.slice(0,10)
    df8['老用户复购率']=df8['数据'].str.replace('{','').str.slice(24,30)


    res=df7[['日期','新用户复购率']].merge(df8[['日期','老用户复购率']],on='日期')
    # print(res)

    res['新用户复购率']=res['新用户复购率'].astype('float')
    res['老用户复购率'] = res['老用户复购率'].astype('float')
    res.sort_values(by='日期',ascending=True,inplace=True)

    values = np.array(res).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'AJ3:AL{len(values) + 2}', values)

    # 头部付费玩家列表
    sql9=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#country_code" group_1,"#os" group_2,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '{nowtime}')) ex_currency_agg FROM (( SELECT sequence(date '{nowtime}', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev inner join (select *, "#account_id" group_0 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231130)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '{time2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by group_0,group_1,group_2,"$__Date_Time")) group by group_0,group_1,group_2)) ORDER BY total_amount DESC limit 1000
    """
    df9=get_sqldata(token,sql9).head(20)
    df9.columns=['账户ID','国家','操作系统','日期','金额','总人数']
    df9=df9[['账户ID','国家','操作系统','金额']]
    df9['金额']=df9['金额'].astype('float')
    values = np.array(df9).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'AN3:AQ{len(values) + 2}', values)


def ng_gpdata():
    # 新用户内购率
    sql1=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select group_0,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0,null internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231201) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ((( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_ev."#os" NOT IN ('HarmonyOS'))) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20231201) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))))) ta_ev where ((( ( "$part_event" IN ( 'login' ) ) )) and (ta_ev."#vp@life_time" IN (0))) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_ev."#os" NOT IN ('HarmonyOS'))) group by group_0,"$__Date_Time") group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    df = get_sqldata(token, sql1)
    df = df[df.iloc[:, 0].isin(country_list)]
    df1 = df.iloc[:, 0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns = date_list
    for i in date_list[1:]:
        df1[i] = df1[i].str.replace('{', '').str.replace('}', '').str.replace("'", '').str.slice(24, 29).astype('float')
    df1 = df1.T
    df1.reset_index(inplace=True)

    df1.sort_values(by='index', ascending=True, inplace=True)
    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'AS2:AX{len(values) + 1}', values)

    # 活跃付费率
    sql2=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select group_0,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0,null internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_ev."#os" NOT IN ('HarmonyOS'))) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'login' ) ) )) and ((("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) and (ta_ev."#os" NOT IN ('HarmonyOS'))) group by group_0,"$__Date_Time") group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    df = get_sqldata(token, sql2)
    df = df[df.iloc[:, 0].isin(country_list)]
    df1 = df.iloc[:, 0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns = date_list
    for i in date_list[1:]:
        df1[i] = df1[i].str.replace('{', '').str.slice(24, 29).astype('float')
    df1 = df1.T
    df1.reset_index(inplace=True)

    df1.sort_values(by='index', ascending=True, inplace=True)
    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'AS12:AX{len(values) + 11}', values)

    #人均内购次数
    sql3=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(if(( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) ),1)), 0) as double) internal_amount_0,cast(coalesce(COUNT(DISTINCT if(( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) ),ta_ev."#user_id")), 0) as double) internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33)))) ta_ev where ("$part_event" in ('In_appPurchases_BuySuccess')) and (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    df = get_sqldata(token, sql3)
    df = df[df.iloc[:, 0].isin(country_list)]
    df1 = df.iloc[:, 0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns = date_list
    for i in date_list[1:]:
        if i in list(df1.columns):
            df1[i] = df1[i].str.replace('[{}]', '',regex=True).str.slice(24, 29).astype('float')
        else:
            pass
    df1 = df1.T
    df1.reset_index(inplace=True)

    df1.sort_values(by='index', ascending=True, inplace=True)
    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'AS22:AX{len(values) + 21}', values)

    # arppu
    sql4=f"""
    select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", cast(row(internal_amount_0,internal_amount_1) as row(internal_amount_0 DOUBLE,internal_amount_1 DOUBLE))) filter (where amount_0 is not null and is_finite(amount_0) ) data_part_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_2 amount_0 from (select *, cast(coalesce(internal_amount_0, 0) as double)/try_cast(cast(coalesce(internal_amount_1, 0) as double) AS DOUBLE) internal_amount_2 from (select group_0,"$__Date_Time",cast(coalesce(SUM(if(( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) ),ta_ev."#vp@currency_type__pay_amount__USD")), 0) as double) internal_amount_0,cast(coalesce(COUNT(DISTINCT if(( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) ),ta_ev."#user_id")), 0) as double) internal_amount_1 from (select *, "#country_code" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","$part_date","$part_event" from v_event_33)) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '{nowtime}')) ex_currency_agg FROM (( SELECT sequence(date '{nowtime}', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev where ("$part_event" in ('In_appPurchases_BuySuccess')) and (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '{time8}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{time7}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{time1}'))) group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC limit 1000
    """
    df = get_sqldata(token, sql4)
    df = df[df.iloc[:, 0].isin(country_list)]
    df1 = df.iloc[:, 0:8]
    date_list = ['0日期']
    for i in df1.iloc[0, :].tolist()[1:]:
        if len(str(i)) > 10:
            col = str(i).replace('{', '')[0:10]
        else:
            col = i
        date_list.append(col)
    df1.columns = date_list
    for i in date_list[1:]:
        df1[i] = df1[i].str.replace('{', '').str.slice(24, 29).astype('float')
    df1 = df1.T
    df1.reset_index(inplace=True)

    df1.sort_values(by='index', ascending=True, inplace=True)
    values = np.array(df1).tolist()
    feishu.insert_tab_new(tab_path, sheet_path, f'AS32:AX{len(values) + 31}', values)
    print('新老用户内购数据更新完成')






# 渠道回收情况
def network_data():
    endtime = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    starttime = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
    # 获取渠道支出数据
    cost_sql = f"""
    select * from wangzhuan_spend_new 
    where date between '{starttime}' and '{endtime}'
    and appname in ('Doomsday Vanguard ios','Doomsday Vanguard')
    """
    install_sql=f"""
    select * from adjust_campaign_cost 
    where date between '{starttime}' and '{endtime}'
    and app in ('Doomsday Vanguard ios','Doomsday Vanguard')
    """
    cost = mysql.select_data(cost_sql)
    # print(cost)


    cost = cost.groupby(by=['date', 'appname', 'country_code', 'channel'])[['cost']].agg({
        'cost': 'sum'
    }).reset_index()
    cost['OS'] = cost['appname'].apply(lambda x: 'iOS' if 'ios' in x.lower() else 'Android')
    cost.columns = ['时间', 'app', '国家', 'channel', '支出', 'OS']
    cost['时间'] = cost['时间'].astype('str')


    #安装数据
    install_data = mysql.select_data(install_sql)
    # print(install_data)
    install_data['network'] = install_data['network'].apply(lambda x: 'MB' if x in ['FB', 'Google Ads ACI', 'TTMB', 'Unity ads_MB'] else x)
    install_data['channel']=install_data['平台']+'-'+install_data['network']
    install_data = install_data.groupby(by=['date', 'app', 'country_code', 'channel'])[['installs']].agg({
        'installs': 'sum'
    }).reset_index()
    install_data['OS'] = install_data['app'].apply(lambda x: 'iOS' if 'ios' in x else 'Android')
    install_data.columns = ['时间', 'app', '国家',  'channel', '安装', 'OS']
    install_data['时间'] = install_data['时间'].astype('str')

    #
    network=cost.merge(install_data,on=['时间', 'app', '国家', 'channel','OS'])
    # print(network)

    # 魔王数据
    mowang_gg, mowang_ng = mowang_guanggao(), mowang_neigou()
    res_mowang = mowang_gg.merge(mowang_ng, how='left', on=['时间', 'OS', '国家', '平台', '渠道'])
    res_mowang['渠道'] = res_mowang['渠道'].apply(lambda x: 'MB' if x in ['FB', 'Google Ads ACI', 'TTMB', 'Unity ads_MB'] else x)
    res_mowang['channel']=res_mowang['平台']+ '-' + res_mowang['渠道']


    df = network[['时间', 'OS', '国家',  'channel', '安装', '支出']].merge(res_mowang, how='left',
                                                                            on=['时间', 'OS', '国家', 'channel'])
    df['cpi'] = df['支出'] / df['安装']
    # 修改渠道数据 fb和gg为mb
    df['渠道'] = df['渠道'].apply(lambda x: 'MB' if x in ['FB', 'Google Ads ACI', 'TTMB', 'Unity ads_MB'] else x)
    res = df[['时间', 'OS', '国家', 'channel', '支出', '安装', 'cpi', '留存率', '当日内购用户数',
              '当日广告收入', '当日内购收入']]
    res.fillna(value=0, inplace=True)
    res.replace(np.inf, 0, inplace=True)
    date_list=[f'{time1}',f'{time2}']
    res=res[res['时间'].isin(date_list)]
    res1=res[res['时间']==f'{time1}']
    res2=res[res['时间']==f'{time2}']
    res2.columns=['时间1', 'OS', '国家', 'channel', '前日支出', '前日安装', '前日cpi', '前日留存率', '前日当日内购用户数',
              '前日当日广告收入', '前日当日内购收入']
    res_new=res1.merge(res2,how='left',on=['OS', '国家', 'channel'])
    res_new.drop(columns=['时间','时间1'],inplace=True)


    res_new.fillna(value=0,inplace=True)
    # print(res_new.head())

    # 详细数据写入飞书表格
    tab_path='NGw1s6YdChF7vYtJ6K6ckclwnug'
    sheet_path='jwNVi1'
    values = np.array(res_new).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'B2:R{len(values) + 1}', values)

    # 大渠道数据写入飞书表格
    def func(x):
        ind=str(x).find('-')
        return x[0:ind]
    res_new['大渠道']=res_new['channel'].apply(lambda x :func(x))
    res_new['当日回收']=res_new['当日广告收入']+res_new['当日内购收入']
    res_new['前日当日回收']=res_new['前日当日广告收入']+ res_new['前日当日内购收入']
    big_qudao=res_new.groupby(by=['OS', '国家','大渠道'])[['支出','安装','当日回收','前日支出','前日安装','前日当日回收']].agg({
        '支出':'sum',
        '安装':'sum',
        '当日回收':'sum',
        '前日支出':'sum',
        '前日安装':'sum',
        '前日当日回收':'sum'
    }).reset_index()
    tab_path = 'NGw1s6YdChF7vYtJ6K6ckclwnug'
    sheet_path = 'v1SI97'
    values = np.array(big_qudao).tolist()
    feishu.insert_tab(tab_path, sheet_path, f'B2:K{len(values) + 1}', values)

def get_shequn():
    # 社群数据飞书表格信息
    tab_path=''
    sheet_path=''
    df=feishu.read_tab(tab_path,sheet_path,'')



    pass


if __name__ == '__main__':
    # get_install()
    get_new_time()
    get_new_fight()
    get_liucun()
    get_ng()
    ng_gpdata()
    network_data()
    pass
