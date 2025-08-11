import datetime
import pandas as pd
import sys
import requests
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import TE_app_token
from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql
mysql=Mysql_handle(mili_mysql,'data_analysis')
from tools.数数查询 import get_sqldata
from time import strftime
from time import gmtime
from tools.EM import EM_api


token=TE_app_token['Doomsday Vanguard']
nowtime=datetime.date.today().strftime('%Y-%m-%d')
endtime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
starttime =(datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')


def get_active_time():
    # 分国家数据
    sql1=f"""
    select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2 from (select group_0,group_1,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", cast(row(internal_amount_2) as row(internal_amount_2 DOUBLE))) filter (where amount_2 is not null and is_finite(amount_2) ) data_part_map_2,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_3 amount_2 from (select *, cast(coalesce(internal_amount_2, 0) as double)/60 internal_amount_3 from (select group_0,group_1,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1,arbitrary(internal_amount_2) internal_amount_2 from (select group_0,group_1,"$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0,null internal_amount_1,null internal_amount_2 from (select *, "#os" group_0,"#country_code" group_1,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'firs_device_add' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_1,null internal_amount_2 from (select *, "#os" group_0,"#country_code" group_1,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'login' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,group_1,"$__Date_Time" union all select group_0,group_1,"$__Date_Time",null internal_amount_0,null internal_amount_1,cast(coalesce(try(try_cast(SUM(ta_ev."#duration") AS DOUBLE )/COUNT(DISTINCT ta_ev."#user_id")), 0) as double) internal_amount_2 from (select *, "#os" group_0,"#country_code" group_1,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#duration","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,group_1,"$__Date_Time") group by group_0,group_1,"$__Date_Time"))) group by group_0,group_1)) ORDER BY total_amount DESC
    """
    df=get_sqldata(token,sql1)
    # print(df)
    df.columns=['os','country_code','installs','dau','active_time','col1','col2','col3','col4','col4']
    df=df[['os','country_code','installs','dau','active_time']]
    df['installs']=df['installs'].str.replace('[{}]','',regex=True).str.slice(24,29)
    df['dau'] = df['dau'].str.replace('[{}]','',regex=True).str.slice(24, 31)
    df['active_time'] = df['active_time'].str.replace('[{}]','',regex=True).str.slice(24, 29)
    print(df.head())
    # 整体数据
    sql2=f"""
    select * from (select *,count(data_map_0) over () group_num_0,count(data_map_1) over () group_num_1,count(data_map_2) over () group_num_2 from (select group_0,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,map_agg("$__Date_Time", amount_1) filter (where amount_1 is not null and is_finite(amount_1) ) data_map_1,map_agg("$__Date_Time", amount_2) filter (where amount_2 is not null and is_finite(amount_2) ) data_map_2,map_agg("$__Date_Time", cast(row(internal_amount_2) as row(internal_amount_2 DOUBLE))) filter (where amount_2 is not null and is_finite(amount_2) ) data_part_map_2,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0,internal_amount_1 amount_1,internal_amount_3 amount_2 from (select *, cast(coalesce(internal_amount_2, 0) as double)/60 internal_amount_3 from (select group_0,"$__Date_Time",arbitrary(internal_amount_0) internal_amount_0,arbitrary(internal_amount_1) internal_amount_1,arbitrary(internal_amount_2) internal_amount_2 from (select group_0,"$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0,null internal_amount_1,null internal_amount_2 from (select *, "#os" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'firs_device_add' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_1,null internal_amount_2 from (select *, "#os" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'login' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,"$__Date_Time" union all select group_0,"$__Date_Time",null internal_amount_0,null internal_amount_1,cast(coalesce(try(try_cast(SUM(ta_ev."#duration") AS DOUBLE )/COUNT(DISTINCT ta_ev."#user_id")), 0) as double) internal_amount_2 from (select *, "#os" group_0,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#duration","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev where (( ( "$part_event" IN ( 'ta_app_end' ) ) )) and (("$part_date" between '{starttime}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{endtime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{endtime}'))) group by group_0,"$__Date_Time") group by group_0,"$__Date_Time"))) group by group_0)) ORDER BY total_amount DESC 
    """
    df2=get_sqldata(token,sql2)
    df2.columns=['os','installs','dau','active_time','col1','col2','col3','col4','col4']
    df2=df2[['os','installs','dau','active_time']]
    df2['installs']=df2['installs'].str.replace('[{}]','',regex=True).str.slice(24,30)
    df2['dau'] = df2['dau'].str.replace('[{}]','',regex=True).str.slice(24, 31)
    df2['active_time'] = df2['active_time'].str.replace('[{}]','',regex=True).str.slice(24, 29)
    df2['country_code']='all'
    print(df2)
    res=pd.concat((df,df2))
    res.fillna(value=0,inplace=True)
    res=res.query('os!="HarmonyOS"')
    res['active_time']=res['active_time'].apply(lambda x:strftime("%H:%M:%S", gmtime(int(float(x)*60))))
    res['app_id']= res['os'].apply(lambda x: 1349504 if x !='iOS' else 1351835)
    res['date']=endtime
    res['app_name']=res['os'].apply(lambda x: 'Doomsday Vanguard' if x !='iOS' else 'Doomsday Vanguard ios')

    # 排序
    res.sort_values(by=['app_name','country_code','installs'],ascending=[False,False,False],inplace=True)
    # 去重
    res.drop_duplicates(subset=['app_name','country_code'],keep='first',inplace=True)

    # 入库
    res=res[['date','app_id','app_name','country_code','installs','dau','active_time']]
    res.columns=['date','app_id','app_name','app_country','newDevices','activeDevices','averageTime_PerDevice']

    print(res)
    # res.to_excel('新增活跃数据.xlsx')
    data = res.to_json(orient='records')
    print(data)
    # mysql.savedata('wangzuan_flurry', data)



# 改为em数据上报
def get_act_em():
    app_infos={'Doomsday Vanguard':['wLj6pADCXbszRclanbaKReskz61DK6YD',25624],
               'Tales of Brave':['P3ewIzs9gigHVSRLXqvF4xjubPwDz5MZ',175680]}
    res = pd.DataFrame()
    for i ,j in app_infos.items():
        app_name = i
        app_token = app_infos[i][0]
        app_id = app_infos[i][1]
        # print(app_token,app_id)

        # 分国家数据
        headers = {
            'Content-Type': 'application/json;charset=UTF-8'
        }
        url = """https://dash.engageminds.ai/api/oka/analysis/v1/event/report"""
        body1 = {"appKey":app_token,"pubAppId":app_id,"fomulars":"[]","groups":"[{\"id\":\"b8207880-5e3c-4a33-9887-4499e45b4403\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2},{\"id\":\"0b51c889-287c-48d2-b345-9ef9b05fe30d\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"id\":\"f4e7ba43-d019-4874-8065-258fcbf2ef0d\",\"f\":[],\"eid\":\"#app_install\",\"display\":\"App 安装后首次启动\",\"type\":0,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"2c496060-39cc-4a83-bcb3-cf14b3a41677\",\"f\":[],\"num\":0,\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"8b275f4a-0d2f-4223-88a8-9a267e535c4a\",\"f\":[],\"num\":0,\"eid\":\"#app_exit\",\"display\":\"App关闭\",\"type\":0,\"countType\":{\"value\":\"avgPrpo\",\"name\":\"按...求人均值(事件时长（分钟）)\",\"props\":\"#vp@duration_m\",\"propType\":0,\"type\":2,\"virtualSql\":\"event.#duration/60\"}}]","filters":"[]"}
        body1 = {"appKey":app_token,"pubAppId":app_id,"fomulars":"[]","groups":"[{\"id\":\"5fb69560-3820-4107-af05-c79ab722c705\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2},{\"id\":\"fd8e98b0-ba72-41cf-9bae-df4d5fbaaa90\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"},\"display\":\"App 安装后首次启动\",\"eid\":\"#app_install\",\"f\":[{\"id\":\"8375bbb3-e196-4fe4-955b-4322efc15ee0\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false}],\"id\":\"c60b2d46-6c33-408b-b3e8-4065d14bbccf\",\"num\":0,\"type\":0},{\"id\":\"827e0e28-fb4f-489d-a4c6-34847bfc0b34\",\"f\":[],\"num\":0,\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"d6c27529-6624-4a6f-be3a-ed6ec3efc5e1\",\"f\":[],\"num\":0,\"eid\":\"#app_exit\",\"display\":\"App关闭\",\"type\":0,\"countType\":{\"value\":\"avgPrpo\",\"name\":\"按...求人均值(事件时长（分钟）)\",\"props\":\"#vp@duration_m\",\"propType\":0,\"type\":2,\"virtualSql\":\"event.#duration/60\"}}]","filters":"[{\"eid\":\"\",\"f\":[{\"id\":\"63534a0d-ef2a-4703-a697-ff98ee587ed9\",\"f\":{},\"columnName\":\"$country\",\"display\":\"国家/地区\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is not\",\"symbolId\":\"B02\",\"ftv\":[\"CN\"],\"showCondition\":false,\"d\":1}],\"id\":\"ff743bc1-efc6-4441-b269-1442f7a1fad4\",\"noPropsPop\":true}]"}
        data = requests.post(url,headers=headers,json=body1)

        if data.json()['code']==0:
            df_country=pd.DataFrame(data.json()['data']['data'])
        else:
            df_country = pd.DataFrame()
            print(data.json())
        print(df_country.head())
        df_country['newDevices']=df_country.apply(lambda x:x['sum'] if '安装后首次启动' in  x['metric'] else 0,axis=1)
        df_country['activeDevices'] = df_country.apply(lambda x: x['sum'] if '登录' in x['metric'] else 0, axis=1)
        df_country['averageTime_PerDevice'] = df_country.apply(lambda x: x['sum'] if '事件时长' in x['metric'] else 0, axis=1)
        df_country['app_country']=df_country['country']
        df_country['app_id']= app_id

        df_country['操作系统'] = df_country['#vp@os_name']
        # 排除 其他操作系统
        df_country = df_country.query('操作系统!="其他"')
        df_country['app_name'] = df_country['#vp@os_name'].apply(lambda x: app_name if x != 'iOS' else app_name + ' ios')
        df_country['date'] = endtime
        df1= df_country.groupby(by=['date','app_id','app_name','app_country'])[['newDevices','activeDevices','averageTime_PerDevice']].agg({
            'newDevices':'max',
            'activeDevices':'max',
            'averageTime_PerDevice':'max'
        }).reset_index()

        # 不分国家数据
        body2= {"appKey":app_token,"pubAppId":app_id,"fomulars":"[]","groups":"[{\"id\":\"7efd0e87-12bc-42f1-bb35-8ac36e7fb9e3\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"id\":\"0153acdf-0947-40aa-a755-f6504b959511\",\"f\":[],\"eid\":\"#app_install\",\"display\":\"App 安装后首次启动\",\"type\":0,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"93fad8c7-beca-47b8-b6d6-2617ea3a53d5\",\"f\":[],\"num\":0,\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"8ed63adf-ca13-4654-9772-c8951ce24650\",\"f\":[],\"num\":0,\"eid\":\"#app_exit\",\"display\":\"App关闭\",\"type\":0,\"countType\":{\"value\":\"avgPrpo\",\"name\":\"按...求人均值(事件时长（分钟）)\",\"props\":\"#vp@duration_m\",\"propType\":0,\"type\":2,\"virtualSql\":\"event.#duration/60\"}}]","filters":"[]"}
        body2 = {"appKey":app_token,"pubAppId":app_id,"fomulars":"[]","groups":"[{\"id\":\"5fb69560-3820-4107-af05-c79ab722c705\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"},\"display\":\"App 安装后首次启动\",\"eid\":\"#app_install\",\"f\":[{\"id\":\"8375bbb3-e196-4fe4-955b-4322efc15ee0\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false}],\"id\":\"c60b2d46-6c33-408b-b3e8-4065d14bbccf\",\"num\":0,\"type\":0},{\"id\":\"827e0e28-fb4f-489d-a4c6-34847bfc0b34\",\"f\":[],\"num\":0,\"eid\":\"login\",\"display\":\"登录\",\"type\":1,\"countType\":{\"value\":\"unique\",\"name\":\"总人数\"}},{\"id\":\"d6c27529-6624-4a6f-be3a-ed6ec3efc5e1\",\"f\":[],\"num\":0,\"eid\":\"#app_exit\",\"display\":\"App关闭\",\"type\":0,\"countType\":{\"value\":\"avgPrpo\",\"name\":\"按...求人均值(事件时长（分钟）)\",\"props\":\"#vp@duration_m\",\"propType\":0,\"type\":2,\"virtualSql\":\"event.#duration/60\"}}]","filters":"[{\"eid\":\"\",\"f\":[{\"id\":\"63534a0d-ef2a-4703-a697-ff98ee587ed9\",\"f\":{},\"columnName\":\"$country\",\"display\":\"国家/地区\",\"selectType\":\"String\",\"symbolType\":2,\"propType\":1,\"type\":1,\"category\":0,\"calcuSymbol\":\"Is not\",\"symbolId\":\"B02\",\"ftv\":[\"CN\"],\"showCondition\":false,\"d\":1}],\"id\":\"ff743bc1-efc6-4441-b269-1442f7a1fad4\",\"noPropsPop\":true}]"}
        data = requests.post(url, headers=headers, json=body2)
        if data.json()['code'] == 0:
            df_all = pd.DataFrame(data.json()['data']['data'])
        else:
            df_all = pd.DataFrame()
        df_all['newDevices'] = df_all.apply(lambda x: x['sum'] if '安装后首次启动' in x['metric'] else 0, axis=1)
        df_all['activeDevices'] = df_all.apply(lambda x: x['sum'] if '登录' in x['metric'] else 0, axis=1)
        df_all['averageTime_PerDevice'] = df_all.apply(lambda x: x['sum'] if '事件时长' in x['metric'] else 0,axis=1)
        df_all['app_country'] = 'all'
        df_all['app_id']=app_id
        df_all['操作系统']=df_all['#vp@os_name']
        # 排除 其他操作系统
        df_all= df_all.query('操作系统!="其他"')
        df_all['app_name'] = df_all['#vp@os_name'].apply(lambda x:app_name if x != 'iOS'  else app_name + ' ios')
        df_all['date'] = endtime
        df2= df_all.groupby(by=['date','app_id','app_name','app_country'])[['newDevices','activeDevices','averageTime_PerDevice']].agg({
            'newDevices':'max',
            'activeDevices':'max',
            'averageTime_PerDevice':'max'
        }).reset_index()
        df  = pd.concat((df1,df2))
        if res.shape[0]==0:
            res=df
        else :
            res= pd.concat((res,df))
    res['averageTime_PerDevice']=res['averageTime_PerDevice'].apply(lambda x:strftime("%H:%M:%S", gmtime(int(float(x)*60))))
    return res

if __name__ == '__main__':
    # get_active_time()
    res=get_act_em()
    # 入库
    data = res.to_json(orient='records')
    mysql.savedata('wangzuan_flurry', data)
    pass