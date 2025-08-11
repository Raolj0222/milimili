import datetime
import sys
import requests
import pandas as pd
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import TE_app_token
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
from tools.数数查询 import get_sqldata
from tools.EM import EM_api

nowtime=datetime.date.today().strftime('%Y-%m-%d')
date2 = (datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d')
startime =(datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')


sql1=f"""select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@life_time" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20230801) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '{nowtime}')) ex_currency_agg FROM (( SELECT sequence(date '{nowtime}', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev inner join (select *, "#account_id" group_2 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20230801)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '{date2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{startime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{startime}'))) group by group_0,group_1,group_2,group_3,"$__Date_Time")) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC """
sql2=f"""select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@live_day" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20240102_1", "#event_time")) as double) "#vp@live_day" from (select a.*, b."@vpc_cluster_tag_20240102_1" "internal_u@@vpc_cluster_tag_20240102_1" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_47) a join (select a.*,"@vpc_cluster_tag_20240102_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_47) where "#event_date" > 20240103) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20240102_1" from user_result_cluster_47 where cluster_name = 'tag_20240102_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '{nowtime}')) ex_currency_agg FROM (( SELECT sequence(date '{nowtime}', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev inner join (select *, "#account_id" group_2 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_47) where "#event_date" > 20240103)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '{date2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{startime}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{startime}'))) group by group_0,group_1,group_2,group_3,"$__Date_Time")) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC """


# sql1="""
# /* sessionProperties: {"ignore_downstream_preferences":"true","join_distribution_type":"BROADCAST"} */
# select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@life_time" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20240213) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '2024-02-23')) ex_currency_agg FROM (( SELECT sequence(date '2024-02-23', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev inner join (select *, "#account_id" group_2 from (select a.*,if(coalesce("@vpc_cluster_cohort_20240207_212115",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_cohort_20240207_212115" from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20240213) a left join (select "#user_id" "#user_id",true "@vpc_cluster_cohort_20240207_212115" from user_result_cluster_33 where cluster_name = 'cohort_20240207_212115') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and ((("$part_date" between '2024-02-19' and '2024-02-21') and ("@vpc_tz_#event_time" >= timestamp '2024-02-20' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-02-20'))) and (ta_u."@vpc_cluster_cohort_20240207_212115"='不属于 国区用户')) group by group_0,group_1,group_2,group_3,"$__Date_Time")) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC
# """
# sql1="""/* sessionProperties: {"ignore_downstream_preferences":"true","join_distribution_type":"BROADCAST"} */
# select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,"$__Date_Time",cast(coalesce(SUM(ta_ev."#vp@currency_type__pay_amount__USD"), 0) as double) internal_amount_0 from (select *, "#os" group_0,"#country_code" group_1,"#vp@life_time" group_3,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, try_cast(try(ROUND((("pay_amount" / IF(("currency_type" = 'USD'), 1, ex_currency_agg["currency_type"])) * IF(('USD' = 'USD'), 1, ex_currency_agg['USD'])), 4)) as double) "#vp@currency_type__pay_amount__USD" from (select v_alias_currency.*, currency__data_tbl.ex_currency_agg from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select *, try_cast(try(date_diff('day', "internal_u@@vpc_cluster_tag_20230913_1", "#event_time")) as double) "#vp@life_time" from (select a.*, b."@vpc_cluster_tag_20230913_1" "internal_u@@vpc_cluster_tag_20230913_1" from (select "pay_amount","#event_name","currency_type","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33) a join (select a.*,"@vpc_cluster_tag_20230913_1" from (select * from (select "#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20240110) a left join (select "#user_id" "#user_id",tag_value_tm "@vpc_cluster_tag_20230913_1" from user_result_cluster_33 where cluster_name = 'tag_20230913_1') b0 on a."#user_id"=b0."#user_id") b on a."#user_id"=b."#user_id"))) v_alias_currency left join (SELECT ex_currency_date, ex_currency_agg FROM (SELECT CAST(ex_date AS varchar) ex_currency_date, (SELECT map_agg(currency, exchange) FROM ta_dim.ta_exchange WHERE (ex_date = '2024-01-19')) ex_currency_agg FROM (( SELECT sequence(date '2024-01-19', current_date)) currency_date (ex_all_date) CROSS JOIN UNNEST(ex_all_date) t (ex_date))) a UNION (SELECT ex_date ex_currency_date, map_agg(currency, exchange) ex_currency_agg FROM ta_dim.ta_exchange GROUP BY ex_date)) currency__data_tbl on currency__data_tbl.ex_currency_date=format_datetime( if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), v_alias_currency."#event_time"), v_alias_currency."#event_time"), 'yyyy-MM-dd'))))) ta_ev inner join (select *, "#account_id" group_2 from (select * from (select "#account_id","#update_time","#event_date","#user_id" from v_user_33) where "#event_date" > 20240110)) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'In_appPurchases_BuySuccess' ) ) )) and (("$part_date" between '2024-01-16' and '2024-01-18') and ("@vpc_tz_#event_time" >= timestamp '2024-01-17' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '2024-01-17'))) group by group_0,group_1,group_2,group_3,"$__Date_Time")) group by group_0,group_1,group_2,group_3)) ORDER BY total_amount DESC limit 1000"""
# sql2=""" """
app_list=['Doomsday Vanguard','Savior: The Stickman']
sql_list=[sql1,sql2]
def get_ng():
    ng_df=pd.DataFrame()
    for i in range(0,2):
        app=app_list[i]
        token=TE_app_token[app]
        sql=sql_list[i]
        df=get_sqldata(token,sql)
        print(df.shape)
        if df.shape[0]>1:
            df.columns = ['操作系统', 'country', 'email_code', 'live_day','date', 'money', 'users']
            df = df.query('操作系统=="Android"|操作系统=="iOS"')
            # df=df.query('操作系统=="iOS"')
            df['date'] = df['date'].str.slice(1, 11)
            df['appname'] = df['操作系统'].apply(
                lambda x: f'{app}' if x == 'Android' else f'{app} ios')
            df['money'] = df['money'].astype('float') * 0.7
            df['gift_name']=df['country']
            res = df[['date', 'appname', 'country', 'money', 'users', 'gift_name', 'email_code','live_day']]
            res=res.query('country!="CN"')
        else:
            res=pd.DataFrame()
        if ng_df.shape[0]==0:
            ng_df=res
        else:
            ng_df=pd.concat((ng_df,res))
    ng_df.to_excel('ng.xlsx')
        # 入库
    data = ng_df.to_json(orient='records')
    print(data)
    # mysql.savedata('sr_neigou', data)



# 改为em数据上报
def get_em_iap():
    # em = EM_api()# 初始化em_api
    headers = {
        'Content-Type': 'application/json;charset=UTF-8'
    }
    # 滋滋新用户内购
    url = """https://dash.engageminds.ai/api/oka/analysis/v1/event/report"""
    body1 = {"appKey": "wLj6pADCXbszRclanbaKReskz61DK6YD", "pubAppId": 25624, "fomulars": "[]",
            "groups": "[{\"id\":\"551396ba-1752-4c9c-9d5a-7eb3821c5e38\",\"display\":\"用户唯一id\",\"value\":\"$ssid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"1d848c14-78b3-4a5c-8e40-dcfa10cb5243\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"93e13231-5e04-46d9-915e-d87e7f13a9cd\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2}]",
            "tz": 0, "dateType": 2, "dateValue": "[\"Yesterday\",\"\"]",
            "events": "[{\"id\":\"da08c690-0fde-4332-ab1f-943c59b1dab8\",\"f\":[],\"eid\":\"IAP\",\"display\":\"内购成功_new\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换收入(App币种收入))\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]",
            "filters": "[{\"id\":\"8376c952-de3d-4281-8e2f-9f7f0927d38d\",\"eid\":\"\",\"f\":[{\"id\":\"35be43b8-7a23-43e2-9550-9671b2740df8\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false,\"d\":1}]}]"}

    # body1_old = {"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"fomulars":"[]","groups":"[{\"id\":\"f048cd3e-a397-451c-9b29-bae3dd474114\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2},{\"id\":\"902af05d-ca8b-48c6-9cc4-91721917c21a\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"c2dedfb0-3e40-425a-96bb-c4d4c6ada4ef\",\"display\":\"用户唯一id\",\"value\":\"$ssid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"2024-05-25\",\"2024-05-25\"]","events":"[{\"id\":\"f4a3e72a-21de-424a-a644-62de60eafcb5\",\"f\":[],\"eid\":\"IAP\",\"display\":\"内购成功_new\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换收入(App币种收入))\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]","filters":"[{\"id\":\"6da97ab1-7f97-4d28-b3a6-7caad0153377\",\"eid\":\"\",\"f\":[{\"id\":\"a080bf15-0f34-4786-b86e-2f8d72410f6c\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Equal\",\"symbolId\":\"A01\",\"ftv\":[\"0\"],\"showCondition\":false,\"d\":1}]}]"}
    # data1 = em.get_event(url,body1)
    data1 = requests.post(url, headers=headers, json=body1)
    if data1.json()['code'] == 0:
        data1 = pd.DataFrame(data1.json()['data']['data'])
    else:
        data1 = pd.DataFrame()
        return
    # 排除国区用户
    data1=data1.query('country!="CN"')
    print(data1.head())

    data1['live_day'] = 0
    data1['gift_name'] = 0
    data1['appname'] = data1['#vp@os_name'].apply(lambda x:'Doomsday Vanguard'+' ios' if x=='iOS' else 'Doomsday Vanguard')
    data1['date'] = startime
    data1 = data1[['date','appname','country','avg','ssid','live_day','gift_name']]

    # 老用户内购
    body2={"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"fomulars":"[]","groups":"[{\"id\":\"551396ba-1752-4c9c-9d5a-7eb3821c5e38\",\"display\":\"用户唯一id\",\"value\":\"$ssid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"1d848c14-78b3-4a5c-8e40-dcfa10cb5243\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"93e13231-5e04-46d9-915e-d87e7f13a9cd\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"Yesterday\",\"\"]","events":"[{\"id\":\"da08c690-0fde-4332-ab1f-943c59b1dab8\",\"f\":[],\"eid\":\"IAP\",\"display\":\"内购成功_new\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换收入(App币种收入))\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]","filters":"[{\"id\":\"8376c952-de3d-4281-8e2f-9f7f0927d38d\",\"eid\":\"\",\"f\":[{\"id\":\"35be43b8-7a23-43e2-9550-9671b2740df8\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Greater than\",\"symbolId\":\"A05\",\"ftv\":[\"0\"],\"showCondition\":false,\"d\":1}]}]"}
    # body2_old ={"appKey":"wLj6pADCXbszRclanbaKReskz61DK6YD","pubAppId":25624,"fomulars":"[]","groups":"[{\"id\":\"f048cd3e-a397-451c-9b29-bae3dd474114\",\"display\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend \",\"category\":0,\"dataType\":2},{\"id\":\"902af05d-ca8b-48c6-9cc4-91721917c21a\",\"display\":\"国家/地区\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2},{\"id\":\"c2dedfb0-3e40-425a-96bb-c4d4c6ada4ef\",\"display\":\"用户唯一id\",\"value\":\"$ssid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2}]","tz":0,"dateType":2,"dateValue":"[\"2024-05-25\",\"2024-05-25\"]","events":"[{\"id\":\"f4a3e72a-21de-424a-a644-62de60eafcb5\",\"f\":[],\"eid\":\"IAP\",\"display\":\"内购成功_new\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换收入(App币种收入))\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]","filters":"[{\"id\":\"6da97ab1-7f97-4d28-b3a6-7caad0153377\",\"eid\":\"\",\"f\":[{\"id\":\"a080bf15-0f34-4786-b86e-2f8d72410f6c\",\"f\":{},\"columnName\":\"#vp@live_day\",\"display\":\"生命周期天数\",\"selectType\":\"Number\",\"symbolType\":1,\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"calcuSymbol\":\"Greater than\",\"symbolId\":\"A05\",\"ftv\":[\"0\"],\"showCondition\":false,\"d\":1}]}]"}
    data2 = requests.post(url, headers=headers, json=body2)
    if data2.json()['code'] == 0:
        data2 = pd.DataFrame(data2.json()['data']['data'])
    else:
        data2 = pd.DataFrame()
        return
    # 排除国区用户
    data2=data2.query('country!="CN"')
    print(data2.head())
    data2['live_day'] = 1
    data2['gift_name'] = 0
    data2['appname'] = data2['#vp@os_name'].apply(lambda x:'Doomsday Vanguard'+' ios' if x=='iOS' else 'Doomsday Vanguard')
    data2['date'] = startime
    data2 = data2[['date', 'appname', 'country', 'avg', 'ssid', 'live_day', 'gift_name']]


    # 勇者内购数据
    body3={"appKey":"P3ewIzs9gigHVSRLXqvF4xjubPwDz5MZ","pubAppId":175680,"fomulars":"[]","groups":"[{\"id\":\"372415c8-558b-4567-8388-7411c87ae638\",\"display\":\"操作系统名称\",\"displayEn\":\"操作系统名称\",\"value\":\"#vp@os_name\",\"propType\":0,\"type\":2,\"virtualSql\":\"case when  event.$os = 0 then 'iOS'\\r\\n     when  event.$os = 1 then 'Android'\\r\\n    else '其他'\\r\\nend\",\"category\":0,\"dataType\":2,\"customBucket\":null},{\"id\":\"df60accf-462e-48ca-b0ae-ac3ab3270ff3\",\"display\":\"国家/地区\",\"displayEn\":\"Country/Region\",\"value\":\"$country\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2,\"customBucket\":null},{\"id\":\"f09a20fc-8963-424d-8216-2c42452bd287\",\"display\":\"用户App登录ID\",\"displayEn\":\"CDID\",\"value\":\"$cdid\",\"propType\":1,\"type\":1,\"category\":0,\"dataType\":2,\"customBucket\":null},{\"id\":\"b9a03b05-7b54-4a09-90e6-127ee9adbbb1\",\"display\":\"生命周期天数\",\"displayEn\":\"生命周期天数\",\"value\":\"#vp@live_day\",\"propType\":0,\"type\":2,\"virtualSql\":\"Datediff(DATE_FORMAT(FROM_UNIXTIME(event.$sts / 1000), '%Y-%m-%d'),DATE_FORMAT(FROM_UNIXTIME(user.$first_visit_time), '%Y-%m-%d'))\",\"category\":0,\"dataType\":1}]","tz":0,"dateType":2,"lang":"cn","dateValue":"[\"Yesterday\",\"\"]","events":"[{\"id\":\"477a9c63-b5bb-4f2d-bd04-2b5f05c2580f\",\"f\":[],\"eid\":\"In_appPurchases_BuySuccess\",\"display\":\"内购成功\",\"displayEn\":\"内购成功\",\"type\":1,\"countType\":{\"value\":\"totalProp\",\"name\":\"按...求和(转换为统一币种后收入)\",\"nameEn\":\"Total of Property value(App Currency Revenue)\",\"props\":\"#em_revenue_ex\",\"propType\":1,\"type\":0}}]","filters":"[]"}
    data3 = requests.post(url, headers=headers, json=body3)
    if data3.json()['code'] == 0:
        data3 = pd.DataFrame(data3.json()['data']['data'])
    else:
        data3 = pd.DataFrame()
        return
    data3 = data3.query('country!="CN"')
    print(data3.head())
    data3['live_day'] = data3['#vp@live_day'].astype('int')
    data3['gift_name'] = 0
    data3['appname'] = data3['#vp@os_name'].apply(
        lambda x: 'Tales of Brave' + ' ios' if x == 'iOS' else 'Tales of Brave')
    data3['date'] = startime
    data3['ssid'] = data3['cdid']
    data3 = data3[['date', 'appname', 'country', 'avg', 'ssid', 'live_day', 'gift_name']]



    res = pd.concat((data1,data2,data3))
    print(res.columns)
    res['money'] = res['avg'].astype('float')*0.7
    res['email_code'] = res['ssid']
    res['users'] = 2

    # res.columns = ['country','avg','metric','os','users','email_code','money','live_day','gift_name','appname','date']
    # res['money'] = res['money'].astype('float')*0.7
    # print(res.columns)

    data = res[['date', 'appname', 'country', 'money', 'users', 'gift_name', 'email_code','live_day']]
    data.to_excel('ng.xlsx')
    # 入库
    data_res = data.to_json(orient='records')
    mysql.savedata('sr_neigou', data_res)





if __name__ == '__main__':
    # get_ng()
    get_em_iap()
    pass