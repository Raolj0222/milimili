import datetime
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')

# 导入飞书板块，读取每日渠道支出
from BI_robot.feishu import FeiShu
feishu=FeiShu()

# 导入数数查询板块
from configs import TE_app_token
from tools.数数查询 import get_sqldata
from 滋滋渠道回收情况 import get_nerwork,get_pingtai,get_country
import pandas as pd

# 回传数数
from tools.TEhandle  import TE
# 初始化
te=TE('hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ','33')


def CPI():
    data = get_sqldata(TE_app_token['Doomsday Vanguard'], sql)
    user_data = data.iloc[:, 0:7]
    user_data.columns = ['distinct_id', 'os', '国家', 'adjust渠道名称', '系列_gg', '系列_fb', 'install_time']
    user_data['install_time'] = user_data['install_time'].str.slice(1, 11)
    user_data = user_data[user_data['distinct_id'] == user_data['distinct_id']]
    user_data['系列'] = user_data.apply(lambda x: x['系列_gg'] if x['系列_fb'] == 'null' else x['系列_fb'], axis=1)

    user_data['渠道'] = user_data.apply(lambda x: get_nerwork(x), axis=1)
    user_data['渠道'] = user_data['渠道'].str.replace('Unattributed', 'FB')
    user_data['平台'] = user_data.apply(lambda x: get_pingtai(x), axis=1)
    user_data['国家'] = user_data.apply(lambda x: get_country(x), axis=1)
    user_data['渠道'] = user_data['渠道'].apply(
        lambda x: 'MB' if x in ['FB', 'Google Ads ACI', 'TTMB', 'Unity ads_MB', 'Mintegral', 'Twitter Installs',
                                'Line-MB', 'Applovin'] else x)

    # 安卓用户
    android_user = user_data.query('os=="Android"')
    # ios用户
    ios_user = user_data.query('os=="iOS"')
    # ios处理其他渠道
    android_user['匹配列'] = android_user['平台'] + android_user['渠道'] + android_user['国家']
    ios_user['匹配列'] = ios_user['平台'] + ios_user['渠道'] + ios_user['国家']
    ios_user['匹配列'] = ios_user.apply(
        lambda x: '其他' if 'Applovin' not in x['匹配列'] and 'Unity' not in x['匹配列'] else x['匹配列'], axis=1)

    # 安卓买量支出
    android = ['XVYDsydUFhpNOPtRJurcPgCCnyf', 'fcd655']
    df_android = feishu.read_tab(android[0], android[1], 'A1:H10000')
    df_android = df_android.query(f'日期=="{date1}"')
    df_android.dropna(how='any',inplace=True)

    df_android = df_android[['平台', '代理/渠道', '地区', '花费']]
    df_android.columns = ['平台', '渠道', '国家', '花费']
    df_android['os'] = 'Android'

    # ios买量支出
    ios = ['XVYDsydUFhpNOPtRJurcPgCCnyf', 'Scf3Q5']
    df_ios = feishu.read_tab(ios[0], ios[1], 'A1:H10000')
    df_ios = df_ios.query(f'日期=="{date1}"')
    df_ios = df_ios[['平台', '代理/渠道', '地区', '花费']]
    df_ios.columns = ['平台', '渠道', '国家', '花费']
    df_ios.dropna(how='any',inplace=True)
    df_ios['os'] = 'iOS'
    # 匹配列
    df_android['匹配列'] = df_android['平台'].astype('str') + df_android['渠道'].astype('str') + df_android['国家'].astype('str')
    df_ios['匹配列'] = df_ios['平台'].astype('str') + df_ios['渠道'].astype('str')+ df_ios['国家'].astype('str')

    # ios其他渠道处理
    df_ios['匹配列'] = df_ios.apply(
        lambda x: '其他' if 'Applovin' not in x['匹配列'] and 'Unity' not in x['匹配列'] else x['匹配列'], axis=1)

    # df_ios['花费']=df_ios['花费'].astype('float')
    df_ios = df_ios.groupby(by=['os', '匹配列'], as_index=False)['花费'].sum()
    # 匹配前删除无用数据
    android_user = android_user.query('渠道!="Organic"')
    android_user = android_user[['distinct_id', 'install_time','匹配列']]
    ios_user = ios_user[['distinct_id', 'install_time','匹配列']]
    # 匹配渠道成本
    res_android = android_user.merge(df_android[['os', '匹配列', '花费']], how='left', on='匹配列')
    res_ios = ios_user.merge(df_ios[['os', '匹配列', '花费']], how='left', on='匹配列')
    res = pd.concat((res_android, res_ios), axis=0)

    # 计算cpi
    res['安装'] = res.groupby(by=['os', '匹配列'], as_index=False)['distinct_id'].transform('nunique')

    res.dropna(axis=0,  # 删除的轴方向
               inplace=True,  # 是否替换原数据
               how="any"  # 删除方法 all | any
               )
    res['cpi'] = res['花费'] / res['安装']
    res['#account_id']=''
    res['#distinct_id'] = res['distinct_id']
    return res



if __name__ == '__main__':
    # 设置读取时间，默认是昨日
    res_event = pd.DataFrame()
    res_user = pd.DataFrame()
    for i in range(0,2):#设置往前的天数
        day = i
        nowtime = (datetime.date.today() - datetime.timedelta(days=day)).strftime('%Y-%m-%d')
        date2 = (datetime.date.today() - datetime.timedelta(days=day + 2)).strftime('%Y-%m-%d')
        date1 = (datetime.date.today() - datetime.timedelta(days=day + 1)).strftime('%Y-%m-%d')
        # 查询数据测试
        sql = f"""
        select * from (select *,count(data_map_0) over () group_num_0 from (select group_0,group_1,group_2,group_3,group_4,group_5,map_agg("$__Date_Time", amount_0) filter (where amount_0 is not null and is_finite(amount_0) ) data_map_0,sum(amount_0) filter (where is_finite(amount_0) and ("$__Date_Time" <> timestamp '1981-01-01')) total_amount from (select *, internal_amount_0 amount_0 from (select group_0,group_1,group_2,group_3,group_4,group_5,"$__Date_Time",cast(coalesce(COUNT(DISTINCT ta_ev."#user_id"), 0) as double) internal_amount_0 from (select *, "#os" group_1,"#country_code" group_2,ta_date_trunc('day',"@vpc_tz_#event_time", 1) "$__Date_Time" from (SELECT * from (select *, if("#zone_offset" is not null and "#zone_offset">=-12 and "#zone_offset"<=14, date_add('second', cast((0-"#zone_offset")*3600 as integer), "#event_time"), "#event_time") "@vpc_tz_#event_time" from (select "#event_name","#country_code","#event_time","#zone_offset","#user_id","#os","$part_date","$part_event" from v_event_33)))) ta_ev inner join (select *, "#distinct_id" group_0,"#adjust_network_name" group_3,"#adjust_campaign_name" group_4,"adjust_campaign_name" group_5 from (select a.*,if(coalesce("@vpc_cluster_cohort_20240207_212115",false),'属于 国区用户','不属于 国区用户') "@vpc_cluster_cohort_20240207_212115" from (select * from (select "#adjust_network_name","#update_time","adjust_campaign_name","#event_date","#user_id","#distinct_id","#adjust_campaign_name" from v_user_33) where "#event_date" > 20230213) a left join (select "#user_id" "#user_id",true "@vpc_cluster_cohort_20240207_212115" from user_result_cluster_33 where cluster_name = 'cohort_20240207_212115') b0 on a."#user_id"=b0."#user_id")) ta_u on ta_ev."#user_id" = ta_u."#user_id" where (( ( "$part_event" IN ( 'ta_app_install' ) ) )) and ((("$part_date" between '{date2}' and '{nowtime}') and ("@vpc_tz_#event_time" >= timestamp '{date1}' and "@vpc_tz_#event_time" < date_add('day', 1, TIMESTAMP '{date1}'))) and (ta_u."@vpc_cluster_cohort_20240207_212115"='不属于 国区用户')) group by group_0,group_1,group_2,group_3,group_4,group_5,"$__Date_Time")) group by group_0,group_1,group_2,group_3,group_4,group_5)) ORDER BY total_amount DESC
        """
        res=CPI()

        df1=res[['#account_id', '#distinct_id', 'cpi']]
        df2=res[['#account_id', '#distinct_id', 'install_time', 'cpi']]
        if res_user.shape[0]==0:
            res_user=df1
        else:
            res_user = pd.concat((res_user,df1))
        if res_event.shape[0]==0:
            res_event=df2
        else:
            res_event = pd.concat((res_event,df2))
        print(f'第{i}次完成-------------------')
    res_user.to_csv(f'D:\milimili\cpidata\用户_cpi_{date1}.csv',index=False)
    res_event.to_csv(f'D:\milimili\cpidata\事件_cpi-{date1}.csv', index=False)
    pass



