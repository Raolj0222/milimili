import pandas as pd
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from tools.mysqlhandle import Mysql_handle
import datetime
import openpyxl
from picture_get import gen
from configs import mili_mysql,app_sql,cost_sql,spend_sql,insert_sql,On_admob_sql,On_admob_cp_sql,adjoe_sql,okspin_sql,ng_sql,cp_adjoe_sql,ng_sql_1,ng_sql_2,ng_user_active_sql
mysql = Mysql_handle(mili_mysql,'data_analysis')
from get_img import img_get

from feishu import FeiShu,web_hook
feishu = FeiShu()
# 设置开始时间和结束时间
end = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
start = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')

## 获取app 地区
# DAUB US
def app_info(start,end):
    # 获取app、投放地区信息
    app=mysql.select_data(app_sql.format(start,end))
    app['date'] = app['date'].astype('str')
    # 获取支出信息
    # 提现支出
    app_cost=mysql.select_data(cost_sql.format(start,end))
    app_cost['date']=app_cost['date'].astype('str')

    # 2024-10-09 新增字段：付费活跃人数
    ng_user_active = mysql.select_data(ng_user_active_sql.format(start,end))
    ng_user_active['date']=ng_user_active['date'].astype('str')

    # 推广支出
    spend_cost=mysql.select_data(spend_sql.format(start,end))
    # 防止小数点问题，继续去重
    spend_cost.drop_duplicates(subset=['date','appname','country'],keep='first',inplace=True)
    spend_cost['date'] = spend_cost['date'].astype('str')
    # print(spend_cost.query('appname=="Daub Farm"'))
    # 新增用户
    install=mysql.select_data(insert_sql.format(start,end))
    install['date'] = install['date'].astype('str')
    # 广告收入
    On_admob=mysql.select_data(On_admob_sql.format(start,end))
    On_admob['date'] = On_admob['date'].astype('str')
    # 广告收入 cp
    On_admob_cp=mysql.select_data(On_admob_cp_sql.format(start,end))
    On_admob_cp['date']=On_admob_cp['date'].astype('str')
    # 替换滋滋 显示名称
    # On_admob_cp['appname']=On_admob_cp['appname'].str.replace('滋滋先遣队','Doomsday Vanguard')
    # 积分墙收入
    adjoe=mysql.select_data(adjoe_sql.format(start,end))
    adjoe['date'] = adjoe['date'].astype('str')
    # okspin 收入
    okspin=mysql.select_data(okspin_sql.format(start,end))
    okspin['date'] = okspin['date'].astype('str')
    # print(okspin)
    # 内购收入
    ng=mysql.select_data(ng_sql.format(start,end))
    ng['date']=ng['date'].astype('str')

    # 新老用户内购收入
    # print(ng_sql_1.format(start,end))
    ng_1 = mysql.select_data(ng_sql_1.format(start,end))
    ng_1['date']=ng_1['date'].astype('str')

    # 大中小R收入
    ng_2 = mysql.select_data(ng_sql_2.format(start, end))
    ng_2['date'] = ng_2['date'].astype('str')

    # cp产品积分墙
    cp_adjoe=mysql.select_data(cp_adjoe_sql.format(start,end))
    cp_adjoe['date']=cp_adjoe['date'].astype('str')



    # 组合表格
    df=app.merge(app_cost,how='left',
                on=['date','appname','country'])
    df1=df.merge(spend_cost,how='left',on=['date','appname','country'])
    df2=df1.merge(install,how='left',on=['date','appname','country'])
    df3=df2.merge(On_admob,how='left',on=['date','appname','country'])
    df4=df3.merge(adjoe,how='left',on=['date','adjoe','country'])
    wangzhuan=df4.merge(okspin,how='left',on=['date','okspin','country'])
    cp=app.merge(install,how='left',on=['date','appname','country'])
    cp=cp.merge(On_admob_cp,how='left',on=['date','appname','country'])
    cp=cp.merge(spend_cost,how='left',on=['date','appname','country'])
    cp=cp.merge(ng,how='left',on=['date','appname','country'])
    cp=cp.merge(cp_adjoe,how='left',on=['date','appname','country'])
    cp = cp.merge(ng_1, how='left', on=['date', 'appname', 'country'])
    cp=cp.merge(ng_2, how='left', on=['date', 'appname', 'country'])
    cp = cp.merge(ng_user_active,how='left', on=['date', 'appname', 'country'])
    res1=wangzhuan[
        ['date', 'appname', 'country', 'cost_money', 'money', 'pushinstall', 'install', 'daus', 'averageTime_PerDevice',
         '激励展示', '激励收入','插页展示', '插页收入', 'banner展示', 'banner收入', 'adjoe收入', 'adjoe_ecpm', 'okspin收入',
         'okspin_ecpm']]
    res2=cp[['date', 'appname', 'country',  'money', 'pushinstall', 'install', 'daus','users', 'averageTime_PerDevice',
         '激励展示', '激励收入','插页展示', '插页收入', 'banner展示', 'banner收入','ng','ng_users','积分墙收入','新用户收入','老用户收入','小R用户收入','中R用户收入','大R用户收入']]
    return res1,res2



# wangzhuan_df,cp_df=app_info(start,end)
# print(cp_df.query('appname=="Doomsday Vanguard ios"'))




