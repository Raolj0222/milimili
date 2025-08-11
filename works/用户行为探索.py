import datetime
import sys
import numpy as np
import pandas as pd
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import TE_app_token
from tools.数数查询 import get_sqldata
token=TE_app_token['Doomsday Vanguard']

# 用户首次登录时间
def get_first_login():
    sql=f"""
    SELECT "#user_id","#event_time","#os","#country_code","is_first_login"
    FROM v_event_33 
    WHERE "$part_event"='login' 
    and "is_first_login" = 'True' 
    and "#country_code" != 'CN'
    AND "$part_date" between '2023-08-01' and '2034-01-09'
    """
    df = get_sqldata(token,sql)
    return df

# 用户事件事件
def get_event_time():
    df_event=pd.DataFrame()
    for i in range(72,0,-5):
        time1=(datetime.date.today() - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
        time2=(datetime.date.today() - datetime.timedelta(days=i-5)).strftime('%Y-%m-%d')
        sql=f"""
        select "#user_id","#event_time","$part_event","#os","#country_code"
        from ta.v_event_33 
        where "$part_event" in ('guide_completed','Game_Level_enter','DailyChallenge_level_finish','equipment','talent','In_appPurchases_BuySuccess','draw_card','role_recruit','pet','chip')
        and "#country_code" != 'CN'
        and "$part_date" between '{time1}' and '{time2}'
        """
        df =get_sqldata(token,sql)
        if df_event.shape[0]==0:
            df_event=df
        else:
            df_event=pd.concat((df_event,df))
        print('-----------------完成一次')
    df_event.to_csv('用户事件.csv')

# get_first_login()
get_event_time()

