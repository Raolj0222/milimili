import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from adjust爬取 import get_installs
from tools.mysqlhandle import Mysql_handle
import datetime
import pandas  as pd
from configs import mili_mysql
## 脚本需在上午10点执行
## 获取数据2
date = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
app_token_list={
    # 'Daub Farm':['g4mozkbn05c0',['US','BR','PH']],
    'Doomsday Vanguard':['3dle7bbjgwhs',['TW','KR','JP','MO','HK']],
    'Doomsday Vanguard ios':['h40ytipbjkzk',['US','JP','TW']],
    'Tales of Brave':['ex9bq7r3kcg0']
    # 'Bingo Stars':['lv2y844vjo5c',['US']],
    # 'Keno Farm':['3ycwnrahi1ds',['US','BR']],
    # 'Bingo Party':['1bqgxy43lvs0',['US']]
}
df =pd.DataFrame()
for app_neme,app_token in app_token_list.items():
    re_df=get_installs(app_token[0],date)
    # re_df=re_df[re_df['country_code'].isin(app_token[1])]
    print(df.head())
    re_df=re_df[['date','app','country_code','network','installs','daus','averageTime_PerDevice']]
    if df.shape[0]==0:
        df=re_df
    else:
        df = pd.concat((df,re_df))

## 写入数据库
if __name__ == '__main__':
    Mysql=Mysql_handle(mili_mysql,'data_analysis')
    Mysql.insert_data(df,'wangzhuan_adjust_installs')
    pass





