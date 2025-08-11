import pymysql as ps
import pandas as pd
from configs import mili_mysql
from sqlalchemy import create_engine
import requests


class Mysql_handle():
    def __init__(self,info,database):
        self.user=info['user'],
        self.pwd=info['pwd']
        self.url=info['url']
        self.database=database
    def sql_conn(self):
        conn = create_engine(f"mysql+pymysql://root:hNJA%40N1FAlNas@{self.url}:3306/{self.database}?charset=utf8mb4")
        return conn
    def select_data(self,sql):
        conn=self.sql_conn()
        df =pd.read_sql(sql,conn)
        return df
    def insert_data(self,df,table):
        # 插入方式为append，当数据库中存在表时，在表中添加数据，当不存在表时，新建数据表后添加
        conn=self.sql_conn()
        df.to_sql(name=table,con=conn,if_exists='append',index=False,index_label=False)
        print('插入成功')

    def savedata(self,table, savedata_list):
        values = savedata_list
        url_update = 'http://150.230.206.248:5000/update'
        payload = {'table': table, 'values': values}
        headers = {}
        response = requests.request("post", url=url_update, headers=headers, data=payload)
        print(response)
# 测试
if __name__ == '__main__':
    mysql = Mysql_handle(mili_mysql, 'data_analysis')
    sql='''
    select date,appname,country,
    sum(if(live_day is NOT NULL and live_day like '0%',money,0)) as '新用户收入',
    sum(if(live_day is NOT NULL and live_day  not like '0%',money,0)) as '老用户收入'
    from sr_neigou
    where date between '2024-01-04' and '2024-01-10'
    and	country <> 'all'
    group by date,appname,country
    union all
    select date,appname,'all' as country,
    sum(if(live_day is NOT NULL and live_day like '0%',money,0)) as '新用户收入',
    sum(if(live_day is NOT NULL and live_day  not like '0%',money,0)) as '老用户收入'
    from sr_neigou
    where date between '2024-01-04' and '2024-01-10'
    and	country <> 'all'
    group by date,appname
    '''
    df=mysql.select_data(sql)
    print(df)