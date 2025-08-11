from tools.mysqlhandle import Mysql_handle
from configs import mili_mysql
mysql=Mysql_handle(mili_mysql,'data_analysis')
# 判断是否首次购买

sql="""
select * from sr_neigou where date >='2022-08-26'
"""
df=mysql.select_data(sql)
df=df.query('appname=="Doomsday Vanguard"')
df['money']=df['money'].astype('float')
df_gp=df.groupby(by=['date','appname','country','email_code'])['money'].sum().reset_index()
df_gp1=df.groupby(by=['appname','country','email_code'])['date'].min().reset_index()[['email_code','date']]
df_gp1.columns=['email_code','first_ng_date']
res=df_gp.merge(df_gp1,how='left',on='email_code')
res['first_ng']=res.apply(lambda x: 1 if x['date']==x['first_ng_date'] else 0,axis=1)
df_ng=res[['date','appname','country','money','first_ng','email_code']]
# print(df_ng.head())
df_ng['money']=df_ng['money'].astype('float')
mysql.insert_data(df_ng,'sr_neigou_first')

