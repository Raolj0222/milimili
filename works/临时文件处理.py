import pandas  as pd
import datetime
import numpy as np
df=pd.read_csv('D:\milimili\临时文件\台湾调价后付费数据.csv')
print(df.columns)
# 计算第几次购买

df['购买次数']=df.groupby(by=['#user_id','#country_code'])['#event_time'].rank(ascending=True,method='dense')

# 处理谷歌id
df['ng_name']=df['googleshopid'].str.replace('vanguard_','')
res=df.groupby(by=['#country_code','购买次数','ng_name'])['#user_id'].nunique().reset_index()
print(res)
# 计算内购时间与设备新增时间差
# df['ng_time']=pd.to_datetime(df['ng_time'])
# df['insert_time']=pd.to_datetime(df['insert_time'])
# df['day_diff']=df['ng_time'].dt.day - df['insert_time'].dt.day
#
# day_list=[i for  i in range(0,15)]
#
# for  day in day_list:
#     df[str(day)]=df.apply(lambda x:x['id'] if x['day_diff']<=day else np.nan,axis=1)
#
#
# res=df.groupby(by=['insert_time'])[[str(i) for  i in range(0,15)]].agg({
#     str(i):'nunique' for  i in range(0,15)
# }).reset_index()


res.to_excel('台湾地区购买礼包分布.xlsx')

# print(res)