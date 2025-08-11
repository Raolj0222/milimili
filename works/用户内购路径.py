import pandas as pd
import numpy as np



data =pd.read_csv('..\临时文件\用户付费时间7-31-8-15.csv',index_col=False)

data = data[['用户App登录ID','首次访问时间','事件发生时间','googleShopID']]

# 根据内购时间进行排序
data['rank'] = data.groupby(by=['用户App登录ID'])['事件发生时间'].rank(ascending=True,method='dense')
# data.to_excel('..\临时文件\付费顺序.xlsx',index=False)
print(data.head())


#区分用户 8-10日之前和之后
data_30_10 = data[(data['首次访问时间']>'2024-07-30')&(data['首次访问时间']<'2024-08-10')]
data_10_ = data[data['首次访问时间']>'2024-08-10']
# 根据首次购买礼包对用户进行分类
def ng_shop(df):
    df_1 =df.query('rank==1')[['用户App登录ID','googleShopID']]
    df_1.columns=['用户App登录ID','首次购买']
    df_2 = df.query('rank==2')[['用户App登录ID','googleShopID']]
    df_2.columns = ['用户App登录ID', '2次购买']
    df_3 = df.query('rank==3')[['用户App登录ID','googleShopID']]
    df_3.columns = ['用户App登录ID', '3次购买']
    # df_4 = df.query('rank==4')
    # df_4.columns = ['用户唯一id', '4次购买']
    # df_5 = df.query('rank==5')
    # df_5.columns = ['用户唯一id', '5次购买']

    res = df_1.merge(df_2,how='left',on=['用户App登录ID']).merge(df_3,how='left',on=['用户App登录ID'])

    # 计算首次 --- 二次 --- 三次购买的路径人数
    res1 = res.groupby(by='首次购买',as_index = False)['用户App登录ID'].nunique()
    res1.columns =['首次购买','首次购买人数']
    res2 = res.groupby(by=['首次购买','2次购买'],as_index = False)['用户App登录ID'].nunique()
    res2.columns = ['首次购买','2次购买', '2次购买人数']
    res3 = res.groupby(by=['首次购买','2次购买','3次购买'],as_index = False)['用户App登录ID'].nunique()
    res3.columns = ['首次购买','2次购买','3次购买', '3次购买人数']

    res_all = res1.merge(res2,how='left',on=['首次购买']).merge(res3,how='left',on=['首次购买','2次购买'])
    return res_all



ng_list =[data_30_10,data_10_]
n = '7-30'
for i in ng_list:
    res=ng_shop(i)
    res.to_excel(f'..\临时文件\用户内购路径_{n}.xlsx',index=False)
    n='8-10'











