import pandas as pd
import re
df =pd.read_csv('20231020_094736_32833_fmhnu.csv')
df.fillna(value=0,inplace=True)
def func(x):
    rs=str(x['reward'])
    if re.match(r'(金币:undefined|金币:0)',rs):
        return '绿币'
    else :
        return '金币'
df['通用奖励类型']=df.apply(lambda x:func(x),axis=1)

def func2(x):
    rs = str(x['reward'])
    if re.match(r'(金币:undefined|金币:0)',rs):
        s=re.sub(r'金币(.*),绿币:','',rs)
    else :
        s1=re.sub(r',绿币(.*)','',rs)
        s=re.sub(r'金币:','',s1)
    return eval(s)
df['通用奖励数量']=df.apply(lambda x:func2(x),axis=1)
print(df.shape)

res=df.groupby(by=['ad_name','通用奖励翻倍原因','通用奖励类型'])[['金币余额','绿币余额','累计游戏场次','通用奖励数量','用户id']].agg({
'金币余额':'mean',
'绿币余额':'mean',
'累计游戏场次':'mean',
'通用奖励数量':'mean',
'用户id':'count'
}).reset_index()
res.to_excel('初次观看广告数据1.xlsx',index=False)

