import pandas as pd
import numpy as np


# 循环读取 并且排除无用的事件 page_enter,achievement_unlock,Game_expLevel_UP

res=pd.DataFrame()
rows = 100000
for chunk in pd.read_csv('../临时文件/8月勇者数据.csv',
                         index_col=False,
                         names=['id','事件名','时间','属性'],
                         chunksize=rows,
                         on_bad_lines='skip',
                         engine='python'):
    print(f'-----处理完成{rows}行数据-----')
    # 去除无用数据
    chunk = chunk[~chunk['事件名'].isin(['page_enter','achievement_unlock','Game_expLevel_UP'])]
    if res.shape[0] == 0:
        res=chunk
    else:
        res= pd.concat((res,chunk))

print(res.head())
