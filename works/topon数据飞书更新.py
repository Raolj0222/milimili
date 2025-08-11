import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
from tools.topon数据 import get_group_data,get_detail_data
import datetime
import numpy as np
day1=(datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d') # 获取昨天的日期

def insert_group_data():
    # 读取飞书位置表格，明确写入位置
    tab_path = 'DPpOsWNQ9h4xOitKjDicrWwBn3h'
    sheet_path = '2gMdQj'
    range_list = 'A:A'
    df = feishu.read_tab(tab_path, sheet_path, range_list)
    if df.shape[0] <= 1:
        new_range = 2
    else:
        new_range = list(df.query(f'date=="{day1}"').index)[0] + 1

    # 写入数据
    topon_df = get_group_data()
    topon_df.fillna(value='-', inplace=True)
    values=np.array(topon_df).tolist()
    data_len = new_range + len(values) - 1
    feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:U{data_len}', values)




def insert_datail_data():
    #读取飞书位置表格，明确写入位置
    tab_path='DPpOsWNQ9h4xOitKjDicrWwBn3h'
    sheet_path='uJCUcN'
    range_list='A:A'
    df=feishu.read_tab(tab_path,sheet_path,range_list)
    if df.shape[0]<=1:
        new_range=2
    else:
        new_range=list(df.query(f'date=="{day1}"').index)[0] +1

    # 写入数据
    topon_df=get_detail_data()
    topon_df.fillna(value='-',inplace=True)
    topon_df = topon_df.query('广告源请求>0')
    topon_df.to_excel('topon广告源数据.xlsx')
    values = np.array(topon_df).tolist()
    data_len = new_range + len(values) - 1
    feishu.insert_tab(tab_path,sheet_path,f'A{new_range}:P{data_len}',values)
insert_datail_data()
insert_group_data()
