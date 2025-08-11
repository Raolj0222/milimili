import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
from tools.max数据 import get_group_data
import datetime
import numpy as np
day1=(datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y-%m-%d') # 获取昨天的日期


def insert_group_data():
    # 读取飞书位置表格，明确写入位置
    tab_path = 'DPpOsWNQ9h4xOitKjDicrWwBn3h'
    sheet_path = 'uqxbob'
    range_list = 'A:A'
    df = feishu.read_tab(tab_path, sheet_path, range_list)
    if df.shape[0] <= 1:
        new_range = 2
    else:
        new_range = list(df.query(f'date=="{day1}"').index)[0] + 1

    # 写入数据
    max_df = get_group_data()
    max_df.fillna(value='-', inplace=True)
    values = np.array(max_df).tolist()
    data_len = new_range + len(values) -1
    feishu.insert_tab(tab_path, sheet_path, f'A{new_range}:K{data_len}', values)

# 汇总数据写入
insert_group_data()

