import pandas as pd
import datetime
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
feishu=FeiShu()
now_time=datetime.date.today().strftime('%Y-%m-%d')
# 读取飞书表格
df=feishu.read_tab('YpOGsuJVQhiNeetBk8dctY5tnKe','d4a4c9','A1:G10000')
df=df.query(f'日期=="{now_time}"')[['日期','产品名','国家','提现通过金额']]
df.columns=['date','appname','country','payout']
print(df)
# 入库
mysql.insert_data(df,'cost_payout')