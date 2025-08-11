import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
from works.adjust_系列支出 import  get_cost
res=get_cost(3)
# res.drop(columns=['平台'],inplace = True)
print(res.head())
tt = res.to_json(orient='records')  # 将数据框转换为 JSON 格式
table = "adjust_campaign_cost"  # 数据库表名
mysql.savedata(table, tt)  # 保存数据到数据库