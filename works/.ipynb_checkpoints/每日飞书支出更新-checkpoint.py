import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()
from configs import mili_mysql
from tools.mysqlhandle import Mysql_handle
mysql=Mysql_handle(mili_mysql,'data_analysis')
import pandas as pd
import datetime
date = (datetime.date.today() - datetime.timedelta(days=3)).strftime('%Y-%m-%d')
# wangzuan_tab_path='NPPTsN1HmhVigrtKj6Uc95Eyn8M'
# range_list='A1:F2000'
# app_list={
#     'Daub Farm': ['42oitZ','dU1o9F'],
#     'Bingo Stars': ['0AF8V4'],
#     'Keno Farm': ['DMLuws'],
#     'Bingo Party': ['IGxdjS'],
# }
# spend_df=pd.DataFrame()
# for appname,spend_sheet in app_list.items():
#     for sheetname in spend_sheet:
#         df=feishu.read_tab(wangzuan_tab_path,sheetname,range_list)
#         df=df.query(f'日期>="{date}"')
#         df['appname']=appname
#         if spend_df.shape[0]==0:
#             spend_df=df
#         else:
#             spend_df=pd.concat((spend_df,df))
# spend_columns=['日期','appname','地区','平台','安装',
#                '花费']
# res_wz=spend_df[spend_columns]
# res_wz.columns=['date','appname','country_code','channel','installs',
#                'cost']
# cp产品单独处理
cp_tab_path='XVYDsydUFhpNOPtRJurcPgCCnyf'
cp_list={
    'Doomsday Vanguard':['XVYDsydUFhpNOPtRJurcPgCCnyf','fcd655'],
    'Doomsday Vanguard ios':['XVYDsydUFhpNOPtRJurcPgCCnyf','Scf3Q5'],
    'Savior: The Stickman':['VPwQsPchdhDPaLtwyYNc564FnWd','fcd655']
}
cp_df=pd.DataFrame()
for appname,spend_sheet in cp_list.items():
    cp_tab_path=spend_sheet[0]
    sheetname=spend_sheet[1]
    # for sheetname in spend_sheet:
    df=feishu.read_tab(cp_tab_path,sheetname,'A1:H3000')
    df=df.query(f'日期>="{date}"')
    df['appname']=appname
    if cp_df.shape[0]==0:
        cp_df=df
    else:
        cp_df=pd.concat((cp_df,df))
print(cp_df.columns)
cp_df.columns=['date','发布平台','平台','代理','country_code','优化','预算','花费','appname']
cp_df['installs']=0
cp_df['channel']=cp_df['平台']+ '-' +cp_df['代理']
# cp_df.columns=['date','发布平台','平台','代理','country_code','优化','预算','花费','CPI','安装','appname','channel']
cp_df=cp_df.groupby(['date','country_code','appname','channel'])[['installs','花费']].agg(
    {
        'installs':'sum',
        '花费':'sum'
    }
).reset_index()
# print(cp_df)
# 获取新增数据
# sql=f"""
#     select date,app as appname,country_code,sum(installs) as installs
#     from wangzhuan_adjust_installs
#     where date ='{date}'
#     and app ='Doomsday Vanguard'
#     and network !='Organic'
#     group by date,app,country_code
# """
# cp_install=mysql.select_data(sql)
# cp_install['date']=cp_install['date'].astype('str')
# print(cp_install)
# cp_df=cp_df.merge(cp_install,how='left',on=['date','appname','country_code'])
# cp_df['channel']='GGFB'
cp_columns=['date','appname','country_code','channel','installs',
               '花费']
res_cp=cp_df[cp_columns]
res_cp.columns=['date','appname','country_code','channel','installs',
               'cost']

# print(res_cp[['push_country','TotalCost','install']])
# 入库
# mysql.insert_data(res_cp,'wangzhuan_spend_new')
# 转换DataFrame为JSON格式并保存到数据库
# res=pd.concat((res_wz,res_cp))
res=res_cp
tt = res.to_json(orient='records')  # 将数据框转换为 JSON 格式
table = "wangzhuan_spend_new"  # 数据库表名
mysql.savedata(table, tt)  # 保存数据到数据库
