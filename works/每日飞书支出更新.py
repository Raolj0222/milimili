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
date = (datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')
# cp产品单独处理
cp_tab_path='XVYDsydUFhpNOPtRJurcPgCCnyf'
cp_list={
    'Doomsday Vanguard':['XVYDsydUFhpNOPtRJurcPgCCnyf','fcd655'],
    'Doomsday Vanguard ios':['XVYDsydUFhpNOPtRJurcPgCCnyf','Scf3Q5'],
    # 'Savior: The Stickman':['VPwQsPchdhDPaLtwyYNc564FnWd','fcd655'],
    'Tales of Brave':['R1F0sluEBhHbhqtXJ5BcCiJZnnf','f9bc87'],
    'Tales of Brave ios':['R1F0sluEBhHbhqtXJ5BcCiJZnnf','kPKx6i']
}
cp_df=pd.DataFrame()
for appname,spend_sheet in cp_list.items():
    cp_tab_path=spend_sheet[0]
    sheetname=spend_sheet[1]
    # for sheetname in spend_sheet:
    df=feishu.read_tab(cp_tab_path,sheetname,'A1:G10000')
    print(df.columns)
    df.columns=['日期', '渠道', '代理', '系统', '国家', '买量方式', '花费']
    df=df.query(f'日期>="{date}"')
    df=df.query('日期!="汇总"')
    df['appname']=appname
    print(appname)
    if cp_df.shape[0]==0:
        cp_df=df
    else:
        cp_df=pd.concat((cp_df,df))
print(cp_df.columns)
cp_df.columns=['date','平台','代理','系统','country_code','优化','花费','appname']
cp_df['installs']=0
cp_df['country_code']=cp_df['country_code'].str.replace('1.0','')
cp_df['country_code']=cp_df['country_code'].apply(lambda x:'JP' if x in ['JP-img','JP-tv','JP-Img','JP-TV'] else x)
cp_df['channel']=cp_df['平台']+ '-' +cp_df['代理']
cp_df.to_excel('消耗数据.xlsx')
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
res_cp['cost_all'] = res_cp['cost']

res=res_cp
# res.to_csv('买量消耗.csv')
tt = res.to_json(orient='records')  # 将数据框转换为 JSON 格式
table = "wangzhuan_spend_new"  # 数据库表名
mysql.savedata(table, tt)  # 保存数据到数据库
