import pandas as pd
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from tools.mysqlhandle import Mysql_handle
import datetime
import time
import openpyxl
from configs import mili_mysql
mysql = Mysql_handle(mili_mysql,'data_analysis')
from get_img import img_get
from wangzuan import app_info
from feishu import FeiShu,web_hook_test,web_hook,web_hook_new
feishu = FeiShu()
# 设置开始时间和结束时间
# end = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
end = (datetime.date(2025,3,17)).strftime('%Y-%m-%d')
# start = (datetime.date.today() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')
start = (datetime.date(2024,12,12)).strftime('%Y-%m-%d')

print(end,start)
wangzhuan_df,cp_df=app_info(start,end)
# 筛选出投放数据
# wangzhuan_df=wangzhuan_df[wangzhuan_df['money']==wangzhuan_df['money']]
# cp_df=cp_df[cp_df['money']==cp_df['money']]
wangzhuan_df.fillna(value=0,inplace=True)
cp_df.fillna(value=0,inplace=True)
# 处理需要计算的列

# cp产品
cp_list=['date','appname','country','总支出','总收入','总ROI','毛利','总体ARPU',
                '新增成本','pushinstall','money','推广CPI','install','daus','新增活跃比','自然新增',
                'averageTime_PerDevice','广告收入','广告ROI','广告ARPU','激励人均展示',
                '激励收入','激励ecpm','ng','ng_users','付费ROI','付费率','付费arpu']

cp_app_list={
    # 'Doomsday Vanguard': ['all'],
    # 'Doomsday Vanguard ios': ['all'],
    # 'Tales of Brave':['all'],
    'Tales of Brave ios':['all']
    # 'Bingo Party': ['US'],
    # 'Keno Farm B':['BR']
}


# cp_file= r'D:\milimili\BI_robot\cp周报源数据.xlsx'
def cp_weekly_data():
    cp_df['总支出'] = cp_df['money']
    cp_df['广告收入'] = cp_df['激励收入'] + cp_df['插页收入']
    cp_df['广告ROI'] = cp_df['广告收入'] / cp_df['总支出']
    cp_df['广告ARPU'] = cp_df['广告收入'] / cp_df['daus']
    cp_df['激励人均展示'] = cp_df['激励展示'] / cp_df['daus']
    cp_df['插页人均展示'] = cp_df['插页展示'] / cp_df['daus']
    cp_df['banner人均展示'] = cp_df['banner展示'] / cp_df['daus']
    cp_df['激励ecpm'] = cp_df['激励收入'] / cp_df['激励展示'] * 1000
    cp_df['插页ecpm'] = cp_df['插页收入'] / cp_df['插页展示'] * 1000
    cp_df['bannerecpm'] = cp_df['banner收入'] / cp_df['banner展示'] * 1000
    cp_df['总收入'] = cp_df['广告收入'] + cp_df['ng'] + cp_df['积分墙收入']
    cp_df['总ROI'] = cp_df['总收入'] / cp_df['总支出']
    cp_df['毛利'] = cp_df['总收入'] - cp_df['总支出']
    cp_df['总体ARPU'] = cp_df['总收入'] / cp_df['daus']
    cp_df['新增成本'] = cp_df['money'] / cp_df['install']
    cp_df['推广CPI'] = cp_df['money'] / cp_df['pushinstall']
    cp_df['新增活跃比'] = cp_df['daus'] / cp_df['install']
    cp_df['自然新增'] = cp_df['install'] - cp_df['pushinstall']
    cp_df['付费ROI'] = cp_df['ng'] / cp_df['总支出']
    cp_df['付费率'] = cp_df['ng_users'] / cp_df['daus']
    cp_df['付费arpu']=cp_df['ng'] / cp_df['daus']
    cp_df['积分墙ROI'] = cp_df['积分墙收入'] / cp_df['总支出']
    # print(cp_df.query('appname=="Keno Farm B"&country=="BR"')[['积分墙收入','积分墙ROI']])
    res = pd.DataFrame()

    for app, country in cp_app_list.items():
        # 筛选app和地区
        for country_code in country:
            df = cp_df[(cp_df['appname'] == f'{app}') & (cp_df['country'] == f'{country_code}')]
            df.sort_values(by='date',ascending=True,inplace=True)
            if res.shape[0]==0:
                res=df
            else:
                res=pd.concat((res,df))
    print(res)

    # 开始写入,按照表格顺序依次写入
    df=res[cp_list]
    df.to_excel(r'D:\milimili\BI_robot\勇者月报源数据.xlsx',index=False)


if __name__ == '__main__':
    cp_weekly_data()
    pass





