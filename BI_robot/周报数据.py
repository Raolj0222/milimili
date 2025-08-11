import pandas as pd
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from tools.mysqlhandle import Mysql_handle
import datetime
import openpyxl
from configs import mili_mysql
mysql = Mysql_handle(mili_mysql,'data_analysis')
from get_img import img_get
from wangzuan import app_info
from feishu import FeiShu,web_hook_test,web_hook,web_hook_new
feishu = FeiShu()
# 设置开始时间和结束时间
end = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
# end ='2024-12-12'
start = (datetime.date.today() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
# start = '2024-12-31'

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
                'averageTime_PerDevice','广告收入','广告ROI','广告ARPU','激励人均展示','插页人均展示','banner人均展示',
                '激励收入','插页收入','banner收入','激励ecpm','插页ecpm','bannerecpm',
                'ng','ng_users','付费ROI','付费率','付费arpu','新用户收入','老用户收入',
                '小R用户收入','中R用户收入','大R用户收入','积分墙收入','积分墙ROI']



cp_app_list={
    'Doomsday Vanguard': ['all'],
    'Doomsday Vanguard ios': ['all'],
    'Tales of Brave':['all'],
    'Tales of Brave ios':['all']
    # 'Bingo Party': ['US'],
    # 'Keno Farm B':['BR']
}


cp_file= r'D:\milimili\BI_robot\cp周报源数据.xlsx'
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
    df.to_excel(r'D:\milimili\BI_robot\cp周报源数据.xlsx',index=False)
    # column_num = 0
    # for column in cp_list:
    #     column_num = 1 + column_num
    #     wb = openpyxl.load_workbook(cp_file)
    #     ws = wb['sheet1']
    #     values = res[column].tolist()
    #     for i in range(0, len(values)):
    #         value = values[i]
    #         # 从表格的第三行开始写入
    #         ws.cell(row=i + 3, column=column_num).value = value
    #     wb.save(cp_file)
    #     wb.close()
            # 每一个国家写入完成后开始截图
            # 设置文件名
            # p_name = 'cpimg'
            # print(p_name)
            # img_get(cp_file, 'sheet1', p_name)
            # # 截图完成开始推送
            # # 设置富文本标题：
            # roi = round(res[res['date'] == end]['总ROI'].tolist()[0]* 100, 1)
            # maoli = round(res[res['date'] == end]['毛利'].tolist()[0], 2)
            # title = f'{app} - {country_code}：昨日总ROI: {roi}%,毛利: {maoli}'
            # print(title)
            # # 发送至飞书群
            # feishu.run(title, 'D:\milimili\BI_robot\截图-cp报表\cpimg.png', web_hook_new)

if __name__ == '__main__':
    cp_weekly_data()
    pass





