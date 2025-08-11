import requests
import datetime
import json
import pandas as pd
import io
import time

# 读取数据库信息，并进行缓存处理
def get_info(sql):
    # 定义缓存键名
    cache_key = f'cache_{sql}'
    # 先尝试从缓存中获取数据
    if cache_key in globals():
        return globals()[cache_key]
    else:
        url_sql="http://150.230.221.78:5000?sql="+sql
        response_sql = requests.request("get", url=url_sql)
        sql_datalist=json.loads(response_sql.text)
        # 将获取到的数据进行缓存
        globals()[cache_key] = sql_datalist
        return sql_datalist

# 保存数据，并使用 session 来管理连接
def savedata(table, savedata_list):
    url_update='http://150.230.221.78:5000/update'
    payload={'table': table,
    'values': savedata_list}
    headers = {}
    with requests.Session() as s:
        response = s.post(url=url_update, headers=headers, data=payload)
    print(response.text)

headers = {
    "accept": "csv",
    "authorization": "Bearer eyJhbGciOiJBMjU2S1ciLCJjdHkiOiJKV1QiLCJlbmMiOiJBMjU2R0NNIiwidHlwIjoiSldUIiwiemlwIjoiREVGIn0.2sWLLyfHEg36P0w_OoN1deMT6utrzPQNUmDsju8aEMYxUE_RoQZ7EQ.dzcuv2401USojJaf.LnbVzx0iDJBnGvm9N2rzh3wj81f-PkiYbNraZfT_LHq5QLo38tKUzqGWZ9D83qZwRMq-X1BNTHXYuoNkIN2fUVUmIRIR6dPQyERmOnAbqlGKeGiPdDIKQfZgI2SldGPm9gEPDRvvFIiqZtk60rF3J4UzendF3xnmd3gd8caeigmae55uD2SK1ONUGMhyVPUjoPH1OJrWeW1O0PnLS2vgaiUZBSsefKZRZ_sSaqoD5CqBCSOEaf-GN0-e1CQBJtxZKwDyHHnzkQoCbUtArQCQc1tQ3q60y1150hxPvZIyxwxKGrMeDLYQYkK6tL7DkdY3ayVK2UZsiDmOXLshdHCO9X9Nwg.7Cih39YOsHdwE_2F7kI2Bw"
}

appidList = {
    #"Time Master Slots":"com.timehero.vegas.slot"
    #"Weather & Rewards":"com.weather.rewards.forecast.money",
    #"Devil Survivor":"com.devilsurvivor.antihero",
    #"Piggy N Piggy":"com.happypiggy.tripletile.match.game"
    #"Lucky Winner ââ Bingo Games":"com.luckywinner.bingo.clash.buffalo",
    #"Devil Survivor IOS":"id6448251913",
    #"Slime Guardian":"com.slimeguardian.roguelike.towerdefense.matchgame",
    #"Slime Guardian (iOS)":"id6449955925",
    #"Keno Farm":"com.vegaskeno.luckyfarm.wincash",
    #"Pig N Pig":"com.pignpig.tripletile.match.game"
    "Savior: The Stickman":'com.stickman.survior.war.warriors',
    "Daub Farm":"com.daub.farm.bingo.bash.free",
    "Bingo Stars":"com.bingo.jackpot.free.casino.game"
}

pidList = {
    "Weather & Rewards":"mintegral_int,bytedanceglobal_int",
    "Devil Survivor":"facebook,bytedanceglobal_int,googleadwords_int",
    "Time Master Slots":"facebook",
    "Piggy N Piggy":"bytedanceglobal_int",
    "Lucky Winner ââ Bingo Games":"googleadwords_int",
    "Devil Survivor IOS":"facebook",
    "Slime Guardian":"facebook",
    "Slime Guardian (iOS)":"facebook",
    "Keno Farm":"unityads_int,facebook,googleadwords_int,",
    "Pig N Pig":"unityads_int,facebook,googleadwords_int,mintegral_int,bytedanceglobal_int",
    "Daub Farm":"facebook",
    "Bingo Stars":"unityads_int"
}

for app_name, app_id in appidList.items():
    for i in range(1,2):
        from_  = str(datetime.date.today() - datetime.timedelta(days=i)) #这里改日期，往前数的天数
        # 分国家数据
        dau_url = "https://hq1.appsflyer.com/api/agg-data/export/app/%s/geo_report/v5?from=%s&to=%s" %(app_id,from_,from_)
        print(i,dau_url)
        uu=requests.get(url=dau_url,headers=headers).text
        if len(uu)== 0 :
            print(from_+"无数据")
        else:
            event_df =  pd.read_csv(io.StringIO(uu))
            event_df.drop(event_df[pd.isnull(event_df['Media Source (pid)'])].index, inplace=True)
            print(event_df)
            dict = {
                'date':from_,
                'appname':app_name,
                "push_country":event_df["Country"],
                "af_prt":event_df["Agency/PMD (af_prt)"],
                "push_channel":event_df["Media Source (pid)"],
                "campaign":event_df["Campaign (c)"],
                "Impressions":event_df["Impressions"],
                "Clicks":event_df["Clicks"],
                "CTR":event_df["CTR"],
                "install":event_df["Installs"],
                "ConversionRate":event_df["Conversion Rate"],
                "Sessions":event_df["Sessions"],
                "LoyalUsers":event_df["Loyal Users"],
                "LoyalUsers_Installs":event_df["Loyal Users/Installs"],
                "TotalRevenue":event_df["Total Revenue"],
                "TotalCost":event_df["Total Cost"],
                "ROI":event_df["ROI"],
                "ARPU":event_df["ARPU"],
                "AverageeCPI":event_df["Average eCPI"]
            }
            df = pd.DataFrame(dict)
            tt=df.to_json(orient ='records')
            table = "wangzuan_spend_AF"
            print(df)
            # 保存数据
            savedata(table, tt)
            time.sleep(60)