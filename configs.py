# mili爬虫数据库，主要用于存储三方平台返回数据
mili_mysql = {
    'url': '150.230.206.248',
    'user': 'root',
    'pwd': 'hNJA@N1FAlNas',
    'port': '3306'
}
# s=f"mysql+pymysql://{mili_mysql['user']}:{mili_mysql['pwd']}@{mili_mysql['url']}:3306/?charset=utf8mb4"


# adjust app token 信息
adjust_app_info={
    'Daub Farm':['g4mozkbn05c0',['US','BR','PH']],
    'Doomsday Vanguard':['3dle7bbjgwhs',['TW','KR','JP','MO','HK']],
    'Doomsday Vanguard ios':['h40ytipbjkzk',['US','JP']],
    'Bingo Stars':['lv2y844vjo5c',['US']],
    'Keno Farm B':['3ycwnrahi1ds',['US','BR']],
    'Bingo Party':['1bqgxy43lvs0',['US']]
}

# 数数app密钥
TE_app_token={
    'Lucky Winner':'bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0',
    'Doomsday Vanguard':'hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ',
    'Savior: The Stickman':'2jmTqGDZwIUkGvf1e806R4UG07WyEfvPYXdrojeBkWvXZNM6o78Wl3ayFcdDU0Hn'
}

# Max报告api_key
max_api_key=['Dk_e4il2vEoKZacIKkWWdTN9SQt3lEhtX4HsEVeoz0wWJQV_my6nR742GTOcE7lDscTySHcyn2k66S1vTHAAG6',
                # 'nHqgpkyXTyeot-vvB65wQmQnH3A-XFh5T903CbT6IQNZpCekoQJ0HlDfHUCyBaboZkI_XecrsJpqfq-cgpgEKy',
                # '3NeR-dGmSFxal-9OZCWsuxgvTdTe0J1Zp3UIfepCcnQC-CGyWl9grPkpb7GWfCzJKDf5QEDc_OAdil-zD6Tou0',
                # 'xyfyfBanPfEcl1RltX-BtH1pBO_OQUAJdECr597KNSZfwq4Eo_4O0WK6l3zhyhgCVe0Kn1hyfmLpJ2y8dno99o',
                # 'y3U_ha5Ox3eZd49eBb2KBFxM42hbztgnvJBDKlUUOiaIo32o5XGJUD7DCW0aQ51PTBxZJbO8u_dFZTHlfseDet'
                # 'AUsu4ZOKZbNQgQoKMDsUTn7RTpL3qy5CshI05aDq6BDeck0swg23vlr1ugS6RGZTeTPOnZrWiBaFvPQIOcTpSb'
                ]

# Top on 发行者密钥
PUBLISHER_KEY = '2d188b58ea9634ded304c4f522a03bfd5a996541'


# app信息
app_sql="""
    select app.*,date.date
from wangzuan_app app,
(select date from date where date between '{0}' and '{1}') date
"""
ng_user_active_sql ="""
SELECT date,appname,country,users FROM `ng_user_active`
where date between '{0}' and '{1}'
union all
SELECT date,appname,'all' as country,sum(users) as users FROM `ng_user_active`
where date between '{0}' and '{1}'
group by date,appname
"""

# 提现支出sql
cost_sql = """
    select date,appname,country,sum(spend_money) as cost_money
    from wz_cashoust_spend
    where date between '{0}' and '{1}'
    group by date,appname,country
    
    union all
    
    select date,appname,country,sum(payout) as cost_money
    from cost_payout
    where date between '{0}' and '{1}'
    group by date,appname,country
    
    union all
    
    select date,appname,'all' as country,sum(payout) as cost_money
    from cost_payout
    where date between '{0}' and '{1}'
    group by date,appname
"""
# 广告支出 和推广人数 unity + 其他渠道,周一 到周五其他渠道支出数据需要飞书表格更新 ,节假日使用adjust
spend_sql = """
-- 飞书每日更新支出
with network_tab as(
select 
date,appname as app,
country_code,
sum(cost) as money
from wangzhuan_spend_new 
where date between '{0}' and '{1}'
group by date,appname,country_code
union all 
select 
date,appname,'all' as country_code ,sum(cost) as money
from wangzhuan_spend_new 
where date between '{0}' and '{1}'
group by date,appname
),
-- adjust 返回支出
adjust_tab as(
select date,if(app='Tales of Brave Android','Tales of Brave',app) as app,country_code,sum(network_cost) as adjust_money 
from adjust_network_cost
where date between '{0}' and '{1}'
group by date,app,country_code
union all 
select date,if(app='Tales of Brave Android','Tales of Brave',app) as app,'all' as country_code,sum(network_cost) as adjust_money 
from adjust_network_cost
where date between '{0}' and '{1}'
group by date,app
),
-- adjust 推广人数
adjust_installs_tab as(
select date,if(app='Tales of Brave Android','Tales of Brave',app) as app,country_code,sum(installs) as installs
from wangzhuan_adjust_installs 
where 
((network !='Organic' and app !='Doomsday Vanguard ios ')
    or app ='Doomsday Vanguard ios')
and date between '{0}' and '{1}'
group by date,app,country_code
union all 
select date,if(app='Tales of Brave Android','Tales of Brave',app) as app,'all' as country_code,sum(installs) as installs
from wangzhuan_adjust_installs 
where 
((network !='Organic' and app !='Doomsday Vanguard ios ')
    or app ='Doomsday Vanguard ios')
and date between '{0}' and '{1}'
group by date,app
),
cost_tab as (
select adjust_tab.date,
adjust_tab.app as appname,
adjust_tab.country_code  as country,
if(adjust_tab.adjust_money >= network_tab.money or network_tab.money is NULL,adjust_tab.adjust_money,network_tab.money) as money
from adjust_tab  left join network_tab 
on network_tab.date = adjust_tab.date 
and network_tab.app = adjust_tab.app 
and network_tab.country_code = adjust_tab.country_code
)

select cost_tab.* ,adjust_installs_tab.installs  as pushinstall
from cost_tab left join adjust_installs_tab
on cost_tab.date = adjust_installs_tab.date 
and cost_tab.appname = adjust_installs_tab.app 
and cost_tab.country = adjust_installs_tab.country_code

"""

spend_sql_old = """
	select a.date,a.appname,a.country,
	max(a.money) as money ,
	max(a.pushinstall) as pushinstall  
	from (select tab.date,tab.appname,tab.country ,sum(tab.money) as money,sum(tab.pushinstall) as pushinstall from (
    select date,
		if(appname='Keno Farm','Keno Farm B',appname) as appname,
		country_code as country,sum(cost) as money,sum(installs) as pushinstall
    from wangzhuan_spend_new
    where date between '{0}' and '{1}'
    and country_code !='全球'
    group by date,appname,country_code ) tab 
		group by tab.date,tab.appname,tab.country 
		
	union 
	
	select tab1.date,tab1.app as appname,tab1.country_code as country ,
	sum(tab1.network_cost) as money,sum(tab1.installs) as pushinstall
	from(
    select a.*,b.network_cost
    from 
    (select date,
    if(app='Keno Farm','Keno Farm B',app) as app,country_code,
    sum(installs) as installs
    from wangzhuan_adjust_installs
    where date between '{0}' and '{1}'
    and ((network !='Organic' and app !='Doomsday Vanguard ios')
    or app ='Doomsday Vanguard ios')
    group by date,app,country_code) a 
    left join 
    (select date,if(app='Keno Farm','Keno Farm B',app) as app,country_code,
    sum(network_cost) as network_cost
    from adjust_network_cost
    where date between '{0}' and '{1}'
    and network !='Organic'
    group by date,app,country_code) b 
    on a.date =b.date and a.app = b.app and a.country_code =b.country_code
    ) tab1 
	group by tab1.date,tab1.app,tab1.country_code) a 
	group by a.date,a.appname,a.country 

union all 

select c.date,c.appname,'all' as country,
sum(c.money) money ,sum(pushinstall) as pushinstall
from 
(
select a.date,a.appname,a.country,max(a.money) as money ,
max(a.pushinstall) as pushinstall 
from (select tab.date,tab.appname,tab.country ,sum(tab.money) as money,sum(tab.pushinstall) as pushinstall from (
    select date,
		if(appname='Keno Farm','Keno Farm B',appname) as appname,
		country_code as country,sum(cost) as money,sum(installs) as pushinstall
    from wangzhuan_spend_new
    where date between '{0}' and '{1}'
    and country_code !='全球'
    group by date,appname,country_code ) tab 
		group by tab.date,tab.appname,tab.country 
		
	union 
	
	select tab1.date,tab1.app as appname,tab1.country_code as country ,
	sum(tab1.network_cost) as money,sum(tab1.installs) as pushinstall
	from(
    select a.*,b.network_cost
    from 
    (select date,if(app='Keno Farm','Keno Farm B',app) as app,country_code,
    sum(installs) as installs
    from wangzhuan_adjust_installs
    where date between '{0}' and '{1}'
    
    and ((network !='Organic' and app !='Doomsday Vanguard ios')
    or app ='Doomsday Vanguard ios')
    group by date,app,country_code) a 
    left join 
    (select date,if(app='Keno Farm','Keno Farm B',app) as app,country_code,
    sum(network_cost) as network_cost
    from adjust_network_cost
    where date between '{0}' and '{1}'
    and country_code in ('TEST')
    and network !='Organic'
    group by date,app,country_code) b 
    on a.date =b.date and a.app = b.app and a.country_code =b.country_code
    ) tab1 
	group by tab1.date,tab1.app,tab1.country_code) a 
	group by a.date,a.appname,a.country 
) c group by c.date,c.appname
"""

# 新增活跃数据 新产品使用数据平台数据，老产品使用flurry数据
insert_sql = """
    select date,app_name as appname ,app_country as country,
    newDevices as install ,activeDevices as daus,averageTime_PerDevice
    from wangzuan_flurry
    where date between '{0}' and '{1}'
"""

# 广告收入 wangzhuan
On_admob_sql = """
    with tab as (
with a AS
    (SELECT date,appname,country,imprCount,revenue,
           (CASE WHEN placementname LIKE 're%%' THEN 'RewardedVideo'
               WHEN placementname LIKE 'in%%' THEN 'Interstitial'
               WHEN placementname LIKE 'ba%%' THEN 'Banner'
               ELSE 'othertype' END) adtype
    FROM sr_OM
    where date between '{0}' and '{1}' 
    union all
    SELECT date,appname,country,imprCount,revenue,
           (CASE WHEN ad_type LIKE 're%%' THEN 'RewardedVideo'
               WHEN ad_type LIKE 'in%%' THEN 'Interstitial'
               WHEN ad_type LIKE 'ba%%' THEN 'Banner'
               ELSE 'othertype' END) adtype
    FROM sr_admobmerge
    where date between '{0}' and '{1}' 
    union all
    select date,app as appname,country,impressions as imprCount,revenue,
           (CASE WHEN ad_type LIKE 'RE%%' THEN 'RewardedVideo'
               WHEN ad_type LIKE 'IN%%' THEN 'Interstitial'
               WHEN ad_type LIKE 'BA%%' THEN 'Banner'
               WHEN ad_type LIKE 'APPOPEN%%' THEN 'APPOPEN'
               ELSE 'othertype' END) adtype
    FROM sr_applovin_MAX
    where date between '{0}' and '{1}' 
    union all
    select date,appname,country,imprCount,revenue,
           (CASE WHEN ad_type LIKE 'RE%%' THEN 'RewardedVideo'
               WHEN ad_type LIKE 'IN%%' THEN 'Interstitial'
               WHEN ad_type LIKE 'BA%%' THEN 'Banner'
               ELSE 'othertype' END) adtype
    FROM sr_topon
    where date between '{0}' and '{1}' 
    
    union all
    select date,appName as appname,countryCode as country,sum(imp),sum(revenue),
           (CASE WHEN adUnits LIKE 'RE%%' THEN 'RewardedVideo'
               WHEN adUnits LIKE 'IN%%' THEN 'Interstitial'
               WHEN adUnits LIKE 'BA%%' THEN 'Banner'
               ELSE 'othertype' END) adtype
    FROM wangzuan_sr_ironsource
    where date between '{0}' and '{1}' 
    group by date,appName,countryCode,adUnits
    
    union all
    select date,appName as appname,'all'  as country,sum(imp),sum(revenue),
           (CASE WHEN adUnits LIKE 'RE%%' THEN 'RewardedVideo'
               WHEN adUnits LIKE 'IN%%' THEN 'Interstitial'
               WHEN adUnits LIKE 'BA%%' THEN 'Banner'
               ELSE 'othertype' END) adtype
    FROM wangzuan_sr_ironsource
    where date between '{0}' and '{1}' 
    and countryCode <> 'all'
    group by date,appName,adUnits
    
    )
    SELECT date,appname,country,adtype,SUM(CASE when imprCount IS NULL THEN 0 ELSE imprCount END) as total_show,SUM(CASE when revenue IS NULL THEN 0 ELSE revenue END) as total_re
    FROM a
    GROUP BY date,appname,country,adtype
		) 
		select tab.date,tab.appname,tab.country,
		sum(if(tab.adtype='RewardedVideo',tab.total_show,0)) as '激励展示',
		sum(if(tab.adtype='RewardedVideo',tab.total_re,0)) as '激励收入',
		sum(if(tab.adtype='Interstitial',tab.total_show,0)) as '插页展示',
		sum(if(tab.adtype='Interstitial',tab.total_re,0)) as '插页收入',
		sum(if(tab.adtype='Banner',tab.total_show,0)) as 'banner展示',
		sum(if(tab.adtype='Banner',tab.total_re,0)) as 'banner收入'
		from tab 
		group by  tab.date,tab.appname,tab.country
"""
# 广告收入 cp  if(tab.appname='Keno Farm','Keno Farm B',tab.appname) 2024-09-13开始全部使用applovin聚合收入
On_admob_cp_sql="""
with tab as(
with a as(

select date,
case when platform ='ios' then concat(app,' ios')
else app
end as appname,
country,impressions as imprCount,revenue,
       (CASE WHEN ad_type LIKE 'RE%%' THEN 'RewardedVideo'
		   WHEN ad_type LIKE 'IN%%' THEN 'Interstitial'
		   WHEN ad_type LIKE 'BA%%' THEN 'Banner'
		   ELSE 'othertype' END) adtype 
from sr_applovin_MAX
where date between '{0}' and '{1}'
and country !='all'
union all
select date,
case when platform ='ios' then concat(app,' ios')
else app
end as appname,
'all' as country,sum(impressions) as imprCount,sum(revenue) as revenue,
       (CASE WHEN max(ad_type) LIKE 'RE%%' THEN 'RewardedVideo'
		   WHEN max(ad_type) LIKE 'IN%%' THEN 'Interstitial'
		   WHEN max(ad_type) LIKE 'BA%%' THEN 'Banner'
		   ELSE 'othertype' END) adtype
from sr_applovin_MAX
where date between '{0}' and '{1}'
and country !='all'
group by date,appname
)

SELECT date,appname,country,adtype,
SUM(CASE when imprCount IS NULL THEN 0 ELSE imprCount END) as total_show,
SUM(CASE when revenue IS NULL THEN 0 ELSE revenue END) as total_re
FROM a
GROUP BY date,appname,country,adtype)
select tab.date,
        if(tab.appname='Keno Farm','Keno Farm B',tab.appname) as appname,
        tab.country,
		sum(if(tab.adtype='RewardedVideo',tab.total_show,0)) as '激励展示',
		sum(if(tab.adtype='RewardedVideo',tab.total_re,0)) as '激励收入',
		sum(if(tab.adtype='Interstitial',tab.total_show,0)) as '插页展示',
		sum(if(tab.adtype='Interstitial',tab.total_re,0)) as '插页收入',
		sum(if(tab.adtype='Banner',tab.total_show,0)) as 'banner展示',
		sum(if(tab.adtype='Banner',tab.total_re,0)) as 'banner收入'
		from tab 
		group by  tab.date,tab.appname,tab.country
"""

# adjoe收入
adjoe_sql = """
    select date,sdk_name as adjoe,country,revenue  as 'adjoe收入',ad_impression_count_distinct,ecpm_nondistinct as 'adjoe_ecpm'
    from sr_adjoe_full
    where date between '{0}' and '{1}'
    
    union all
    
    select date,sdk_name as adjoe,'all' country,sum(revenue),sum(ad_impression_count_distinct),sum(revenue)/sum(ad_impression_count_distinct)*1000
    from sr_adjoe_full
    where date between '2023-06-26' and '{1}'
    group by date,sdk_name
"""
# okspin收入
okspin_sql = """
    select Day as date,Media as okspin,Country as country,Earning as 'okspin收入',CPM as 'okspin_ecpm'
    from sr_okspin
    where Day between '{0}' and '{1}'
"""
# 内购收入
ng_sql="""
select date,appname,country,sum(money) as ng,count(distinct email_code) as ng_users
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname,country
union all
select date,appname,'all' as country,sum(money) as ng,count(distinct email_code) as ng_users
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname
"""

# cp产品积分墙收入
cp_adjoe_sql="""
with tab as(
select date,
case when sdk_name = 'Keno Farm' then 'Keno Farm B'
else sdk_name
end as appname,
country,revenue  as '收入'
    from sr_adjoe_full
    where date between '{0}' and '{1}'
    
    union all
    
    select date,
		case when sdk_name = 'Keno Farm' then 'Keno Farm B'
		else sdk_name
		end as appname,
		'all' country,sum(revenue) as '收入'
    from sr_adjoe_full
    where date between '{0}' and '{1}'
    group by date,appname
		
		union all 
		
		select Day as date,
		case when Media = '25279' then 'Keno Farm B'
		else Media
		end as appname,
		Country as country,Earning as '收入'
				from sr_okspin
				where Day between '{0}' and '{1}')
		select tab.date,tab.appname,tab.country,sum(tab.收入) as '积分墙收入'
		from tab 
		group by tab.date,tab.appname,tab.country
"""

# cp产品新老用户收入
ng_sql_1="""
select date,appname,country,
sum(if(live_day is NOT NULL and live_day = 0,money,0)) as '新用户收入',
sum(if(live_day is NOT NULL and live_day  !=0,money,0)) as '老用户收入'
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname,country

union all

select date,appname,'all' as country,
sum(if(live_day is NOT NULL and live_day = 0,money,0)) as '新用户收入',
sum(if(live_day is NOT NULL and live_day  != 0,money,0)) as '老用户收入'
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname
"""

# cp产品大中小R收入

ng_sql_2="""
select date,appname,country,
sum(if(money < 100,money,0)) as '小R用户收入',
sum(if(money >=100 and money < 300,money,0)) as '中R用户收入',
sum(if(money >=300,money,0)) as '大R用户收入'
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname,country

union all

select date,appname,'all' as country,
sum(if(money < 100,money,0)) as '小R用户收入',
sum(if(money >=100 and money < 300,money,0)) as '中R用户收入',
sum(if(money >=300,money,0)) as '大R用户收入'
from sr_neigou
where date between '{0}' and '{1}'
and	country <> 'all'
group by date,appname
"""




