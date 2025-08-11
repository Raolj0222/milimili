# coding=utf-8
import pandas as pd
import re
import requests
import sys
# path为tools的上层文件夹
sys.path.append('D:\milimili')
from BI_robot.feishu import FeiShu
feishu=FeiShu()

sql1="""WITH fengxian_name_table as (
        SELECT 
        -- concat(CAST("#user_id" as varchar),'@') as "#user_id",
        "#user_id",
        "cluster_name" , 
            (CASE 
                WHEN "cluster_name" = 'cohort_20230808_170447' THEN '下级用户无收入'
                WHEN "cluster_name" = 'cohort_20230808_165152' THEN '下级用户无收入有提现申请'
                WHEN "cluster_name" = 'cohort_20230808_143831' THEN '不同uid相同收款账号'
                WHEN "cluster_name" = 'cohort_20230808_141055' THEN '当日申请金额总和>=15'
                WHEN "cluster_name" = 'cohort_20230807_184927' THEN '当日申请次数>=3'
                WHEN "cluster_name" = 'cohort_20230803_170805' THEN '模拟器用户'
                WHEN "cluster_name" = 'cohort_20230807_162918' THEN 'VPN用户'
                WHEN "cluster_name" = 'cohort_20230815_162158' THEN '一日内国家代码有2个'
                WHEN "cluster_name" = 'cohort_20230815_162545' THEN '一日内国家代码大于2个'
                WHEN "cluster_name" = 'cohort_20230815_164642' THEN '同设备不同GAID'
                WHEN "cluster_name" = 'cohort_20230815_165341' THEN '一个账号中有三个及以上设备号'
                WHEN "cluster_name" = 'cohort_20230808_182002' THEN '绿币单次变化量大于100用户'
                WHEN "cluster_name" = 'cohort_20230815_170513' THEN '超过每日广告限制'
                WHEN "cluster_name" = 'cohort_20230808_183121' THEN '有广告完毕次数无收益'
                WHEN "cluster_name" = 'cohort_20230808_183303' THEN 'ecpm小于1'
                WHEN "cluster_name" = 'cohort_20230808_183415' THEN 'ecpm小于10'
                ELSE "cluster_name"
            END ) AS "风险名称"
        FROM user_result_cluster_25
        WHERE "cluster_name" IN (
                    'cohort_20230808_165152',
                    'cohort_20230808_143831',
                    'cohort_20230808_141055',
                    'cohort_20230807_184927',
                    'cohort_20230807_162918',
                    'cohort_20230815_162158',
                    'cohort_20230815_162545',
                    'cohort_20230815_164642',
                    'cohort_20230815_165341',
                    'cohort_20230808_182002',
                    'cohort_20230815_170513',
                    'cohort_20230808_183121'
                                )
                -- and "#account_id" =1703258802164195330
                ),
                tab2 as(SELECT "#user_id" ,CONCAT_WS('&', ARRAY_AGG("风险名称")) as fengxian_name  FROM fengxian_name_table
    GROUP BY "#user_id" )
select 
concat(CAST(users_tab."#account_id" as varchar),'@') as "账户ID",
fengxian_name as "风险名称",
users_tab."af_country_code" as "国家"
FROM 
(select "#account_id","af_country_code" ,"#user_id" from  ta.v_user_25 u where EXISTS (
SELECT "#user_id" FROM user_result_cluster_25 u1 WHERE "cluster_name"='tag_20231025_1' and u."#user_id"=u1."#user_id")
) users_tab
LEFT  JOIN tab2
ON users_tab."#user_id" = tab2."#user_id" """
url = "http://13.214.112.1:8992/querySql"
headers = {"Content-Type": "application/x-www-form-urlencoded"}
params = {
    "token": "bXjkYFgO2yzZcM1EnoaeHQg9oMoqjaUtWC7qrYUMI8PXKV2xyzOx1SCLhNFkm4r0",
    # "sql": "WITH fengxian_name_table as( SELECT \"#user_id\", \"cluster_name\" , (CASE WHEN \"cluster_name\" = 'cohort_20230808_170447' THEN '下级用户无收入' WHEN \"cluster_name\" = 'cohort_20230808_165152' THEN '下级用户无收入有提现申请' WHEN \"cluster_name\" = 'cohort_20230808_143831' THEN '不同uid相同收款账号' WHEN \"cluster_name\" = 'cohort_20230808_141055' THEN '当日申请金额总和>=15' WHEN \"cluster_name\" = 'cohort_20230807_184927' THEN '当日申请次数>=3' WHEN \"cluster_name\" = 'cohort_20230803_170805' THEN '模拟器用户' WHEN \"cluster_name\" = 'cohort_20230807_162918' THEN 'VPN用户' WHEN \"cluster_name\" = 'cohort_20230815_162158' THEN '一日内国家代码有2个' WHEN \"cluster_name\" = 'cohort_20230815_162545' THEN '一日内国家代码大于2个' WHEN \"cluster_name\" = 'cohort_20230815_164642' THEN '同设备不同GAID' WHEN \"cluster_name\" = 'cohort_20230815_165341' THEN '一个账号中有三个及以上设备号' WHEN \"cluster_name\" = 'cohort_20230808_182002' THEN '绿币单次变化量大于100用户' WHEN \"cluster_name\" = 'cohort_20230815_170513' THEN '超过每日广告限制' WHEN \"cluster_name\" = 'cohort_20230808_183121' THEN '有广告完毕次数无收益' WHEN \"cluster_name\" = 'cohort_20230808_183303' THEN 'ecpm小于1' WHEN \"cluster_name\" = 'cohort_20230808_183415' THEN 'ecpm小于10' ELSE \"cluster_name\" END) AS \"风险名称\" FROM user_result_cluster_25 WHERE \"cluster_name\" IN ( 'cohort_20230808_170447', 'cohort_20230808_165152', 'cohort_20230808_143831', 'cohort_20230808_141055', 'cohort_20230807_184927', 'cohort_20230803_170805', 'cohort_20230807_162918', 'cohort_20230815_162158', 'cohort_20230815_162545', 'cohort_20230815_164642', 'cohort_20230815_165341', 'cohort_20230808_182002', 'cohort_20230815_170513', 'cohort_20230808_183121', 'cohort_20230808_183303', 'cohort_20230808_183415' ) ), tab2 as(SELECT \"#user_id\" ,CONCAT_WS('&', ARRAY_AGG(\"风险名称\")) as fengxian_name FROM fengxian_name_table GROUP BY \"#user_id\" ) select concat(CAST(users_tab.\"#account_id\" as varchar),'@') as \"账户ID\", fengxian_name as \"风险名称\", users_tab.\"af_country_code\" as \"国家\" FROM (select \"#account_id\",\"af_country_code\" ,\"#user_id\" from ta.v_user_25 u where EXISTS ( SELECT \"#user_id\" FROM user_result_cluster_25 u1 WHERE \"cluster_name\"='tag_20231025_1' and u.\"#user_id\"=u1.\"#user_id\") ) users_tab LEFT JOIN tab2 ON users_tab.\"#user_id\" = tab2.\"#user_id\"",
    'sql':f"{sql1}",
    "format": "json",
    "timeoutSeconds": "600"
}
response = requests.post(url, headers=headers, params=params)
response = re.split('[\n]', response.text)
df_list=[['账号ID','风险等级','国家']]
for i in response[1:]:
    lis=re.sub('[\r@"\[\]]','',i)
    df_list.append(re.split(',',lis))
value_range=len(df_list)

# 清空飞书表内容
feishu.del_tab('YpOGsuJVQhiNeetBk8dctY5tnKe','6LhAGr','COLUMNS',1,3)
# 写入飞书表
feishu.insert_tab('YpOGsuJVQhiNeetBk8dctY5tnKe','6LhAGr',f'A1:C{value_range}',df_list)

