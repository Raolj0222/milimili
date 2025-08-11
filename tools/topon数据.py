import pandas as pd
import hashlib  # 导入 hashlib 库用于进行哈希计算
import json  # 导入 json 库用于 JSON 数据处理
import urllib.parse  # 导入 urllib.parse 库用于 URL 编码和解码
import requests
import datetime
import time

date_num = 1
PUBLISHER_KEY = '2d188b58ea9634ded304c4f522a03bfd5a996541'  # 发行者密钥
CONTENT_TYPE = "application/json"  # 请求内容类型
# 获取数据 示例

end= int((datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d'))  # 获取昨天的日期
start = int((datetime.date.today() - datetime.timedelta(days=2)).strftime('%Y%m%d'))  # 获取昨天的日期


def do_request(http_method, req_url, req_body):
    now_millis = int(time.time() * 1e3)  # 当前时间的毫秒级时间戳

    # 创建最终签名
    content_md5 = hashlib.md5(req_body.encode()).hexdigest().upper()  # 对请求体进行 MD5 哈希计算
    header_str = "X-Up-Key:{}\nX-Up-Timestamp:{}".format(PUBLISHER_KEY, str(now_millis))  # 构造请求头字符串
    relative_path = urllib.parse.urlsplit(req_url).path  # 获取请求 URL 的路径部分
    sign_str = "{}\n{}\n{}\n{}\n{}".format(http_method, content_md5, CONTENT_TYPE, header_str, relative_path)  # 构造签名字符串
    final_sign = hashlib.md5(sign_str.encode()).hexdigest().upper()  # 对签名字符串进行 MD5 哈希计算

    # 发起请求
    headers = {
        "X-Up-Key": PUBLISHER_KEY,
        "X-Up-Timestamp": str(now_millis),
        "X-Up-Signature": final_sign,
        "Content-Type": CONTENT_TYPE,
    }
    if http_method == "POST":
        response = requests.post(req_url, headers=headers, data=req_body)  # 发送 POST 请求
    elif http_method == "GET":
        response = requests.get(req_url, headers=headers)  # 发送 GET 请求
    else:
        # TODO: 处理其他请求方法
        response = "todo"  # 其他请求方法的处理（待完善）
    return response.text  # 返回响应结果的文本形式



def get_group_data():
    # 汇总数据
    req_url = 'https://openapi.toponad.com/v2/fullreport'  # 请求的 URL
    req_body = {
        "startdate": start,
        "enddate": end,
        'app_id_list':['a649e51147a81c','a64d9c53694b9e'],
        "group_by": ["date", "app", "area", "placement"],
        'metric':['all']
    }  # 请求体数据
    req_body_str = json.dumps(req_body, sort_keys=False)  # 将请求体转换为 JSON 字符串
    response = do_request("POST", req_url, req_body=req_body_str)  # 发起 POST 请求
    data = json.loads(response)["records"]
    # 转化为df
    df=pd.DataFrame(data)

    res=pd.DataFrame()
    res['date']=df['date'].str.slice(0,4) +'-'+ df['date'].str.slice(4,6 )+ '-'+ df['date'].str.slice(6,8)
    res['app']=df['app']
    res['country'] = df['area']
    res['ad']=df['placement']
    res['appname'] = res['app'].apply(lambda x: x['name'])
    res['appid'] = res['app'].apply(lambda x: x['id'])
    res['广告位'] = res['ad'].apply(lambda x: x['name'] if type(x) == type({'1': '1'}) and x != {} else '')
    res['广告位ID'] = res['ad'].apply(lambda x: x['id'] if type(x) == type({'1': '1'}) and x != {} else '')
    res['收益API']=df['revenue']
    res['ARPDAU']=df['arpu']
    res['新用户']=df['new_users']
    res['新用户占比']=df['new_user_rate']
    res['DEU']=df['deu']
    res['渗透率']=df['engaged_rate']
    res['展示/DAU']=df['imp_dau']
    res['展示/DEU'] = df['imp_deu']
    res['广告源请求']=df['request']
    res['广告源填充率']=df['fillrate']
    res['展示']=df['impression']
    res['展示率']=df['impression_rate']
    res['展示API']=df['impression_api']
    res['ecpm']=df['ecpm']
    res['ecpmAPI']=df['ecpm_api']
    # 删除多余列
    res.drop(columns=['app','ad'],inplace=True)
    res.sort_values(['date','广告源请求'], ascending=[True,False], inplace=True)
    return res


def get_detail_data():
    # 广告平台广告源明细数据
    req_url = 'https://openapi.toponad.com/v2/fullreport'  # 请求的 URL
    req_body = {
        "startdate": start,
        "enddate": end,
        'app_id_list':['a649e51147a81c','a64d9c53694b9e'],
        "group_by": ["date", "app", "area", "placement",'network_firm_id','adsource'],
        'metric':['all']
    }  # 请求体数据
    req_body_str = json.dumps(req_body, sort_keys=False)  # 将请求体转换为 JSON 字符串
    response = do_request("POST", req_url, req_body=req_body_str)  # 发起 POST 请求
    data = json.loads(response)["records"]
    # 转化为df
    df=pd.DataFrame(data)
    df.fillna(value=0,inplace=True)
    res = pd.DataFrame()
    res['date'] = df['date'].str.slice(0,4) +'-'+ df['date'].str.slice(4,6 )+ '-'+ df['date'].str.slice(6,8)
    res['app'] = df['app']
    res['appname'] = res['app'].apply(lambda x: x['name'])
    res['appid'] = res['app'].apply(lambda x: x['id'])
    res['country'] = df['area']
    res['ad'] = df['placement']
    res['广告位'] = res['ad'].apply(lambda x: x['name'] if type(x) == type({'1': '1'}) and x != {} else '')
    res['广告位ID'] = res['ad'].apply(lambda x: x['id'] if type(x) == type({'1': '1'}) and x != {} else '')
    res['广告平台']=df['network_firm']
    res['广告源']=df['adsource'].apply(lambda x: x['adsource_name'] if type(x) == type({'1': '1'}) and x != {} else '')
    res['收益API'] = df['revenue']
    res['展示'] = df['impression']
    res['展示率'] = df['impression'].astype('float')/df['request'].astype('float')/df['fillrate'].astype('float')
    res['展示API'] = df['impression_api']
    res['ecpm'] = df['ecpm']
    res['ecpmAPI'] = df['ecpm_api']
    res['广告源请求'] = df['request'].astype('int')
    res['广告源填充率'] = df['fillrate']
    #删除多余列
    res.drop(columns=['app', 'ad'], inplace=True)
    res.sort_values(['date','广告源请求'], ascending=[True,False], inplace=True)
    return res



