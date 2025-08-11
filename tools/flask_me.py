from flask import Flask
from flask import request#用于接收请求参数
import pymysql
import pandas as pd
import json
import datetime
import requests
from flask import Flask, send_from_directory, Response, make_response
import os
import mimetypes
app = Flask(__name__)

# 定义数据库连接
class with_mysql(object):
    def __init__(self):
        # 数据库实例基础信息
        self.config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': 'milipwd',
            'db': 'data_analysis',
            'charset': 'utf8mb4',
        }

    def get_info(self,sql):
        conn = pymysql.connect(**self.config)
        cursor = conn.cursor()
        sql = sql
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        results = cursor.fetchall()
        conn.close()
        # print(results)
        df_results = pd.DataFrame(list(results))
        #, columns=['pkgname','state_manage']
        return df_results
    def save_dict(self, table_name, data_dict):
        # 连接数据库
        conn = pymysql.connect(**self.config)
        # 获取游标，写入数据、更新数据基于游标操作
        cursor = conn.cursor()
        table = table_name
        # 获取数据列名
        keys = ', '.join(data_dict.keys())
        # 组装数据值，将数据格式转化为字符串，
        values = ', '.join(['%s'] * len(data_dict))
        sql = 'REPLACE INTO {table}({keys}) VALUES ({values})'.format(table=table, keys=keys, values=values)
        try:
            # 使用游标时，支持占位符，第二个参数为实际值
            if cursor.execute(sql, tuple(data_dict.values())):
                # print('S')
                conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
        conn.close()

@app.route('/update', methods=["GET", "POST"])
def indexupdate():
    #获取IP地址
    ip = request.remote_addr
    print(ip)
    table = request.form.get("table")  # 获取表单的请求参数
    print(table)
    values_str = request.form.get("values", "null")
    # 将json对象转化为Python对象，此处返回列表
    value_list = json.loads(values_str)
    print(value_list)
    mysql = with_mysql()
    # 每次插入一条数据
    try:
        for j in value_list:
            mysql.save_dict(table, j)
        return '更新成功'
    except:
        return '更新失败'
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)