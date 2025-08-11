import pandas  as pd
import requests
import numpy as np


class TE():
    def __init__(self,token,product_id):
        self.token=token
        self.product_id = product_id
    def uploadFile(self,path):
        # 上传文件至数数 返回文件id，用于创建、更新数据表
        params={"token":self.token,
                 "projectId":self.product_id
                 }
        files = {
            'file': open(path, 'rb'),
        }
        response = requests.post('http://13.214.112.1:8992/open/datatable/uploadFile', params=params, files=files)
        print(response.json())
        return response.json()['data']['fileId']
    def create_tab(self,path,tab_name):
         # 根据文件创建数据表 ，需要填写数据表表名、列名和type
         fileid=self.uploadFile(path)
         # 读取表头，组装列明和type
         df=pd.read_excel(path)
         df_columns=list(df.columns)
         columns_list=[]
         for i in df_columns:
             dic={'columnName':i,
                  'dataType':'string'}
             columns_list.append(dic)
         # print(columns_list)
         headers = {
             'Content-Type': 'application/json',
         }
         params = {"token": self.token,
              "projectId": self.product_id
              }
         json_data = {
             'fileId': fileid,
             'datatableName': tab_name,
             'datatableColumns': columns_list
         }
         response = requests.post('http://13.214.112.1:8992/open/datatable/createDatatable',
                                  params=params,
                                  headers=headers,
                                  json=json_data)
         # print(response.json())
         return response.json()['data']['successRowCount']
    def update_tab(self,tab_name,path):
         # 利用本地文件更新数据表 incr_update:增量更新;repl_update:替换整表内容更新
         params = {
             'projectId': self.product_id,
             'token': self.token,
             'datatableName': tab_name,
             'updateType': 'INCR_UPDATE',
         }
         files = {
             'file': open(path, 'rb'),
         }
         response = requests.post('http://13.214.112.1:8992/open/datatable/updateDatatable', params=params, files=files)
         # print(response.json())
         return response.json()['data']['successRowCount']

# 测试
if __name__ == '__main__':
    te=TE('hr2Hu1VnBHg7g0UlNVuUKOjGQdxOwHGIyTBwF2QUWA1DQFW6f0kkOMIbXOOFlMAJ','33')
    id=te.create_tab('D:\Desktop\维度表.xlsx','daoju_info')
    print(id)




