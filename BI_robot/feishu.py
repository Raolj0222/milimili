import json
import pandas as pd
import requests
from apscheduler.schedulers.blocking import BlockingScheduler
web_hook='https://open.feishu.cn/open-apis/bot/v2/hook/fe451c6c-ad56-4978-82c6-6823acc7dd06'
web_hook_test='https://open.feishu.cn/open-apis/bot/v2/hook/d6c5f871-bd4b-448e-9f12-7a44cd292393'
web_hook_new ='https://open.feishu.cn/open-apis/bot/v2/hook/5d47bc35-8c59-466d-95db-d43f8e7f47c3'
class FeiShu(object):
    def get_token(self):
    	# 调用机器人的地址 如有更改 可查看飞书文档
        url = 'https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal'
        headers = {'content-type': 'application/json; charset=utf-8'}
        # 需要调用机器人的appid、app_secret
        #
        data = {'app_id': 'cli_a5acb73f22bf500c',
                'app_secret': 'FxLLNE3dRn8Y8tHu7SJhqE2yay8SFeal'}
        token = requests.post(url, headers=headers, json=data).json()['tenant_access_token']
        print(token)
        return token
    def read_tab(self,tabid,sheetid,sheet_range):
        # 功能： 读取指定表格位置的内容
        tat=self.get_token()
        header = {"content-type": "application/json",
                  "Authorization": "Bearer " + str(tat)}
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{tabid}/values/{sheetid}!{sheet_range}?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
        r = requests.get(url, headers=header)
        # print(r.json())
        df = pd.DataFrame(r.json()['data']['valueRange']['values'])
        if df.shape[0]>1:
            df.columns = df.iloc[0]
            df = df.iloc[1:, ]
        else:
            pass
        return df
    def insert_tab(self,tabid,sheetid,sheet_range,values_list):
        import json
        # 功能：向表格中写入内容
        tat = self.get_token()
        header = {"content-type": "application/json",
                  "Authorization": "Bearer " + str(tat)}
        url=f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{tabid}/values'
        post_data={"valueRange": {"range": f"{sheetid}!{sheet_range}" ,"values":values_list}}
        r=requests.put(url, data=json.dumps(post_data), headers=header)
        print(r.json()['msg'])
    def insert_tab_new(self,tabid,sheetid,sheet_range,values_list):
        import json
        # 功能：向表格中写入内容
        tat = self.get_token()
        header = {"content-type": "application/json",
                  "Authorization": "Bearer " + str(tat)}
        url=f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{tabid}/values_batch_update'
        post_data={"valueRanges": [{"range": f"{sheetid}!{sheet_range}" ,"values":values_list}]}
        r=requests.post(url, headers=header, json=post_data)
        print(r.json()['msg'])
    def del_tab(self,tabid,sheetid,del_type,start,end):
        import json
        # 功能：删除表格行列
        tat = self.get_token()
        header = {"content-type": "application/json",
                  "Authorization": "Bearer " + str(tat)}
        url=f'https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{tabid}/dimension_range'
        json_data = {
            'dimension': {
                'sheetId': f'{sheetid}',
                'majorDimension': del_type,
                'startIndex': start,
                'endIndex': end,
            },
        }
        response = requests.delete(url,headers=header, json=json_data)
        print(response.json()['msg'])
    def upload(self, path, token):
        # 功能：向飞书上传图片，返回img_key
        url = 'https://open.feishu.cn/open-apis/im/v1/images'
        headers = {'Authorization': 'Bearer {}'.format(token)}
        # print(headers)
        # path 图片路径
        with open(path, 'rb')as f:
            image = f.read()
        files = {
            "image": image
        }
        data = {
            "image_type": "message"
        }
        print(requests.post(url, headers=headers, files=files, data=data).json()['code'])
        img_key = requests.post(url, headers=headers, files=files, data=data).json()['data']['image_key']

        return img_key

    def push_report(self, title,img_key, web_hook):
        # 功能：利用飞书机器人推送消息至飞书群
        header = {
            "Content-Type": "application/json;charset=UTF-8"
        }

        # 发送富文本消息
        payload_message = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [
            [{
                "tag": "img",
                "image_key": img_key
            }]
        ]
                    }
                }
            }
        }
        # 图片消息
        message_body = {
            "msg_type": "image",
            "content": {'image_key': img_key}
        }

        ChatRob = requests.post(url=web_hook, data=json.dumps(payload_message), headers=header)
        # opener = ChatRob.json()
        # print(opener)
        #
        # if opener["StatusMessage"] == "success":
        #     print(u"%s 通知消息发送成功！" % opener)
        # else:
        #     print(u"通知消息发送失败，原因：{}".format(opener))

    def run(self, title,path, webhook):
        token = self.get_token()
        img_key = self.upload(path=path, token=token)
        self.push_report(title,img_key, web_hook=webhook)

    def my_schedule():
        schedu = BlockingScheduler()
        schedu.add_job(run, 'cron',day_of_week='0-6',hour='10', minute='40', second='00')
        schedu.start()

# 测试
if __name__ == '__main__':
    feishu = FeiShu()
    df=feishu.read_tab('XVYDsydUFhpNOPtRJurcPgCCnyf','9CtuQD','A1:A10')
    # feishu.del_tab('YlkGsdOYeh2XcQtr9YWcUiTwnKd', '1lHVw0', 'COLUMNS', 1, 3)
    print(df)
#     #需要传入图片path、web_hook 即可实现调用
#     feishu.run('title',r'D:\Desktop\20231012-105334.jpg',web_hook)
# feishu.my_schedule()


