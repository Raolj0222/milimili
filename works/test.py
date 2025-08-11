# import subprocess
# # 打开新的命令窗口
# subprocess.Popen('cmd', shell=True)
# # 在新的命令提示符窗口下执行命
# subprocess.Popen('"C:Program Files\Google\Chrome\Application\chrome.exe"--remote-debugging-port=9222',shell=True)
# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# import time
# options = Options()
#
# # options.add_experimental_option( "debuggerAddress"," ocahost:9222")
# options.add_argument(r'--user-data-dir=C:\Users\admin\AppData\Local\Google\Chrome\User Data')
# chromedriver_path = r"C:\Program Files\Google\Chrome\Application\chromedriver.exe"
# driver = webdriver.Chrome(chromedriver_path,options=options)
#
# driver.get('https://app.diandian.com/app/kxupur8o87gpoaw/ios-cover?market=1&id=6468928820&country=26&language=23')
#
# #appinfo-content > div.container > div.app-content-body.dd-flex.dd-align-start > div.content-side > div.container-body > div > div > div > div > div.cover > div.review-box > div.desc > div > div > div.overview-table > div > div.loading-wrap > div > div.transform-table-data
#
# time.sleep(30)
#
# list = driver.find_element(By.CSS_SELECTOR,".transform-table-data tr")
# print(list)
# for i in list:
#     print(i.text)
# exit()
gg_list=[f'{i}日广告收入' for i in range(1,46)]
gg_list.append('系列')
col=['新增设备用户数','1日留存人数', '当日广告收入'].extend(gg_list)
print(col)