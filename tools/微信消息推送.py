import pyautogui
import pyperclip
import time
from wxpy import *

def get_msg():
    """想发的消息，每条消息空格分开"""
    contents = '《谢啦，兄弟》'
    return [contents for i in range(20)]


def send(msg):
    # 复制需要发送的内容到粘贴板
    pyperclip.copy(msg)
    # 模拟键盘 ctrl + v 粘贴内容
    pyautogui.hotkey('ctrl', 'v')
    # 发送消息
    pyautogui.press('enter')


def send_msg(friend):
    # Ctrl + alt + w 打开微信
    pyautogui.hotkey('ctrl', 'alt', 'w')
    # 搜索好友
    pyautogui.hotkey('ctrl', 'f')
    # 复制好友昵称到粘贴板
    pyperclip.copy(friend)
    # 模拟键盘 ctrl + v 粘贴
    pyautogui.hotkey('ctrl', 'v')
    time.sleep(1)
    # 回车进入好友消息界面
    pyautogui.press('enter')
    # 一条一条发送消息
    for msg in get_msg():
        send(msg)
        # 每条消息间隔 2 秒
        time.sleep(2)

class push_wx():
    def __init__(self):
        # 初始机器人对象
        self.bot=Bot()
    def get_friend(self,friend_name):
        friend=self.bot.friends().search(friend_name)[0]
        return friend
    def push_friend(self,content):
        self.get_friend().send(content)
        


# if __name__ == '__main__':
#     friend_name = "崽种"
#     send_msg(friend_name)