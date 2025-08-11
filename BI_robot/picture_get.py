# -*- coding: utf-8 -*-
import xlsxwriter
from win32com.client import Dispatch, DispatchEx
import pythoncom
from PIL import ImageGrab, Image
import datetime

area_list = ['A1:AM9']  # 截图区域
picture_list = ['D:\milimili\BI_robot\报表截图\pic']  # 生成截图的文件名


# 函数功能：截取excel文件多个区域，生成对应多个.PNG图片
# 参数说明：
# filename：excel文件名（包括完整路径）
# sheetname：excel文件的sheet页名称
# screen_area：截图区域，以列表形式存储。取值例如：['A1:M11','A13:L23','A26:L36']，表示分别截取3个区域：A1到M11、A13到L23、A26到L36
# picture_name：生成截图的文件名（包括完整路径），以列表形式存储，程序会加上.PNG后缀。取值例如：['E:/pic1','E:/pic2','E:/pic3']
# flag：是否需要继续处理的标记，Y表示中间出现异常，没有生成全部截图，需要再调用此函数，处理未生成的截图；N表示全部处理完成
def excel_catch_screen(filename, sheetname, screen_area, picture_name, flag):
    pythoncom.CoInitialize()  # excel多线程相关
    try:
        excel = DispatchEx("Excel.Application")  # 启动excel
        excel.Visible = True  # 可视化
        excel.DisplayAlerts = False  # 是否显示警告

        wb = excel.Workbooks.Open(filename)  # 打开excel
        ws = wb.Sheets(sheetname)  # 选择sheet

        # 循环处理每个截图区域
        for i in range(0, len(screen_area)):
            ws.Range(screen_area[i]).CopyPicture()  # 复制图片区域
            ws.Paste()  # 粘贴
            excel.Selection.ShapeRange.Name = picture_name[i]  # 将刚刚选择的Shape重命名，避免与已有图片混淆
            ws.Shapes(picture_name[i]).Copy()  # 选择图片
            img = ImageGrab.grabclipboard()  # 获取剪贴板的图片数据
            img_name = picture_name[i] + ".PNG"  # 生成图片的文件名
            img.save(img_name)  # 保存图片

            # 记录已处理的截图区域
            x = screen_area[i]
            # 将已处理的截图区域从列表移除
            area_list.remove(x)
            # 记录已处理的截图名称
            y = picture_name[i]
            # 将已处理的截图名称从列表移除
            picture_list.remove(y)
            # 打印日志
            print('移除区域：' + x + ' 移除图片：' + y)
            print('剩余计数：' + str(len(screen_area)) + ' ' + str(len(picture_name)))

        flag = 'N'  # 如果程序执行到这里，说明所有截图都正常处理完成，将flag置为N
    except Exception as e:
        flag = 'Y'  # 只要有任一截图异常，退出当前程序，将flag置为Y，等待再次调用此函数

        print('error is:', e)  # 打印异常日志
    finally:
        wb.Close(SaveChanges=0)  # 关闭工作薄，不保存
        excel.Quit()  # 退出excel
        pythoncom.CoUninitialize()

        return flag  # 返回flag


# 调用截图函数excel_catch_screen的入口
def gen(filename, sheetname, area_list, picture_list):
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' : 截图开始')

    try:
        flag = 'Y'
        while flag == 'Y':  # 循环调用截图函数
            flag = excel_catch_screen(filename, sheetname, area_list, picture_list, flag)
    except Exception as e:
        print('main error is:', e)
    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ' : 截图结束')


if __name__ == "__main__":
    gen(r'D:\milimili\BI_robot\网赚报表.xlsx','sheet1',area_list,picture_list)
