import excel2img
import os

def img_get(file,sheet_name,name):
    if os.path.dirname(file) == "":
        file = os.getcwd() + "//" + file
    img_save = os.path.dirname(file) + "\截图-" + os.path.splitext(os.path.basename(file))[0]
    if not os.path.exists(img_save):
        os.makedirs(img_save)
    try:
        print("开始截图，请耐心等待。。。")
        excel2img.export_img(file, img_save +'\/' +f"{name}.png", sheet_name)
    except:
        print("【没有截图成功！！！】请检查excel文件路径名称、工作表的名称是否全部正确！！！")
    else:
        print("截图成功，【保存在" + img_save + "】")
# img_get(r'D:\milimili\BI_robot\网赚报表.xlsx','sheet1','pic111')
