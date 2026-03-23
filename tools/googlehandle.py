import gspread
from google.oauth2.service_account import Credentials
import pandas as pd

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
client = gspread.authorize(creds)

# 打开表格
sheet = client.open("你的表格名").sheet1

# 读取所有数据
data = sheet.get_all_values()
print(data)


# 批量更新多个单元格
cell_list = sheet.range('A2:B4')  # 获取 A2:B4 的单元格范围

# 填充数据
for cell in cell_list:
    cell.value = '批量写入'

# 更新所有单元格
sheet.update_cells(cell_list)



# 示例
# 示例 DataFrame 数据
data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 35],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
df = pd.DataFrame(data)

# 将 DataFrame 转换为二维列表（包括列名作为第一行）
values = [df.columns.values.tolist()] + df.values.tolist()

# 定义写入的位置（A2 开始）
range_ = "A2"

# 批量更新数据到 Google Sheets update 方法会根据values自动调整实际范围，
sheet.update(range_, values)  # values 是前面转换的二维列表
