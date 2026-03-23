import gspread
import pandas as pd
from typing import Any, List, Optional


class GoogleSheetClient:
    def __init__(
        self,
        credentials_file: str,
        spreadsheet_key: str,
        worksheet_name: Optional[str] = None,
    ):
        """
        初始化 Google Sheet 客户端

        :param credentials_file: Service Account JSON 文件路径
        :param spreadsheet_key: Google Sheet 的 ID（推荐用 URL 中那串 key）
        :param worksheet_name: 工作表名称，不传则默认取第一个 sheet
        """
        self.credentials_file = credentials_file
        self.spreadsheet_key = spreadsheet_key
        self.worksheet_name = worksheet_name

        self.gc = None
        self.spreadsheet = None
        self.worksheet = None

        self._connect()

    def _connect(self):
        """建立连接并打开表格/工作表"""
        try:
            self.gc = gspread.service_account(filename=self.credentials_file)
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_key)

            if self.worksheet_name:
                self.worksheet = self.spreadsheet.worksheet(self.worksheet_name)
            else:
                self.worksheet = self.spreadsheet.sheet1

        except Exception as e:
            raise RuntimeError(f"初始化 Google Sheet 失败: {e}")

    def switch_worksheet(self, worksheet_name: str):
        """切换工作表"""
        try:
            self.worksheet = self.spreadsheet.worksheet(worksheet_name)
            self.worksheet_name = worksheet_name
        except Exception as e:
            raise RuntimeError(f"切换工作表失败: {e}")

    # =========================
    # 读取方法
    # =========================

    def read_cell(self, cell: str) -> Any:
        """读取单个单元格，比如 A1"""
        try:
            return self.worksheet.acell(cell).value
        except Exception as e:
            raise RuntimeError(f"读取单元格 {cell} 失败: {e}")

    def read_range(self, cell_range: str) -> List[List[Any]]:
        """读取范围，比如 A1:C5"""
        try:
            return self.worksheet.get(cell_range)
        except Exception as e:
            raise RuntimeError(f"读取范围 {cell_range} 失败: {e}")

    def read_row(self, row_num: int) -> List[Any]:
        """读取整行"""
        try:
            return self.worksheet.row_values(row_num)
        except Exception as e:
            raise RuntimeError(f"读取第 {row_num} 行失败: {e}")

    def read_col(self, col_num: int) -> List[Any]:
        """读取整列，1=A列"""
        try:
            return self.worksheet.col_values(col_num)
        except Exception as e:
            raise RuntimeError(f"读取第 {col_num} 列失败: {e}")

    def read_all_values(self) -> List[List[Any]]:
        """读取所有有效数据"""
        try:
            return self.worksheet.get_all_values()
        except Exception as e:
            raise RuntimeError(f"读取全部数据失败: {e}")

    def read_all_records(self) -> List[dict]:
        """
        按表头读取
        第一行需为 header
        """
        try:
            return self.worksheet.get_all_records()
        except Exception as e:
            raise RuntimeError(f"读取 records 失败: {e}")

    def read_range_cells(self, cell_range: str):
        """
        读取 Cell 对象列表，带 row/col/value 信息
        """
        try:
            return self.worksheet.range(cell_range)
        except Exception as e:
            raise RuntimeError(f"读取 Cell 对象范围 {cell_range} 失败: {e}")

    def to_dataframe(self, cell_range: Optional[str] = None) -> pd.DataFrame:
        """
        转为 DataFrame
        默认按 get_all_records() 读取
        如果传 cell_range，则默认第一行为表头
        """
        try:
            if cell_range:
                data = self.worksheet.get(cell_range)
                if not data:
                    return pd.DataFrame()
                return pd.DataFrame(data[1:], columns=data[0])
            else:
                return pd.DataFrame(self.worksheet.get_all_records())
        except Exception as e:
            raise RuntimeError(f"转换 DataFrame 失败: {e}")

    # =========================
    # 写入方法
    # =========================

    def write_cell(self, cell: str, value: Any):
        """写入单个单元格"""
        try:
            self.worksheet.update_acell(cell, value)
        except Exception as e:
            raise RuntimeError(f"写入单元格 {cell} 失败: {e}")

    def write_range(self, start_range: str, values: List[List[Any]]):
        """
        批量写入范围
        例如:
        start_range = "D2:F3"
        values = [
            ["name", "age", "city"],
            ["Long", 28, "Singapore"]
        ]
        """
        try:
            self.worksheet.update(start_range, values)
        except Exception as e:
            raise RuntimeError(f"写入范围 {start_range} 失败: {e}")

    def append_row(self, values: List[Any]):
        """追加一行"""
        try:
            self.worksheet.append_row(values)
        except Exception as e:
            raise RuntimeError(f"追加行失败: {e}")

    def append_rows(self, values: List[List[Any]]):
        """追加多行"""
        try:
            self.worksheet.append_rows(values)
        except Exception as e:
            raise RuntimeError(f"追加多行失败: {e}")

    def update_cells_by_range(self, cell_range: str, new_values: List[Any]):
        """
        按 cell 对象方式更新
        适合你需要逐格控制时使用

        示例:
            cell_range = "A1:A3"
            new_values = ["x", "y", "z"]
        """
        try:
            cells = self.worksheet.range(cell_range)
            if len(cells) != len(new_values):
                raise ValueError("new_values 数量必须和范围内单元格数量一致")

            for cell, value in zip(cells, new_values):
                cell.value = value

            self.worksheet.update_cells(cells)
        except Exception as e:
            raise RuntimeError(f"更新范围 {cell_range} 失败: {e}")

    # =========================
    # 工具方法
    # =========================

    def get_worksheet_title(self) -> str:
        """当前工作表名称"""
        return self.worksheet.title

    def get_spreadsheet_title(self) -> str:
        """当前表格名称"""
        return self.spreadsheet.title