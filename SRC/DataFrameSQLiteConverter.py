import pandas as pd
import sqlite3
import os
from typing import List, Optional, Union, Literal
import logging


class DataFrameSQLiteConverter:
    """
    DataFrame与SQLite数据库互转工具类
    """

    def __init__(self, db_path: str = "data.db"):
        """
        初始化转换器

        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
        self.setup_logging()

    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def dataframe_to_sqlite(self,
                            df: pd.DataFrame,
                            table_name: str,
                            if_exists: Literal["fail", "replace", "append"] = 'replace',
                            index: bool = False,
                            **kwargs) -> bool:
        """
        将DataFrame写入SQLite数据库

        Args:
            df: 要写入的DataFrame
            table_name: 目标表名
            if_exists: 表存在时的处理方式 ('fail', 'replace', 'append')
            index: 是否将DataFrame的索引作为列写入
            **kwargs: 其他传递给to_sql的参数

        Returns:
            bool: 操作是否成功
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                df.to_sql(
                    name=table_name,
                    con=conn,
                    if_exists=if_exists,
                    index=index,
                    **kwargs
                )

            self.logger.info(f"DataFrame成功写入表 '{table_name}'")
            return True

        except Exception as e:
            self.logger.error(f"写入数据库失败: {str(e)}")
            return False

    def sqlite_to_dataframe(self,
                            table_name: str = None,
                            query: str = None,
                            **kwargs) -> Optional[pd.DataFrame]:
        """
        从SQLite数据库读取数据到DataFrame

        Args:
            table_name: 要读取的表名
            query: 自定义SQL查询语句
            **kwargs: 其他传递给read_sql的参数

        Returns:
            pd.DataFrame: 读取的数据，失败返回None
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                if query:
                    df = pd.read_sql(query, conn, **kwargs)
                    self.logger.info(f"SQL查询执行成功，返回 {len(df)} 行数据")
                elif table_name:
                    df = pd.read_sql(f"SELECT * FROM {table_name}", conn, **kwargs)
                    self.logger.info(f"表 '{table_name}' 读取成功，共 {len(df)} 行")
                else:
                    self.logger.error("必须提供table_name或query参数")
                    return None

            return df

        except Exception as e:
            self.logger.error(f"从数据库读取失败: {str(e)}")
            return None

    def get_table_names(self) -> List[str]:
        """
        获取数据库中的所有表名

        Returns:
            List[str]: 表名列表
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [table[0] for table in cursor.fetchall()]
                self.logger.info(f"数据库中共有 {len(tables)} 个表")
                return tables
        except Exception as e:
            self.logger.error(f"获取表名失败: {str(e)}")
            return []

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """
        获取表结构信息

        Args:
            table_name: 表名

        Returns:
            pd.DataFrame: 表结构信息
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns_info = cursor.fetchall()

                # 转换为DataFrame
                df_info = pd.DataFrame(columns_info,
                                       columns=['cid', 'name', 'type', 'notnull', 'default_value', 'pk'])
                self.logger.info(f"表 '{table_name}' 结构信息获取成功")
                return df_info
        except Exception as e:
            self.logger.error(f"获取表结构失败: {str(e)}")
            return pd.DataFrame()

    def export_dataframe_to_sqlite(self,
                                   dataframes: Union[pd.DataFrame, dict],
                                   **kwargs) -> bool:
        """
        批量导出DataFrame到SQLite数据库

        Args:
            dataframes: 单个DataFrame或{表名: DataFrame}字典
            **kwargs: 传递给dataframe_to_sqlite的参数

        Returns:
            bool: 操作是否成功
        """
        try:
            if isinstance(dataframes, pd.DataFrame):
                # 单个DataFrame，使用默认表名
                table_name = kwargs.pop('table_name', 'data_table')
                return self.dataframe_to_sqlite(dataframes, table_name, **kwargs)

            elif isinstance(dataframes, dict):
                # 多个DataFrame
                success_count = 0
                for table_name, df in dataframes.items():
                    if self.dataframe_to_sqlite(df, table_name, **kwargs):
                        success_count += 1

                self.logger.info(f"批量导出完成: {success_count}/{len(dataframes)} 个表导出成功")
                return success_count == len(dataframes)
            else:
                self.logger.error("dataframes参数必须是DataFrame或字典")
                return False

        except Exception as e:
            self.logger.error(f"批量导出失败: {str(e)}")
            return False

    def import_sqlite_to_dataframes(self,
                                    table_names: List[str] = None) -> dict:
        """
        从SQLite数据库批量导入多个表到DataFrame

        Args:
            table_names: 要导入的表名列表，None表示导入所有表

        Returns:
            dict: {表名: DataFrame} 字典
        """
        try:
            if table_names is None:
                table_names = self.get_table_names()

            dataframes = {}
            success_count = 0

            for table_name in table_names:
                df = self.sqlite_to_dataframe(table_name=table_name)
                if df is not None:
                    dataframes[table_name] = df
                    success_count += 1

            self.logger.info(f"批量导入完成: {success_count}/{len(table_names)} 个表导入成功")
            return dataframes

        except Exception as e:
            self.logger.error(f"批量导入失败: {str(e)}")
            return {}


def main():
    """示例用法"""
    converter = DataFrameSQLiteConverter("example.db")

    # 示例1: 创建测试DataFrame并导出到SQLite
    print("=== 示例1: DataFrame导出到SQLite ===")

    # 创建测试数据
    df1 = pd.DataFrame({
        'id': range(1, 6),
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'London', 'Tokyo', 'Paris', 'Sydney']
    })

    df2 = pd.DataFrame({
        'product_id': ['P001', 'P002', 'P003'],
        'product_name': ['Laptop', 'Mouse', 'Keyboard'],
        'price': [999.99, 25.50, 75.00],
        'stock': [50, 200, 150]
    })

    # 导出单个DataFrame
    converter.dataframe_to_sqlite(df1, 'users')

    # 批量导出多个DataFrame
    converter.export_dataframe_to_sqlite({
        'users': df1,
        'products': df2
    })

    # 示例2: 从SQLite导入DataFrame
    print("\n=== 示例2: 从SQLite导入DataFrame ===")

    # 获取所有表名
    tables = converter.get_table_names()
    print(f"数据库中的表: {tables}")

    # 导入单个表
    users_df = converter.sqlite_to_dataframe(table_name='users')
    if users_df is not None:
        print("用户表数据:")
        print(users_df)

    # 批量导入所有表
    all_dataframes = converter.import_sqlite_to_dataframes()
    print(f"\n导入的表数量: {len(all_dataframes)}")

    # 示例3: 使用自定义查询
    print("\n=== 示例3: 使用自定义查询 ===")
    query_df = converter.sqlite_to_dataframe(
        query="SELECT name, age FROM users WHERE age > 28"
    )
    if query_df is not None:
        print("年龄大于28的用户:")
        print(query_df)

    # 示例4: 获取表结构信息
    print("\n=== 示例4: 表结构信息 ===")
    table_info = converter.get_table_info('users')
    print("用户表结构:")
    print(table_info)


if __name__ == "__main__":
    main()