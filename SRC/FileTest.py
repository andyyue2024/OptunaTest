import pandas as pd
import os
from pathlib import Path
from typing import Union, Optional
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def query_data_with_cache(
        start_time: str,
        end_time: str,
        cache_dir: str = "./cache",
        force_refresh: bool = False
) -> Optional[pd.DataFrame]:
    """
    查询数据并缓存到Excel文件，如果缓存存在则直接读取缓存

    Args:
        start_time (str): 开始时间
        end_time (str): 结束时间
        cache_dir (str): 缓存目录路径，默认为"./cache"
        force_refresh (bool): 是否强制刷新缓存，默认False

    Returns:
        pd.DataFrame or None: 查询到的数据，如果查询失败返回None
    """

    # 创建缓存目录
    cache_path = Path(cache_dir)
    cache_path.mkdir(exist_ok=True)

    # 生成文件名（处理可能包含非法文件名字符的情况）
    safe_start_time = "".join(c for c in start_time if c.isalnum() or c in (' ', '-', '_', '.'))
    safe_end_time = "".join(c for c in end_time if c.isalnum() or c in (' ', '-', '_', '.'))
    filename = f"{safe_start_time}_{safe_end_time}.xlsx"
    file_path = cache_path / filename

    # 检查缓存是否存在且不需要强制刷新
    if file_path.exists() and not force_refresh:
        try:
            logger.info(f"读取缓存文件: {file_path}")
            df = pd.read_excel(file_path)
            logger.info(f"成功从缓存加载数据，数据形状: {df.shape}")
            return df
        except Exception as e:
            logger.error(f"读取缓存文件失败: {e}")
            # 如果缓存文件损坏，继续执行查询

    # 执行数据查询
    try:
        logger.info(f"开始查询数据: {start_time} 到 {end_time}")
        df = query_data_from_source(start_time, end_time)

        if df is not None and not df.empty:
            # 保存到缓存文件
            try:
                df.to_excel(file_path, index=False)
                logger.info(f"数据已缓存到: {file_path}")
            except Exception as e:
                logger.error(f"保存缓存文件失败: {e}")

            return df
        else:
            logger.warning("查询结果为空")
            return None

    except Exception as e:
        logger.error(f"数据查询失败: {e}")
        return None


def query_data_from_source(start_time: str, end_time: str) -> pd.DataFrame:
    """
    实际的数据查询函数 - 需要根据您的具体数据源实现

    Args:
        start_time (str): 开始时间
        end_time (str): 结束时间

    Returns:
        pd.DataFrame: 查询到的数据
    """
    # 这里是示例实现，您需要根据实际的数据源进行修改

    # 示例1: 从数据库查询
    # import sqlite3
    # conn = sqlite3.connect('your_database.db')
    # query = f"SELECT * FROM your_table WHERE timestamp BETWEEN '{start_time}' AND '{end_time}'"
    # df = pd.read_sql_query(query, conn)
    # conn.close()

    # 示例2: 从API获取数据
    # import requests
    # response = requests.get(f"your_api_url?start={start_time}&end={end_time}")
    # data = response.json()
    # df = pd.DataFrame(data)

    # 示例3: 模拟数据（仅供测试）
    import datetime
    import random

    # 生成示例数据
    date_range = pd.date_range(start=start_time, end=end_time, freq='D')
    data = {
        'date': date_range,
        'value': [random.randint(100, 1000) for _ in range(len(date_range))],
        'category': [random.choice(['A', 'B', 'C']) for _ in range(len(date_range))]
    }
    df = pd.DataFrame(data)

    logger.info(f"成功查询到数据，形状: {df.shape}")
    return df


def clear_cache(cache_dir: str = "./cache", pattern: str = None):
    """
    清除缓存文件

    Args:
        cache_dir (str): 缓存目录路径
        pattern (str): 文件名模式，用于选择性删除
    """
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        logger.info("缓存目录不存在")
        return

    files_to_delete = []
    if pattern:
        files_to_delete = list(cache_path.glob(f"*{pattern}*"))
    else:
        files_to_delete = list(cache_path.glob("*.xlsx"))

    for file_path in files_to_delete:
        try:
            file_path.unlink()
            logger.info(f"已删除缓存文件: {file_path}")
        except Exception as e:
            logger.error(f"删除缓存文件失败 {file_path}: {e}")


def list_cache_files(cache_dir: str = "./cache") -> list:
    """
    列出所有缓存文件

    Args:
        cache_dir (str): 缓存目录路径

    Returns:
        list: 缓存文件列表
    """
    cache_path = Path(cache_dir)
    if not cache_path.exists():
        return []

    return [f.name for f in cache_path.glob("*.xlsx")]


# 使用示例
if __name__ == "__main__":
    # 示例使用
    start_time = "2024-01-01"
    end_time = "2024-01-10"

    # 第一次查询（会执行实际查询并缓存）
    print("第一次查询（会缓存数据）:")
    df1 = query_data_with_cache(start_time, end_time)
    print(f"数据形状: {df1.shape if df1 is not None else '无数据'}")

    # 第二次查询（会从缓存读取）
    print("\n第二次查询（从缓存读取）:")
    df2 = query_data_with_cache(start_time, end_time)
    print(f"数据形状: {df2.shape if df2 is not None else '无数据'}")

    # 强制刷新查询
    print("\n强制刷新查询:")
    df3 = query_data_with_cache(start_time, end_time, force_refresh=True)
    print(f"数据形状: {df3.shape if df3 is not None else '无数据'}")

    # 列出缓存文件
    print(f"\n缓存文件列表: {list_cache_files()}")

    # 清除特定模式的缓存（可选）
    # clear_cache(pattern="2024-01-01")