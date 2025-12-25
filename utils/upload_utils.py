# utils/file_utils.py

import pandas as pd
import io
from fastapi import UploadFile
from utils.logger import app_logger
import pandas as pd



ITEM_ID_ALIASES = {
    'itemid', 'item ID', 'item-id', 'itemID', 'item', 'ITEM_ID'
}

Location_ID_ALIASES = {
    'Location', 'Location-ID', 'LocationID', 'rtl_loc_id', 'Location_ID', 'location_id', 'RTL_LOC_ID', 'Location Id'
}

CUST_ID_ALIASES = {
    'phone'
}


def standardize_columns(df):
    item_id_aliases = {alias.lower() for alias in ITEM_ID_ALIASES}
    location_id_aliases = {alias.lower() for alias in Location_ID_ALIASES}
    cust_id_aliases = {alias.lower() for alias in CUST_ID_ALIASES}

    standardized = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in item_id_aliases:
            standardized[col] = 'item_id'
        elif col_lower in location_id_aliases:
            standardized[col] = 'rtl_loc_id'
        elif col_lower in cust_id_aliases:
            standardized[col] = 'cust_phone'
        else:
            standardized[col] = col
    return df.rename(columns=standardized)

async def _clean_and_standardize_data(df):
    """清理和标准化数据"""
    # 标准化列名
    original_columns = df.columns.tolist()
    df = standardize_columns(df)

    # 检查必需列是否存在
    required_columns = ['item_id', 'price']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    # 清理数据
    initial_count = len(df)
    df = df.dropna(subset=['item_id', 'price'])

    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df = df.dropna(subset=['price'])

    if len(df) == 0:
        raise ValueError("No valid data found in the uploaded file.")

    return df

async def validate_and_read_file(uFile: UploadFile) -> pd.DataFrame:

    max_file_size = 10 * 1024 * 1024  # 10MB
    if uFile.size > max_file_size:
        raise ValueError("File size exceeds the maximum allowed limit.")

    contents = await uFile.read()
    file_name = uFile.filename

    # 检查文件是否为空
    if not contents:
        raise ValueError("Uploaded file is empty.")

    # 读取Excel文件
    try:
        df = pd.read_excel(io.BytesIO(contents), dtype=str)
        app_logger.info(f"Excel file parsed successfully, shape: {df.shape}")
    except pd.errors.ParserError as e:
        raise ValueError(f"Failed to parse Excel file: {str(e)}")
    except Exception as e:
        raise ValueError(f"Failed to parse Excel file: {str(e)}")

    if df.empty:
        raise ValueError("Uploaded file is empty (no data)")

    return df
