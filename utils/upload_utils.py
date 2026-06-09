# utils/file_utils.py

import io
from fastapi import UploadFile
from utils.logger import app_logger
import pandas as pd

from utils.segment_etl import load_item_data_from_db
from utils.translator import get_message

ITEM_ID_ALIASES = {'ITEM', 'ITEM_ID', 'SKU', 'EAN'}

Location_ID_ALIASES = {
    'Location', 'Location-ID', 'LocationID', 'rtl_loc_id', 'Location_ID', 'location_id', 'RTL_LOC_ID', 'Location Id'
}

CUST_ID_ALIASES = {
    'phone'
}
price_aliases = {'price', 'sale_price', 'new_price'}


def standardize_columns(df, lang='en'):
    item_id_aliases = {alias.lower() for alias in ITEM_ID_ALIASES}
    location_id_aliases = {alias.lower() for alias in Location_ID_ALIASES}
    cust_id_aliases = {alias.lower() for alias in CUST_ID_ALIASES}

    standardized = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in item_id_aliases:
            if col_lower == 'sku':
                standardized[col] = 'sku'
            else:
                standardized[col] = 'item_id'
        elif col_lower in location_id_aliases:
            standardized[col] = 'rtl_loc_id'
        elif col_lower in cust_id_aliases:
            standardized[col] = 'cust_phone'
        elif col_lower in price_aliases:
            standardized[col] = 'price'
        else:
            raise ValueError(get_message("no_valid_file", lang))
    return df.rename(columns=standardized)


async def _clean_and_standardize_data(df, org_id, lang='en'):
    """清理和标准化数据"""
    # 标准化列名
    # original_columns = df.columns.tolist()
    df = standardize_columns(df,lang)

    df = await validate_and_enrich_upload_data(df, 'item', org_id)
    # 检查必需列是否存在
    required_columns = ['item_id', 'price']
    df_columns_lower = {col.lower(): col for col in df.columns}
    missing_columns = [col for col in required_columns if col.lower() not in df_columns_lower]
    if missing_columns:
        raise ValueError(get_message("missing_columns", lang, columns='Price OR (ITEM_ID, SKU, EAN)'))

    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['price'] = df['price'].replace([float('inf'), float('-inf')], None)
    df = df.dropna(subset=['price'])
    df['price'] = df['price'].astype(float)

    app_logger.info(f"Total {df.shape[0]} item price records after cleaning")
    # if len(valid_data) == 0:
    #     raise ValueError(get_message("no_valid_data", lang))

    return df.fillna('')


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


async def validate_and_enrich_upload_data(upload_data: pd.DataFrame, segment_type: str,
                                          org_id: str = None) -> pd.DataFrame:
    """
    验证上传数据并从数据库补充字段信息，标记错误数据

    Args:
        upload_data: 上传的DataFrame数据
        segment_type: 分群类型 ('item', 'location', 'customer')
        org_id: 组织ID

    Returns:
        经过字段补充和错误标记的DataFrame
    """
    if upload_data.empty:
        return upload_data

    # 从数据库加载参考数据
    db_data = load_item_data_from_db(segment_type, org_id)

    if segment_type == 'item':
        # 确定主键列
        if 'item_id' in upload_data.columns:
            key_column = 'item_id'
        elif 'sku' in upload_data.columns:
            key_column = 'sku'
        else:
            app_logger.warning(f"No valid key column found for item segment")
            upload_data['error_flag'] = 1
            upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')
            return upload_data

        # 筛选并重命名数据库字段
        db_data_filtered = db_data[['item_id', 'name', 'sku', 'description', 'merch_level_1']].rename(
            columns={
                'name': 'item_name',
                'description': 'item_description',
                'merch_level_1': 'item_department'
            }
        )

        # 去重
        upload_data = upload_data.drop_duplicates(subset=key_column, keep='first')

        # 左连接补充字段
        upload_data = upload_data.merge(db_data_filtered, on=key_column, how='left')

        upload_data = upload_data.drop_duplicates(subset=key_column, keep='first')
        # 初始化错误标记
        upload_data['error_flag'] = 0
        upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')

        # 标记不存在于数据库中的记录
        upload_data[key_column] = upload_data[key_column].astype(str)
        missing_items = ~upload_data[key_column].isin(db_data[key_column].astype(str))
        upload_data.loc[missing_items, 'error_flag'] = 1

    elif segment_type == 'location':
        # 筛选数据库字段
        loc_data_filtered = db_data[['rtl_loc_id', 'store_name', 'location_type', 'city']]

        # 去重
        upload_data = upload_data.drop_duplicates(subset='rtl_loc_id', keep='first')

        # 转换数据类型
        upload_data['rtl_loc_id'] = upload_data['rtl_loc_id'].astype('Int64')

        # 左连接补充字段
        upload_data = upload_data.merge(loc_data_filtered, on='rtl_loc_id', how='left')

        # 初始化错误标记
        upload_data['error_flag'] = 0
        upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')

        # 标记不存在于数据库中的记录
        missing_items = ~upload_data['rtl_loc_id'].isin(loc_data_filtered['rtl_loc_id'])
        upload_data.loc[missing_items, 'error_flag'] = 1

    elif segment_type == 'customer':
        # 客户分群暂时不做数据库验证，只初始化错误标记
        upload_data['error_flag'] = 0
        upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')

    else:
        app_logger.warning(f"Unsupported segment type: {segment_type}")
        upload_data['error_flag'] = 0
        upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')

    return upload_data
