import os
from datetime import datetime

from sqlalchemy import text
from utils.logger import app_logger
import pandas as pd
from utils.sftp_uploader import upload_sftp
from utils.app_config import app_config


def get_new_price_tag(db, org_id):
    """
    获取价格标签数据
    """
    query = text("""
        SELECT a.promotion_id,a.org_id,
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, 1, CHARINDEX('-', part_number) - 1)  else '' end AS material,
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, CHARINDEX('-', part_number) + 1, LEN(part_number)) else '' end AS grid,
        f.MANUFACTURER_UPC as SKU,
        c.item_id as EAN,
        d.discount_value as PRICE,
        a.start_date,
        a.end_date
        from promotions a 
        INNER JOIN promotions_item_segments b on a.promotion_id=b.promotion_id
        INNER JOIN segments_item_detail c on b.segment_id=c.segment_id
        INNER JOIN promotions_result d on a.promotion_id=d.promotion_id and b.set_id=d.set_id
        INNER JOIN ITM_ITEM_OPTIONS e on e.ITEM_ID=c.item_id
        INNER JOIN ITM_ITEM_CROSS_REFERENCE f on e.ORGANIZATION_ID=f.ORGANIZATION_ID and e.ITEM_ID=f.ITEM_ID
        where 
        a.promotion_status='active' and price_tag=1 and 
        d.discount_type='NEW_PRICE' and a.org_id=:org_id
    """)

    try:
        result = db.execute(query, {"org_id": org_id})
        # result = db.execute(query)
        rows = result.fetchall()

        # 将结果转换为字典列表
        price_tags = []
        for row in rows:
            price_tag = {
                'promotion_id': row[0],
                'org_id': row[1],
                'material': row[2],
                'grid': row[3],
                'SKU': row[4],
                'EAN': row[5],
                'PRICE': row[6],
                'start_date': row[7],
                'end_date': row[8]
            }
            price_tags.append(price_tag)

        return price_tags
    except Exception as e:
        app_logger.error(f"Error executing get_price_tag query: {str(e)}")
        raise e


def get_percent_off_price_tag(db, org_id):
    """
    获取百分比折扣价格标签数据
    """
    query = text("""
        SELECT a.promotion_id,a.org_id,
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, 1, CHARINDEX('-', part_number) - 1)  else '' end AS material,
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, CHARINDEX('-', part_number) + 1, LEN(part_number)) else '' end AS grid,
        f.MANUFACTURER_UPC as SKU,
        c.item_id as EAN,
        (1-d.discount_value/100)*g.price as price,
        a.start_date,
        a.end_date
        from promotions a 
        INNER JOIN promotions_item_segments b on a.promotion_id=b.promotion_id
        INNER JOIN segments_item_detail c on b.segment_id=c.segment_id
        INNER JOIN promotions_result d on a.promotion_id=d.promotion_id and b.set_id=d.set_id
        INNER JOIN ITM_ITEM_OPTIONS e on e.ITEM_ID=c.item_id
        INNER JOIN ITM_ITEM_CROSS_REFERENCE f on e.ORGANIZATION_ID=f.ORGANIZATION_ID and e.ITEM_ID=f.ITEM_ID
        INNER JOIN itm_item_prices g on e.ORGANIZATION_ID=g.organization_id  and g.ITEM_ID=f.ITEM_ID and g.itm_price_property_code='REGULAR_PRICE' and a.org_id=g.level_value
        and (a.start_date BETWEEN g.effective_date and expiration_date or a.end_date BETWEEN g.effective_date and expiration_date )
        where 
        a.promotion_status='active' and  price_tag=1 and 
        d.discount_type='PERCENT_OFF' and a.org_id=:org_id
    """)

    try:
        result = db.execute(query, {"org_id": org_id})
        rows = result.fetchall()

        # 将结果转换为字典列表
        price_tags = []
        for row in rows:
            price_tag = {
                'promotion_id': row[0],
                'org_id': row[1],
                'material': row[2],
                'grid': row[3],
                'SKU': row[4],
                'EAN': row[5],
                'PRICE': row[6],
                'start_date': row[7],
                'end_date': row[8]
            }
            price_tags.append(price_tag)

        return price_tags
    except Exception as e:
        app_logger.error(f"Error executing get_percent_off_price_tag query: {str(e)}")
        raise e


def get_regular_price_tag(db, org_id):

    query = text("""
        SELECT  
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, 1, CHARINDEX('-', part_number) - 1)  else '' end AS material,
        case  when part_number like '%-%'  then 
        SUBSTRING(part_number, CHARINDEX('-', part_number) + 1, LEN(part_number)) else '' end AS grid,
        f.MANUFACTURER_UPC as SKU,
        g.item_id as EAN,
        g.price,
        g.effective_date,
        g.expiration_date
        from itm_item_prices g 
        INNER JOIN
        ITM_ITEM_CROSS_REFERENCE f on g.organization_id=f.ORGANIZATION_ID and g.ITEM_ID=f.ITEM_ID
        INNER JOIN 
        ITM_ITEM_OPTIONS e on g.organization_id=e.ORGANIZATION_ID and g.ITEM_ID=e.ITEM_ID
        where g.level_value=:org_id and itm_price_property_code='REGULAR_PRICE'
    """)

    try:
        result = db.execute(query, {"org_id": str(org_id)})
        rows = result.fetchall()

        # 将结果转换为字典列表
        price_tags = []
        for row in rows:
            price_tag = {
                'material': row[0],
                'grid': row[1],
                'SKU': row[2],
                'EAN': row[3],
                'PRICE': row[4],
                'start_date': row[5],
                'end_date': row[6]
            }
            price_tags.append(price_tag)

        return price_tags
    except Exception as e:
        app_logger.error(f"Error executing get_regular_price_tag query: {str(e)}")
        raise e

def generate_and_upload_price_tags(db, org_id, REMOTE_BASE_PATH,CURRENCY):
    """
    生成价格标签 Excel 文件并上传到 SFTP

    功能：
    1. 从数据库获取价格标签数据
    2. 生成带时间戳的 Excel 文件
    3. 保存到指定目录
    4. 上传到 SFTP 服务器

    Args:
        db: 数据库连接对象

    Returns:
        dict: 包含生成文件路径和上传状态的结果字典
    """
    try:
        # 确保输出目录存在
        output_dir = app_config.PT_PATH
        os.makedirs(output_dir, exist_ok=True)

        # 生成带时间戳的文件名
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'price_tag_{timestamp}.xlsx'
        output_path = os.path.join(output_dir, filename)



        # 获取两种类型的价格标签数据
        new_price_tags = get_new_price_tag(db, org_id)
        percent_off_price_tags = get_percent_off_price_tag(db, org_id)
        regular_price_tags = get_regular_price_tag(db, org_id)
        # 合并数据
        df_new = pd.DataFrame(new_price_tags)
        if not df_new.empty:
            df_new['PRICE TYPE'] = 'P1'
            df_new['REMARKS'] = 'Discount item'

        df_percent = pd.DataFrame(percent_off_price_tags)
        if not df_percent.empty:
            df_percent['PRICE TYPE'] = 'P1'
            df_percent['REMARKS'] = 'Discount item'

        df_regular = pd.DataFrame(regular_price_tags)
        if not df_regular.empty:
            df_regular['PRICE TYPE'] = 'P0'
            df_regular['REMARKS'] = ''

        # 合并所有数据
        dfs = [df for df in [df_new, df_percent, df_regular] if not df.empty]
        if not dfs:
            app_logger.warning("No price tag data found")
            return {
                'success': False,
                'message': 'No price tag data found',
                'file_path': None,
                'uploaded': False
            }

        df = pd.concat(dfs, ignore_index=True)

        app_logger.info(f"Retrieved ORG:{org_id} Count of {len(df)} price tag records")
        # 转换为 DataFrame
        # df = pd.DataFrame(all_price_tags)
        initial_count = len(df)
        df = df.drop_duplicates(subset=['promotion_id', 'org_id', 'material', 'grid', 'SKU', 'EAN', 'PRICE'])
        deduplicated_count = len(df)

        if initial_count != deduplicated_count:
            app_logger.info(f"Removed {initial_count - deduplicated_count} duplicate records")

        app_logger.info(f"Total {deduplicated_count} price tag records after deduplication")

        # 添加缺失的字段
        # df['PRICE TYPE'] = 'P1'
        df['CUSTOMER CODE'] = ''
        # df['PRODUCT PRICE'] = df['PRICE']
        df['PRICE UNIT'] = 1
        df['UOM'] = 'PC'
        df['CURRENCY'] = CURRENCY


        # 重命名列以匹配 Excel 格式
        column_mapping = {
            'material': 'MATERIAL NUMBER',
            'grid': 'GRID',
            'SKU': 'SKU',
            'EAN': 'EAN',
            'PRICE': 'PRODUCT PRICE',
            'start_date': 'VALID FROM',
            'end_date': 'VALID TO'
        }

        df = df.rename(columns=column_mapping)

        def safe_date_format(date_value):
            """安全地格式化日期，处理超出范围的日期"""
            if pd.isna(date_value):
                return '9999-12-31'
            try:
                dt = pd.to_datetime(date_value)
                # 检查是否超出合理范围（例如年份大于 9999）
                if dt.year > 9999:
                    return '9999-12-31'
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError, OverflowError):
                # 如果转换失败，返回原始值或空字符串
                return str(date_value) if date_value else ''

        df['VALID FROM'] = df['VALID FROM'].apply(safe_date_format)
        df['VALID TO'] = df['VALID TO'].apply(safe_date_format)
        # 确保日期格式正确
        # df['VALID FROM'] = pd.to_datetime(df['VALID FROM']).dt.strftime('%Y-%m-%d')
        # df['VALID TO'] = pd.to_datetime(df['VALID TO']).dt.strftime('%Y-%m-%d')

        # 设置列顺序
        columns_order = ['PRICE TYPE', 'CUSTOMER CODE', 'MATERIAL NUMBER', 'GRID', 'SKU', 'EAN',
                         'PRODUCT PRICE', 'PRICE UNIT', 'UOM', 'CURRENCY', 'VALID FROM', 'VALID TO', 'REMARKS']

        df = df[columns_order]

        app_logger.info(f"Starting to generate price tags file: {output_path}")
        # 写入 Excel 文件
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Price Tags')

        app_logger.info(f"Price tags written to {output_path}, total records: {len(df)}")

        # 上传到 SFTP
        uploaded = False
        try:
            uploaded = upload_sftp(output_path, filename, 'DEFAULT', REMOTE_BASE_PATH)
            app_logger.info(f"File {'successfully' if uploaded else 'failed to'} upload to SFTP")
        except Exception as upload_error:
            app_logger.error(f"SFTP upload failed: {str(upload_error)}")

        return {
            'success': True,
            'message': 'Price tags generated successfully',
            'file_path': output_path,
            'filename': filename,
            'record_count': len(df),
            'uploaded': uploaded
        }

    except Exception as e:
        app_logger.error(f"Failed to generate price tags: {str(e)}", exc_info=True)
        return {
            'success': False,
            'message': f'Failed to generate price tags: {str(e)}',
            'file_path': None,
            'uploaded': False
        }


def generate_and_upload_price_tags_for_all_orgs(db):
    try:
        # 从配置中获取 TAG 配置
        sftp_config = app_config.get_sftp_config('TAG')
        app_logger.info(f"TAG configuration: {sftp_config}")

        if not sftp_config or not isinstance(sftp_config, list):
            app_logger.warning("No TAG configuration found in SFTP_CONFIG")
            return {
                'success': False,
                'message': 'No TAG configuration found',
                'results': []
            }

        app_logger.info(f"Found {len(sftp_config)} organizations in TAG configuration")

        results = []
        success_count = 0
        fail_count = 0
        total_records = 0

        # 循环处理每个组织
        for org_config in sftp_config:
            org_id = org_config.get('ORG_ID')
            REMOTE_BASE_PATH = org_config.get('REMOTE_BASE_PATH')
            CURRENCY=org_config.get('CURRENCY')

            if not org_id:
                app_logger.warning("Organization ID not found in TAG config, skipping")
                continue

            app_logger.info(f"Processing price tags for organization: {org_id}")

            try:
                result = generate_and_upload_price_tags(db, org_id, REMOTE_BASE_PATH,CURRENCY)
                results.append(result)

                if result.get('success'):
                    success_count += 1
                    total_records += result.get('record_count', 0)
                else:
                    fail_count += 1

            except Exception as e:
                app_logger.error(f"Error processing org {org_id}: {str(e)}", exc_info=True)
                fail_count += 1
                results.append({
                    'success': False,
                    'message': f'Error: {str(e)}',
                    'org_id': org_id,
                    'file_path': None,
                    'uploaded': False
                })

        app_logger.info(
            f"Price tag generation completed. Success: {success_count}, "
            f"Failed: {fail_count}, Total records: {total_records}"
        )

        return {
            'success': success_count > 0,
            'message': f'Processed {len(sftp_config)} organizations: {success_count} succeeded, {fail_count} failed',
            'results': results,
            'summary': {
                'total_orgs': len(sftp_config),
                'success_count': success_count,
                'fail_count': fail_count,
                'total_records': total_records
            }
        }

    except Exception as e:
        app_logger.error(f"Failed to process price tags for all organizations: {str(e)}", exc_info=True)
        return {
            'success': False,
            'message': f'Failed to process all organizations: {str(e)}',
            'results': []
        }
