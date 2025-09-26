from enum import Enum
import yaml
from fastapi import FastAPI, Depends, Query, UploadFile, File, HTTPException, status
from starlette.responses import StreamingResponse

import schemas
from service.mnt_generate import generate_deal_insert, generate_deal_item_insert, generate_deal_item_test_insert, \
    generate_deal_trigger_insert, generate_deal_coupon_xref_insert
from utils.sftp_uploader import upload_mnt_file
from worker_api.api import router as worker_api_router
from routers.user import router as user_api_router
import service
from models.model import SegmentsItem, SegmentsItemDetail, SegmentsCustomer, SegmentsCustomerDetail, SegmentsLocation, \
    SegmentsLocationDetail
from schemas import SegmentSubmit, PromotionSubmit
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
import pandas as pd
import io
import os
import asyncio
from sqlalchemy import text
from typing import List

from service.segments_service \
    import create_segment_item, delete_segment_item, update_segment_item, \
    create_segment_item_condition, delete_segment_item_condition, \
    get_segments_item_list, get_segments_location_list, \
    get_item_segment_by_id, get_item_segment_condition_by_id, \
    get_item_segment_schedule_by_id, \
    get_item_segment_by_name, \
    create_segment_schedule, delete_segment_schedule, update_segment_item_status, get_segments_item_detail, \
    update_segment_location, create_segment_location, delete_segment_location_condition, \
    create_segment_location_condition, create_segment_customer_condition, create_segment_customer, \
    delete_segment_customer_condition, update_segment_customer, get_setgments_location_detail, \
    get_location_segment_by_id, get_location_segment_condition_by_id, get_customer_segment_by_id, \
    get_customer_segment_condition_by_id, get_segments_customer_list, update_segment_location_status, \
    update_segment_customer_status, get_segments_customer_detail, create_item_segments_detail, \
    delete_segment_item_detail, delete_segment_import, create_segment_import, get_segment_import_by_id, \
    update_segment_some, delete_segment_customer_detail, delete_segment_location_detail, \
    get_customer_segment_by_name, get_location_segment_by_name, create_customer_segments_detail, \
    create_location_segments_detail, get_item_segments_in_use_by_id, delete_segment_location, delete_segment_customer, \
    get_location_segments_in_use_by_id, get_customer_segments_in_use_by_id, get_store_list, process_segment_data

from service.promotion import create_promotion, create_promotion_condition, create_promotion_result, \
    create_promotion_item_segments, create_promotion_location_segments, create_promotion_customer_segments, \
    update_promotion, delete_promotion_item_segments, get_promotion_list, \
    get_promotion_by_id, get_promotion_condition_by_id, get_promotion_result_by_id, \
    get_promotion_customer_segments_by_id, get_promotion_item_segments_by_id, \
    get_promotion_location_segments_by_id, delete_promotion_customer_segments, delete_promotion_location_segments, \
    update_promotion_result, update_promotion_status, update_promotion_condition, get_promotion_location_detail_by_id, \
    update_promotion_export_time, delete_promotion, create_promotion_org_data, get_promotion_org_join_by_id, \
    get_promotion_location_detail_by_id_v2, process_promotion_data

from service.worker import create_worker_task
from service.access_service import verify_password
from utils.segment_etl import run_segment_cleaning
from service import get_db

app = FastAPI(
    title="promotion_api",
    description="promotion_api"
)

import logging
from utils.logger import setup_logger

logger = setup_logger(__name__, log_file="./logs/promotion.log", level=logging.INFO)

# # 创建后台调度器
# def start_scheduler():
#     from apscheduler.schedulers.background import BackgroundScheduler
#     scheduler = BackgroundScheduler()
#     scheduler.add_job(run_segment_cleaning, 'interval', minutes=1, args=[20006, 'and'])
#     scheduler.start()
#     print("Scheduler started.")
#
#     # 应用退出时关闭调度器
#     import atexit
#     @atexit.register
#     def on_shutdown():
#         scheduler.shutdown()
#         print("Scheduler stopped.")

# 显式调用同步数据库初始化函数

file = open('config/segments_condition.yaml', 'r', encoding='utf-8')
dict_condition = yaml.safe_load(file)

config_file = open('config/config.yaml', 'r', encoding='utf-8')
dict_config = yaml.safe_load(config_file)

directory = dict_config['MNT_PATH']
os.makedirs(directory, exist_ok=True)

Export_Type = dict_config['Export_Type']

app.include_router(worker_api_router, prefix="/worker")
app.include_router(user_api_router, prefix="/user_api")

PROMOTION_TABLES = dict_config['PROMOTION_TABLES']

#
# file_handler = TimedRotatingFileHandler('./promotion.log', when='midnight', interval=1, backupCount=7, encoding='utf-8')
# file_handler.suffix = "%Y-%m-%d"
# file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
#
# logger = logging.getLogger()
# logger.setLevel(logging.INFO)
# logger.addHandler(file_handler)

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 720

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="promotion_api/token")


class Segment_Type(Enum):
    item = "item"
    location = "location"
    customer = "customer"


class Segment_Status(Enum):
    active = "active"
    inactive = "inactive"


class Data_Status(Enum):
    ALL = "ALL"
    active = "active"
    inactive = "inactive"


class Promotion_Type(Enum):
    Product = "Product"
    Coupon = "Coupon"


# @asynccontextmanager
# async def get_db():
#     db = await service.create_async_session()
#     try:
#         yield db
#     finally:
#         await db.close()


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    to_encode.update({"exp": datetime.utcnow() + expires_delta})
    encoded_jwt = jwt.encode(to_encode, dict_config['SECRET_KEY'], algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # 示例中没有真正解码 JWT，根据实际业务实现
        payload = jwt.decode(token, dict_config['SECRET_KEY'], algorithms=[ALGORITHM])
        userid: str = payload.get("sub")

        if userid is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return userid


@app.post("/promotion_api/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), session=Depends(get_db)):
    user = await authenticate_user(form_data.username, form_data.password, session)
    if not user:
        # raise HTTPException(
        #     status_code=status.HTTP_401_UNAUTHORIZED,
        #     detail="Incorrect username or password",
        #     headers={"WWW-Authenticate": "Bearer"},
        # )
        return {"code": 301, "msg": "Incorrect username or password"}
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user['username']}, expires_delta=access_token_expires
    )
    return {"code": 200, "access_token": access_token, "token_type": "bearer"}


async def authenticate_user(username: str, password: str, session):
    if await verify_password(session, username, password):
        return {"username": username}
    return None


async def get_location_detail_by_promotionId(promotion_id: int, session=Depends(get_db)) -> dict:
    res_location = await get_promotion_location_detail_by_id(session, promotion_id)
    df_locs = pd.DataFrame(res_location)
    if df_locs.empty:
        res_location = await get_promotion_location_detail_by_id_v2(session, promotion_id)
        df_locs = pd.DataFrame(res_location)
        data_type = "hierarchy"
    else:
        excluded_locs = df_locs[df_locs['include'] == 0]['rtl_loc_id'].unique()
        data_type = "segment"

        df_locs = df_locs[~df_locs['rtl_loc_id'].isin(excluded_locs)]
    return {"data": df_locs, "data_type": data_type}


@app.get("/promotion_api/segments_condition")
async def read_segments_condition(segment_type: Segment_Type, session=Depends(get_db),
                                  user_id=Depends(get_current_user)):
    """
    获取指定类型的分段条件配置。

    Args:
        Segment_Type (Segment_Type_Enum): 分段类型，只能是 'item', 'location', 'customer'。

    Returns:
        dict: 分段条件配置字典。
        :param user_id:
        :param segment_type:
    """
    condition_list = dict_condition.get(segment_type.value, [])

    updated_conditions = []

    for condition in condition_list:
        if condition.get("value_type") == "SQL":
            sql_query = condition["condition_value"][0]
            try:
                condition["value_type"] = "LIST"
                result = session.execute(text(sql_query))
                values = [{'k': row[0], 'v': row[1]} for row in result.fetchall()]
                condition["condition_value"] = values
            except Exception as e:
                logging.error(f"Error executing SQL: {sql_query}, Error: {e}")
                condition["condition_value"] = []  # 出错时设为空列表
        updated_conditions.append(condition)

    return updated_conditions


def generate_item_mnt_file(segment_id, item_list,
                           org, ORG_ID='1', segment_status='active'):
    """
    生成MNT文件
    """
    file_name = '{0}_PROP_DEAL_{1}.mnt'.format(segment_id, datetime.now().strftime('%Y%m%d%H%M%S'))
    deployment_name = '{0}_PROP_DEAL_{1}'.format(segment_id, datetime.now().strftime('%Y%m%d%H%M%S'))
    td = datetime.now().strftime('%Y-%m-%d')
    file_path = os.path.join(directory, file_name)

    end_date = '2099-01-01 23:59:59'

    try:
        with open(file_path, 'w') as mnt_file:
            mnt_file.write(
                f'<Header application_date="{td}" apply_immediately="TRUE" deployment_name="{deployment_name}" download_id="{deployment_name}" download_time="IMMEDIATE" target_org_node="{org}" />\n')
            mnt_file.write(
                f"BEGIN_RUN_SQL|DELETE FROM ITM_ITEM_DEAL_PROP  where organization_id = {ORG_ID} and itm_deal_property_code ='ITM_PROP_{segment_id}' and string_value= 'TRUE'\n")
            if segment_status == 'active':
                for item_data in item_list:
                    for item in item_data['data']:
                        mnt_file.write(
                            f"INSERT|ITEM_DEAL_PROPERTY|{item.get('item_id','')}|{item.get('itm_deal_property_code','')}||{item.get('begin_date','')}|{end_date}|STRING|TRUE|||*|*\n")
        return True
    except Exception as e:
        logging.error("Error writing MNT file: {}".format(repr(e)))
        return False


@app.get("/promotion_api/segments/export_segments")
async def export_segments(segment_type: Segment_Type, segment_id: int,
                          store_ids: str = Query(None, description="逗号分隔的门店ID列表"),
                          session=Depends(get_db),
                          user_id=Depends(get_current_user)):
    """
    导出MNT
    """
    try:
        if store_ids:
            store_list = [loc.strip() for loc in store_ids.split(",")]
        else:
            store_list = None

        item_data = await process_segment_data(segment_id, session)

        if store_list:
            if Export_Type == 'MNT':
                for store in store_list:
                    success = generate_item_mnt_file(segment_id, item_data, f"STORE:{store}")

            if Export_Type == 'WORKER':
                sessionId = await create_worker_task(session, store_list, 'segment_item', segment_id)
                await update_segment_some(segment_type.value, session, segment_id,
                                          {"last_session_id": sessionId, "export_time": datetime.now()})

            msg = "export tag success"
        else:
            msg = "No Store data export"

        return {"code": 200, "msg": msg}
    except Exception as e:
        logging.error("export_tag {}".format(repr(e)))
        return {"code": 301, "msg": "export tag error {0}".format(repr(e))}


@app.get("/promotion_api/promotion/export_promotion")
async def export_promotion(promotion_id: int, session=Depends(get_db), user_id=Depends(get_current_user)):
    """
    导出MNT
    """
    export_date = datetime.now()

    try:

        locs_data = await get_location_detail_by_promotionId(promotion_id, session)
        df_locs = locs_data['data']
        if df_locs.empty:
            return {"code": 300, "msg": "Promotion No matching location data available for export"}

        res_segments = await get_promotion_item_segments_by_id(session, promotion_id)

        if Export_Type == 'MNT':
            if locs_data['data_type'] == 'hierarchy':
                promotion_org_join = await get_promotion_org_join_by_id(session, promotion_id)
            else:
                promotion_org_join = ['STORE:' + str(rtl_loc_id) for rtl_loc_id in df_locs['rtl_loc_id']]
            data_detail = await process_promotion_data(promotion_id, session, 1)
            for org in promotion_org_join:
                file_name = '{0}_Promotion_{1}.mnt'.format(promotion_id, datetime.now().strftime('%Y%m%d%H%M%S'))
                deployment_name = '{0}_Promotion_{1}'.format(promotion_id, datetime.now().strftime('%Y%m%d%H%M%S'))
                td = datetime.now().strftime('%Y-%m-%d')
                file_path = os.path.join(directory, file_name)

                with open(file_path, 'w', encoding='utf-8') as mnt_file:

                    mnt_file.write(
                        f'<Header application_date="{td}" apply_immediately="TRUE" deployment_name="{deployment_name}" download_id="{deployment_name}" download_time="IMMEDIATE" target_org_node="{org}" />\n')

                    for t in PROMOTION_TABLES:
                        mnt_file.write(
                            f"BEGIN_RUN_SQL|DELETE FROM {t}  where DEAL_ID ='{promotion_id}' \n")

                    deals: List[str] = []
                    for table in data_detail:
                        table_name = table.get("table")
                        table_data = table.get("data", [])

                        if table_name == "PRC_DEAL":
                            for line in table_data:
                                deals.append(generate_deal_insert(promotion_id, line))
                        elif table_name == "PRC_DEAL_ITEM":
                            for line in table_data:
                                deals.append(generate_deal_item_insert(promotion_id, line))
                        elif table_name == "PRC_DEAL_FIELD_TEST":
                            for line in table_data:
                                deals.append(generate_deal_item_test_insert(promotion_id, line))
                        elif table_name == "PRC_DEAL_TRIG":
                            for line in table_data:
                                deals.append(generate_deal_trigger_insert(promotion_id, line))
                        elif table_name == "DSC_COUPON_XREF":
                            for line in table_data:
                                deals.append(generate_deal_coupon_xref_insert(promotion_id, line))
                    if deals:
                        mnt_file.write("".join(deals))
                upload_success = upload_mnt_file(file_path, file_name)
                for segment in res_segments:
                    item_data = await process_segment_data(segment['segment_id'], session)
                    success = generate_item_mnt_file(segment['segment_id'], item_data, org)
        if Export_Type == 'WORKER':

            for segment in res_segments:
                sessionId = await create_worker_task(session, df_locs['rtl_loc_id'].tolist(), 'segment_item',
                                                     segment['segment_id'])
                await update_segment_some(Segment_Type.item.value, session, segment['segment_id'],
                                          {"last_session_id": sessionId, "export_time": datetime.now()})
            sessionId = await create_worker_task(session, df_locs['rtl_loc_id'].tolist(), 'promotion', promotion_id)
            await update_promotion_export_time(session, promotion_id, export_date, sessionId)
        return {"code": 200, "msg": "export success"}
    except Exception as e:
        logging.error("export_tag {}".format(repr(e)))
        return {"code": 301, "msg": "export tag error {0}".format(repr(e))}


@app.get("/promotion_api/segments/download_segments")
async def download_segments(
        segment_type: Segment_Type,
        segment_id: int,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    下载指定Segment的数据为Excel文件

    Args:
        segment_type: Segment类型(item/location/customer)
        segment_id: Segment ID
        session: 数据库会话
        user_id: 当前用户ID

    Returns:
        StreamingResponse: Excel文件下载流
    """
    try:

        if segment_type == Segment_Type.item:
            segments_detail = await get_segments_item_detail(session, segment_id, None, 1, -1)
            filename = f"item_segment_{segment_id}.xlsx"
            sheet_name = "Items"
            column_name = 'item_id'
        elif segment_type == Segment_Type.location:
            segments_detail = await get_setgments_location_detail(session, segment_id, None, 1, -1)
            filename = f"location_segment_{segment_id}.xlsx"
            sheet_name = "Locations"
            column_name = "rtl_loc_id"
        elif segment_type == Segment_Type.customer:
            segments_detail = await get_segments_customer_detail(session, segment_id, None, 1, -1)
            filename = f"customer_segment_{segment_id}.xlsx"
            sheet_name = "Customers"
            column_name = "cust_phone"
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        # 检查是否有数据
        if not segments_detail or segments_detail['total'] == 0:
            return {'code': 300, "msg": "No data found for this segment."}

        output = io.BytesIO()

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            worksheet = writer.book.add_worksheet()
            worksheet.write(0, 0, column_name)  # 写入表头

            # 写入每一行的 item_id
            for row_num, data in enumerate(segments_detail['data']):
                worksheet.write(row_num + 1, 0,
                                getattr(data, column_name, '') if hasattr(data, column_name) else data.get(column_name,
                                                                                                           ''))

        output.seek(0)

        # 创建并返回文件下载流
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logging.error(f"Error downloading segment data: {str(e)}")
        return {'code': 500, "msg": f"Error downloading data: {str(e)}"}


ITEM_ID_ALIASES = {
    'itemid', 'item ID', 'item-id', 'itemID', 'item', 'ITEM_ID'
}

Location_ID_ALIASES = {
    'Location', 'Location-ID', 'LocationID', 'rtl_loc_id', 'Location_ID', 'location_id', 'RTL_LOC_ID', 'Location Id'
}

CUST_ID_ALIASES = {
    'phone'
}


# 标准化 DataFrame 列名
def standardize_columns(df):
    item_id_aliases = {alias.lower() for alias in ITEM_ID_ALIASES}
    location_id_aliases = {alias.lower() for alias in Location_ID_ALIASES}
    standardized = {}
    for col in df.columns:
        col_lower = col.lower()
        if col_lower in item_id_aliases:
            standardized[col] = 'item_id'
        elif col_lower in location_id_aliases:
            standardized[col] = 'rtl_loc_id'
        elif col_lower in CUST_ID_ALIASES:
            standardized[col] = 'cust_phone'
        else:
            standardized[col] = col
    return df.rename(columns=standardized)


@app.post("/promotion_api/segment/upload_segment")
async def upload_segment(segment_type: Segment_Type, name: str, description: str, segment_id: int = None,
                         uFile: UploadFile = File(...), session=Depends(get_db), user_id=Depends(get_current_user)):
    try:
        # 限制文件大小
        max_file_size = 10 * 1024 * 1024  # 10MB
        if uFile.size > max_file_size:
            return {'code': 302, "msg": "File size exceeds the maximum allowed limit."}

        contents = await uFile.read()
        file_name = uFile.filename

        # 检查文件是否为空
        if not contents:
            return {'code': 303, "msg": "Uploaded file is empty."}

        # 读取Excel文件
        try:
            upload_data = pd.read_excel(io.BytesIO(contents), dtype=str)
        except pd.errors.ParserError:
            logging.error(f"upload_segment: Failed to parse Excel file. User ID: {user_id}, File: {file_name}")
            return {'code': 304, "msg": "Failed to parse Excel file."}

        if upload_data.empty:
            return {'code': 303, "msg": "Uploaded file is empty."}

        upload_data = standardize_columns(upload_data)

        segment_classes = {
            Segment_Type.item: (SegmentsItem, SegmentsItemDetail, get_item_segment_by_name, create_segment_item),
            Segment_Type.customer: (
                SegmentsCustomer, SegmentsCustomerDetail, get_customer_segment_by_name, create_segment_customer),
            Segment_Type.location: (
                SegmentsLocation, SegmentsLocationDetail, get_location_segment_by_name, create_segment_location)
        }

        if segment_type not in segment_classes:
            return {'code': 305, "msg": "Invalid segment type."}

        SegmentClass, DetailClass, get_segment_by_name_func, create_segment_func = segment_classes[segment_type]

        if segment_id:
            delete_detail_func_map = {
                Segment_Type.item: delete_segment_item_detail,
                Segment_Type.customer: delete_segment_customer_detail,
                Segment_Type.location: delete_segment_location_detail
            }
            await delete_detail_func_map[segment_type](session, segment_id)
            insert_segment_id = segment_id
        else:
            segment = SegmentClass(
                name=name,
                description=description,
                sub_count=0,
                segment_status='active',
                create_type='import'
            )
            existing_segment = await get_segment_by_name_func(session, name=segment.name)
            if existing_segment:
                return {'code': 300, "msg": f"{segment_type.value.capitalize()} segment with this name already exists."}
            insert_segment = await create_segment_func(session, segment, user_id)
            insert_segment_id = insert_segment.segment_id

        detail_creation_map = {
            Segment_Type.item: create_item_segments_detail,
            Segment_Type.customer: create_customer_segments_detail,
            Segment_Type.location: create_location_segments_detail
        }

        detail_creator = detail_creation_map[segment_type]
        details = [
            DetailClass(segment_id=insert_segment_id, **row.to_dict())
            for _, row in upload_data.iterrows()
        ]
        await detail_creator(session, insert_segment_id, details)

        update_segment_map = {
            Segment_Type.item: update_segment_item,
            Segment_Type.customer: update_segment_customer,
            Segment_Type.location: update_segment_location
        }

        update_segment_func = update_segment_map[segment_type]
        segment = SegmentClass(**{
            'segment_id': insert_segment_id,
            'name': name,
            'description': description,
            'sub_count': len(details),
            'segment_status': 'active',
            'create_type': 'import'
        })
        await update_segment_func(session, insert_segment_id, segment)

        await delete_segment_import(session, segment_id=insert_segment_id, segment_type=segment_type.value)
        await create_segment_import(session, insert_segment_id, segment_type.value, file_name, len(details))

        return {'code': 200, "segment_id": insert_segment_id, "msg": "Segment submitted successfully."}
    except pd.errors.EmptyDataError:
        logging.error(f"upload_segment: Uploaded file is empty. User ID: {user_id}, File: {file_name}")
        return {'code': 303, "msg": "Uploaded file is empty."}
    except Exception as e:
        logging.error(f"upload_segment: {repr(e)}. User ID: {user_id}, File: {file_name}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/segments/segments_list")
async def read_segments_list(
        segment_type: Segment_Type,
        key_word: str = None,
        segment_status: Data_Status = Data_Status.ALL,
        page: int = Query(1, description="页码", ge=1),
        page_size: int = Query(30, description="每页数量", ge=1, le=100),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    if segment_type == Segment_Type.item:
        return await get_segments_item_list(session, key_word, segment_status.value, page, page_size)
    elif segment_type == Segment_Type.location:
        return await get_segments_location_list(session, key_word, segment_status.value, page, page_size)
    elif segment_type == Segment_Type.customer:
        return await get_segments_customer_list(session, key_word, segment_status.value, page, page_size)
    else:
        return {'code': 300, "msg": "Invalid segment type."}


@app.get("/promotion_api/segments/segments")
async def read_segments(
        segment_type: Segment_Type,
        segment_id: int = Query(None, description="标签ID"),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    if segment_type == Segment_Type.item:
        segments = await get_item_segment_by_id(session, segment_id)
        segments_condition = await get_item_segment_condition_by_id(session, segment_id)
        # segments_detail = await get_item_segment_detail_by_id(session, segment_id)

    elif segment_type == Segment_Type.location:
        segments = await get_location_segment_by_id(session, segment_id)
        segments_condition = await get_location_segment_condition_by_id(session, segment_id)
        # segments_detail = await get_location_segment_detail_by_id(session, segment_id)
    elif segment_type == Segment_Type.customer:
        segments = await get_customer_segment_by_id(session, segment_id)
        segments_condition = await get_customer_segment_condition_by_id(session, segment_id)
    else:
        return {"msg": "Invalid segment type."}
    try:
        segment_schedule = await get_item_segment_schedule_by_id(session, segment_id, segment_type.value)
        segments_import = await get_segment_import_by_id(session, segment_id, segment_type.value)

        if segments is None:
            return {'code': 300, "msg": "Segment not found."}
        else:
            condition_type = segments[0].create_type
            if condition_type == 'import':
                segments_condition = []
                segment_schedule = []
            else:
                segments_import = []

        return {'code': 200,
                'segments': segments,
                'segments_condition': segments_condition,
                # 'segments_detail': segments_detail,
                'segments_import_file': segments_import,
                'segment_schedule': segment_schedule
                }
    except Exception as e:
        logging.error(f"Error Get Segments: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.post("/promotion_api/segments/submit")
async def submit_segments(
        segment: SegmentSubmit,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:

        logger.info(f"Received segment data: {repr(segment.model_dump())}")
        if segment.segment_type.value == Segment_Type.item.value:
            item_segment = segment.segment
            if item_segment.segment_id:
                await update_segment_item(session, item_segment.segment_id, item_segment)
                await delete_segment_item_condition(session, item_segment.segment_id)
                insert_segment_id = item_segment.segment_id
            else:
                existing_segment = await get_item_segment_by_name(session, name=item_segment.name)
                if existing_segment:
                    return {'code': 300, "msg": "Item segment with this name already exists."}
                insert_segment = await create_segment_item(session, item_segment, user_id)
                insert_segment_id = insert_segment.segment_id

            await create_segment_item_condition(session, insert_segment_id, segment.segment_condition)
        elif segment.segment_type == Segment_Type.location.value:
            location_segment = segment.segment
            if location_segment.segment_id:
                insert_segment_id = location_segment.segment_id
                await update_segment_location(session, location_segment.segment_id, location_segment)
                await delete_segment_location_condition(session, location_segment.segment_id)
            else:
                insert_segment = await create_segment_location(session, location_segment, user_id)
                insert_segment_id = insert_segment.segment_id
            await create_segment_location_condition(session, insert_segment_id, segment.segment_condition)
        elif segment.segment_type == Segment_Type.customer.value:
            customer_segment = segment.segment
            if customer_segment.segment_id:
                insert_segment_id = customer_segment.segment_id
                await update_segment_customer(session, customer_segment.segment_id, customer_segment)
                await delete_segment_customer_condition(session, customer_segment.segment_id)
            else:
                insert_segment = await create_segment_customer(session, customer_segment, user_id)
                insert_segment_id = insert_segment.segment_id
            await create_segment_customer_condition(session, insert_segment_id, segment.segment_condition)
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        await delete_segment_schedule(session, insert_segment_id, segment.segment_type.value)
        await create_segment_schedule(session, insert_segment_id, segment.segment_type.value, segment.segment_schedule)
        if segment.segment.create_type == 'condition':
            await run_segment_cleaning(segment.segment_type.value, insert_segment_id, segment.segment.condition_type,
                                       session)
        return {'code': 200, "segment_id": insert_segment_id, "msg": "Segment submitted successfully."}
    except Exception as e:
        logger.error(f"Error Submit Segments: {str(e)}")
        return {'code': 300, "msg": str(e)}


@app.delete("/promotion_api/segments/delete")
async def delete_segments(
        segment_id: int,
        segment_type: Segment_Type,
        session=Depends(get_db),
        user_id: int = Depends(get_current_user)
):
    try:
        if segment_type == Segment_Type.item:
            in_use = await get_item_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": "Segment in use.cannot be deleted."}
            await delete_segment_item(session, segment_id)
            await delete_segment_item_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        elif segment_type == Segment_Type.location:
            in_use = await get_location_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": "Segment in use.cannot be deleted."}
            await delete_segment_location(session, segment_id)
            await delete_segment_location_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        elif segment_type == Segment_Type.customer:
            in_use = await get_customer_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": "Segment in use.cannot be deleted."}
            await delete_segment_customer(session, segment_id)
            await delete_segment_customer_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        data = {'code': 200, "msg": f"Segment deleted successfully user: {user_id}."}
        logging.info(data)
        return data
    except Exception as e:
        logger.error(f"Error Delete Segments: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.post("/promotion_api/segments/update_status")
async def update_segments_status(
        segment_id: int,
        segment_type: Segment_Type,
        segment_status: Segment_Status,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        if segment_type.value == Segment_Type.item.value:
            await update_segment_item_status(session, segment_id, segment_status.value)
        elif segment_type == Segment_Type.location:
            await update_segment_location_status(session, segment_id, segment_status.value)
        elif segment_type == Segment_Type.customer:
            await update_segment_customer_status(session, segment_id, segment_status.value)
        else:
            return {'code': 300, "msg": "Invalid segment type."}
        return {'code': 200, "msg": "Segment status updated successfully."}
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/segments/details")
async def read_segments_details(
        segment_type: Segment_Type,
        segment_id: int,
        key_word: str = None,
        page: int = 1,
        page_size: int = 40,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        if segment_type.value == Segment_Type.item.value:
            segments_detail = await get_segments_item_detail(session, segment_id, key_word, page, page_size)
            show_column = dict_config['item_column']
        elif segment_type == Segment_Type.location:
            segments_detail = await get_setgments_location_detail(session, segment_id, key_word, page, page_size)
            show_column = dict_config['location_column']
        elif segment_type == Segment_Type.customer:
            segments_detail = await get_segments_customer_detail(session, segment_id, key_word, page, page_size)
            show_column = dict_config['customer_column']
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        return {
            "code": 200,
            "show_column": show_column,
            "segments_detail": segments_detail
        }
    except Exception as e:
        logger.error(f"Error reading segments details: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/segments/get_store_list")
async def read_store_list(key_word: str = None,
                          page: int = 1,
                          page_size: int = 40,
                          session=Depends(get_db), user_id=Depends(get_current_user)):
    try:
        store_list = await get_store_list(session, key_word, page, page_size)
        return {'code': 200, 'store_list': store_list}
    except Exception as e:
        logger.error(f"Error reading store list: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_class")
async def read_promotion_class(user_id=Depends(get_current_user)):
    p_class = dict_config['promotion_class']
    return {'code': 200, 'promotion_class': p_class}


@app.get("/promotion_api/promotion/promotion_default")
async def read_promotion_defult(class_id: int, subclass_id: int = 0, user_id=Depends(get_current_user)):
    try:
        p_default = dict_config['promotion_template_default']
        if p_default:
            p_default = p_default[class_id][subclass_id]

        return {'code': 200, 'template_default': p_default}
    except Exception as e:
        print('promotion_template_default error')
        logger.error(f"Error reading promotion default: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_default_p")
async def read_promotion_defult_p(class_id: int, subclass_id: int = 0, user_id=Depends(get_current_user)):
    try:
        p_default = dict_config['promotion_template_default_p']
        if p_default:
            p_default = p_default[class_id][subclass_id]

        return {'code': 200, 'template_default': p_default}
    except Exception as e:
        print('promotion_template_default error')
        logger.error(f"Error reading promotion default: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_level")
async def read_promotion_level(user_id=Depends(get_current_user)):
    p_level = dict_config['promotion_level']
    return {'code': 200, 'promotion_level': p_level}


@app.get("/promotion_api/promotion/promotion_type")
async def read_promotion_type(user_id=Depends(get_current_user)):
    p_type = dict_config['promotion_type']
    return {'code': 200, 'promotion_type': p_type}


@app.get("/promotion_api/promotion/promotion_template")
async def read_promotion_template(class_id: int, user_id=Depends(get_current_user)):
    p_template = dict_config['promotion_template']
    filtered_data = [item for item in p_template if item['class_id'] == class_id]
    return {'code': 200, 'promotion_template': filtered_data}


@app.get("/promotion_api/promotion/promotion_condition")
async def read_promotion_condition(user_id=Depends(get_current_user)):
    p_condition = dict_config['promotion_condition']
    return {'code': 200, 'promotion_condition': p_condition}


@app.get("/promotion_api/promotion/promotion_result")
async def read_promotion_result(user_id=Depends(get_current_user)):
    p_result = dict_config['promotion_result']
    return {'code': 200, 'promotion_condition': p_result}


@app.get("/promotion_api/promotion/promotion_group")
async def read_promotion_group(user_id=Depends(get_current_user)):
    p_group = dict_config['promotion_group']
    return {'code': 200, 'promotion_group': p_group}


@app.post("/promotion_api/promotion/submit")
async def submit_promotion(
        promotionsubmit: PromotionSubmit,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        logging.info(f"Submit Promotion: {repr(promotionsubmit.model_dump())}")

        if promotionsubmit.promotion.promotion_id:
            promotion_id = promotionsubmit.promotion.promotion_id
            await update_promotion(session, promotionsubmit.promotion, user_id)
            await update_promotion_condition(session, promotionsubmit.promotion.promotion_id,
                                             promotionsubmit.promotion_condition, user_id)
            await update_promotion_result(session, promotionsubmit.promotion.promotion_id,
                                          promotionsubmit.promotion_result, user_id)
            await delete_promotion_item_segments(session, promotion_id)
            await delete_promotion_location_segments(session, promotion_id)
            await delete_promotion_customer_segments(session, promotion_id)
        else:
            new_promotion = await create_promotion(session, promotionsubmit.promotion, user_id)
            promotion_id = new_promotion.promotion_id
            await create_promotion_condition(session, promotion_id, promotionsubmit.promotion,
                                             promotionsubmit.promotion_condition)
            await create_promotion_result(session, promotion_id, promotionsubmit.promotion,
                                          promotionsubmit.promotion_result)

        await create_promotion_item_segments(session, promotion_id, promotionsubmit.promotion,
                                             promotionsubmit.promotion_item_segments)
        if promotionsubmit.promotion_customer_segments:
            await create_promotion_customer_segments(session, promotion_id, promotionsubmit.promotion,
                                                     promotionsubmit.promotion_customer_segments)
        if promotionsubmit.promotion_location_segments:
            await create_promotion_location_segments(session, promotion_id, promotionsubmit.promotion,
                                                     promotionsubmit.promotion_location_segments)
        if promotionsubmit.promotion_org_data:
            await create_promotion_org_data(session, promotion_id,
                                            promotionsubmit.promotion_org_data)

        return {'code': 200, "promotion_id": promotion_id, "msg": "Promotion submitted successfully."}
    except Exception as e:
        logging.error(f"Error submitting promotion: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.delete("/promotion_api/promotion/delete")
async def delete_promo(
        promotion_id: int,
        session=Depends(get_db)
):
    try:
        res_promo = await get_promotion_by_id(session, promotion_id)

        if res_promo.last_export_time:
            return {'code': 301, "msg": "Promotion has been exported, cannot be deleted."}

        await delete_promotion(session, promotion_id)
        await delete_promotion_item_segments(session, promotion_id)
        await delete_promotion_location_segments(session, promotion_id)
        return {'code': 200, "msg": "Promotion deleted successfully."}
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.post("/promotion_api/promotion/update_status")
async def set_promotion_status(
        promotion_id: int,
        promotion_status: Segment_Status,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        await update_promotion_status(session, promotion_id, promotion_status.value)
        return {'code': 200, "msg": "Promotion status updated successfully."}
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_list")
async def read_promotion_list(
        key_word: str = None,
        promotion_status: Data_Status = Data_Status.ALL,
        page: int = 1,
        page_size: int = 30,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:

        return await get_promotion_list(session, key_word, promotion_status.value, page, page_size)
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion")
async def read_promotion(
        promotion_id: int,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        promotion_header, promotion_condition, promotion_result, promotion_item_segments, promotion_location_segments, promotion_customer_segments = await asyncio.gather(
            get_promotion_by_id(session, promotion_id),
            get_promotion_condition_by_id(session, promotion_id),
            get_promotion_result_by_id(session, promotion_id),
            get_promotion_item_segments_by_id(session, promotion_id),
            get_promotion_location_segments_by_id(session, promotion_id),
            get_promotion_customer_segments_by_id(session, promotion_id)
        )
        promotion_org_join = await get_promotion_org_join_by_id(session, promotion_id)
        locs_data = await get_location_detail_by_promotionId(promotion_id, session)
        df_locs = locs_data['data']
    except Exception as e:
        logging.error(f"Error reading promotion: {str(e)}")
        return {'code': 301, "msg": str(e)}

    return {
        'code': 200,
        'promotion_header': promotion_header,
        'promotion_condition': promotion_condition,
        'promotion_result': promotion_result,
        'promotion_item_segments': promotion_item_segments,
        'promotion_customer_segments': promotion_customer_segments,
        'promotion_location_segments': promotion_location_segments,
        'location_count': 0 if df_locs is None else len(df_locs),
        'promotion_org_data': promotion_org_join
    }


@app.get("/promotion_api/promotion/promotion_dashboard")
async def read_promotion_dashboard(
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        res_promo = await service.promotion.get_promotion_dashboard(session)
        res_item = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.item)
        res_location = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.location)
        res_customer = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.customer)
        logging.info(res_item)
        data = {
            "Promotion_Count": {
                "Total": res_promo['Total'],
                "Not_Started": res_promo['Not_Started'],
                "In_Progress": res_promo['In_Progress'],
                "Completed": res_promo['Completed']
            },
            "Promotion_Type": {
                "Product": res_promo['Product'],
                "Coupon": res_promo['Coupon']
            },
            "Apply_Type": {
                "Transaction": res_promo['Transaction'],
                "Line": res_promo['Line']
            },
            "Discount_Type": {
                "Percentage": res_promo['Percent_off'],
                "Amount": res_promo['Amount_off'],
                "Fix_Price": res_promo['Fix_Price']
            },
            "Customer_Segment":
                {
                    "Total": res_customer.get('Total', 0),
                    "Active": res_customer.get('Active', 0),
                    "In_Use": res_customer.get('In_Use', 0)
                },
            "Item_Segment":
                {
                    "Total": res_item.get('Total', 0),
                    "Active": res_item.get('Active', 0),
                    "In_Use": res_item.get('In_Use', 0)
                },
            "Location_Segment":
                {
                    "Total": res_location.get('Total', 0),
                    "Active": res_location.get('Active', 0),
                    "In_Use": res_location.get('In_Use', 0)
                }
        }
        logging.info(data)
        return {'code': 200, 'data': data}
    except Exception as e:
        logging.error(f"promotion_dashboard: {repr(e)}")
        return {'code': 301, "msg": str(e)}



# # # # #
if __name__ == '__main__':
    import uvicorn

    port = dict_config.get('SERVER_PORT', 8000)
    uvicorn.run(app, host="0.0.0.0", port=8000)
