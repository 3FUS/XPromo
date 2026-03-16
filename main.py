import json

import yaml
from fastapi import FastAPI, Depends, Query, UploadFile, File, HTTPException, status, Body

import schemas
from service.mnt_generate import generate_deal_insert, generate_deal_item_insert, generate_deal_item_test_insert, \
    generate_deal_trigger_insert, generate_deal_coupon_xref_insert
from utils.sftp_uploader import upload_mnt_file

import service
from models.model import SegmentsItem, SegmentsItemDetail, PromotionItemSegments, PromotionCondition, PromotionResult, \
    PromotionImport
from schemas.schemas import PromotionSubmit
from schemas import schemas
from datetime import datetime, timedelta
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
import pandas as pd

import os
import asyncio
from sqlalchemy import text
from typing import List, Union
from utils.translator import get_message

from service.segments_service \
    import create_segment_item, update_segment_some, process_segment_data, \
    generate_segment_id

from service.promotion import create_promotion, create_promotion_condition, create_promotion_result, \
    create_promotion_item_segments, create_promotion_location_segments, create_promotion_customer_segments, \
    update_promotion, delete_promotion_item_segments, get_promotion_list, \
    get_promotion_by_id, get_promotion_condition_by_id, get_promotion_result_by_id, \
    get_promotion_customer_segments_by_id, get_promotion_item_segments_by_id, \
    get_promotion_location_segments_by_id, delete_promotion_customer_segments, delete_promotion_location_segments, \
    update_promotion_status, get_promotion_location_detail_by_id, \
    update_promotion_export_time, delete_promotion, create_promotion_org_data, get_promotion_org_join_by_id, \
    get_promotion_location_detail_by_id_v2, process_promotion_data, delete_promotion_condition, delete_promotion_result, \
    delete_promotion_import, get_promotion_import_by_id, get_location_detail_by_promotionId

from service.worker import create_worker_task, create_termination_task
from service.access_service import verify_password, get_sys_user_configuration

from service import get_db
from utils.config_manager import config_manager
from utils.upload_utils import validate_and_read_file, _clean_and_standardize_data
from core.security import get_current_user, create_access_token
from utils.logger import app_logger

from contextlib import asynccontextmanager
from scheduler.scheduler_manager import scheduler_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        # 启动时
        scheduler_manager.init_scheduler()
        await scheduler_manager.start_scheduler()
        app_logger.info("Application and scheduler started successfully")
    except Exception as e:
        app_logger.error(f"Failed to start scheduler: {e}")
        raise
    yield
    try:
        await scheduler_manager.shutdown_scheduler()
        app_logger.info("Application and scheduler shutdown completed")
    except Exception as e:
        app_logger.error(f"Error during shutdown: {e}")
        raise


app = FastAPI(
    title="promotion_api",
    description="promotion_api",
    lifespan=lifespan
)

from utils.config_manager import ConfigManager

config_condition = ConfigManager('segments_condition.yaml')

file = open('config/segments_condition.yaml', 'r', encoding='utf-8')
dict_condition = yaml.safe_load(file)

from utils.app_config import app_config, reload_config


def on_config_update():
    """配置更新回调函数"""
    reload_config()


config_manager.add_callback(on_config_update)
on_config_update()

from routers.configuration import router as configuration_api_router
from worker_api.api import router as worker_api_router
from routers.user import router as user_api_router
from routers.segments import router as segments_api_router
from routers.competitorsales import router as competitor_sales_api_router

app.include_router(configuration_api_router)
app.include_router(worker_api_router, prefix="/worker", tags=["worker"])
app.include_router(user_api_router, prefix="/user_api")
app.include_router(segments_api_router, prefix="/promotion_api/segments", tags=["segments"])
app.include_router(competitor_sales_api_router, prefix="/competitor_api", tags=["competitor"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="promotion_api/token")

from enums import Segment_Type, Segment_Status, Data_Status


@app.post("/promotion_api/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(),
                                 lang: str = Query("en",
                                                   description="Language preference: 'en' for English, 'zh' for Chinese"),
                                 session=Depends(get_db)):
    user = await authenticate_user(form_data.username, form_data.password, session)
    if not user:
        # raise HTTPException(
        #     status_code=status.HTTP_401_UNAUTHORIZED,
        #     detail="Incorrect username or password",
        #     headers={"WWW-Authenticate": "Bearer"},
        # )
        return {"code": 301, "msg": get_message("incorrect_credentials", lang)}
    if user['user_status'] != 'active':
        return {"code": 301, "msg": get_message("user_status_error", lang)}
    access_token_expires = timedelta(minutes=720)
    access_token = create_access_token(
        data={"sub": user['user_code']}, expires_delta=access_token_expires
    )
    return {"code": 200, "access_token": access_token, "token_type": "bearer", "configuration": user['configuration'],
            "user_code": user['user_code'], "user_name": user['username']}


async def authenticate_user(username: str, password: str, session):
    if await verify_password(session, username, password):
        user_info = await get_sys_user_configuration(session, username)
        return {"user_code": user_info['user_code'], "username": user_info['user_name'],
                "configuration": user_info['configuration'], "user_status": user_info['user_status']}
    return None


@app.get("/promotion_api/organizations")
async def get_organizations(
        active_only: bool = Query(True, description="只返回激活的组织"),
        lang: str = Query("en", description="语言: 'en' 英文, 'zh' 简体中文, 'zh_tw' 繁体中文, 'jp' 日文")
):
    """
    获取组织配置信息

    Args:
        active_only (bool): 是否只返回激活的组织，默认True
        lang (str): 返回的语言版本
        user_id: 当前用户ID

    Returns:
        dict: 组织配置信息
    """
    try:
        # 读取组织配置文件

        org_config = app_config.org_config
        app_logger.debug(f"Reading organization configuration file: {org_config}")
        # 获取组织列表
        organizations = org_config.get('organizations', [])

        # 根据active_only参数过滤
        if active_only:
            organizations = [org for org in organizations if org.get('active', True)]

        # 根据语言偏好处理返回数据
        processed_orgs = []
        for org in organizations:
            processed_org = org.copy()

            # 根据语言设置返回相应的名称
            if lang == "zh":
                if 'org_name_zh' in processed_org:
                    processed_org['org_name'] = processed_org['org_name_zh']
            elif lang == "zh_tw":
                if 'org_name_zh_tw' in processed_org:
                    processed_org['org_name'] = processed_org['org_name_zh_tw']
            elif lang == "jp":
                if 'org_name_jp' in processed_org:
                    processed_org['org_name'] = processed_org['org_name_jp']

            # 移除语言特定字段，避免暴露给前端多余数据
            processed_org.pop('org_name_zh', None)
            processed_org.pop('org_name_zh_tw', None)
            processed_org.pop('org_name_jp', None)

            processed_orgs.append(processed_org)

        return {
            'code': 200,
            'organizations': processed_orgs,
            'total_count': len(processed_orgs)
        }

    except FileNotFoundError:
        app_logger.error("Organization config file not found")
        return {'code': 301, 'msg': 'Organization configuration file not found'}
    except Exception as e:
        app_logger.error(f"Error reading organization config: {str(e)}")
        return {'code': 301, 'msg': f'Error reading organization configuration: {str(e)}'}


@app.get("/promotion_api/segments_condition")
async def read_segments_condition(segment_type: Segment_Type, session=Depends(get_db),
                                  org_id: str = Query(None, description="组织ID"),
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
    if org_id:
        # dict_condition = dict_config['ORG_CONDITION'][org_id]

        c_condition = config_condition.get_config(org_id)
        app_logger.info(f"Reading segments condition for org_id: {c_condition}")
        condition_list = c_condition.get(segment_type.value, [])
    else:
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
                app_logger.error(f"Error executing SQL: {sql_query}, Error: {e}")
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
    file_path = os.path.join(app_config.directory, file_name)

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
                            f"INSERT|ITEM_DEAL_PROPERTY|{item.get('item_id', '')}|{item.get('itm_deal_property_code', '')}||{item.get('begin_date', '')}|{end_date}|STRING|TRUE|||*|*\n")
        return True
    except Exception as e:
        app_logger.error("Error writing MNT file: {}".format(repr(e)))
        return False


@app.get("/promotion_api/segments/export_segments")
async def export_segments(segment_type: Segment_Type, segment_id: int,
                          store_ids: str = Query(None, description="逗号分隔的门店ID列表"),
                          lang: str = Query("en"),
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
            if app_config.Export_Type == 'MNT':
                for store in store_list:
                    success = generate_item_mnt_file(segment_id, item_data, f"STORE:{store}")

            if app_config.Export_Type == 'WORKER':
                sessionId = await create_worker_task(session, store_list, 'segment_item', segment_id)
                await update_segment_some(segment_type.value, session, segment_id,
                                          {"last_session_id": sessionId, "export_time": datetime.now()})

            msg = get_message("export_tag_success", lang)
        else:
            msg = get_message("no_store_data_export", lang)

        return {"code": 200, "msg": msg}
    except Exception as e:
        app_logger.error("export_tag {}".format(repr(e)))
        return {"code": 301, "msg": "export tag error {0}".format(repr(e))}


@app.get("/promotion_api/promotion/export_promotion")
async def export_promotion(promotion_id: int, session=Depends(get_db),
                           lang: str = Query("en",
                                             description="Language preference: 'en' for English, 'zh' for Chinese"),
                           user_id=Depends(get_current_user)):
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

        if app_config.Export_Type == 'MNT':
            if locs_data['data_type'] == 'hierarchy':
                promotion_org_join = await get_promotion_org_join_by_id(session, promotion_id)
            else:
                promotion_org_join = ['STORE:' + str(rtl_loc_id) for rtl_loc_id in df_locs['rtl_loc_id']]
            data_detail = await process_promotion_data(promotion_id, session, 1)
            for org in promotion_org_join:
                file_name = '{0}_Promotion_{1}.mnt'.format(promotion_id, datetime.now().strftime('%Y%m%d%H%M%S'))
                deployment_name = '{0}_Promotion_{1}'.format(promotion_id, datetime.now().strftime('%Y%m%d%H%M%S'))
                td = datetime.now().strftime('%Y-%m-%d')
                file_path = os.path.join(app_config.directory, file_name)

                with open(file_path, 'w', encoding='utf-8') as mnt_file:

                    mnt_file.write(
                        f'<Header application_date="{td}" apply_immediately="TRUE" deployment_name="{deployment_name}" download_id="{deployment_name}" download_time="IMMEDIATE" target_org_node="{org}" />\n')

                    for t in app_config.PROMOTION_TABLES:
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
        if app_config.Export_Type == 'WORKER':

            for segment in res_segments:
                sessionId = await create_worker_task(session, df_locs['rtl_loc_id'].tolist(), 'segment_item',
                                                     segment['segment_id'])
                await update_segment_some(Segment_Type.item.value, session, segment['segment_id'],
                                          {"last_session_id": sessionId, "export_time": datetime.now()})
            sessionId = await create_worker_task(session, df_locs['rtl_loc_id'].tolist(), 'promotion', promotion_id)
            df_termination_locs = locs_data['termination_locs']
            if not df_termination_locs.empty:
                await create_termination_task(session, df_termination_locs['rtl_loc_id'].tolist(),
                                              'promotion',
                                              promotion_id)
            await update_promotion_export_time(session, promotion_id, export_date, sessionId)
        return {"code": 200, "msg": get_message("export_tag_success", lang)}
    except Exception as e:
        app_logger.error(f"export_tag: {repr(e)}")
        return {"code": 301, "msg": get_message("export_tag_error", lang)}


@app.get("/promotion_api/promotion/promotion_class")
async def read_promotion_class(
        lang: str = Query("en", description="Language preference: 'en' for English, 'zh' for Chinese"),
        org_id: str = Query(None, description="ORG ID"),
        user_id=Depends(get_current_user)):
    template = app_config.template_config_org[org_id]['promotion_class'] if org_id else app_config.template_config[
        'promotion_class']
    p_class = [item.copy() for item in template]
    app_logger.info(f"Promotion class: {p_class}")

    # 根据语言偏好设置返回相应的描述
    if lang == "zh":
        # 处理简体中文
        for item in p_class:
            if 'code_zh' in item:
                item['code'] = item['code_zh']
            if 'description_zh' in item:
                item['description'] = item['description_zh']

            item.pop('code_zh', None)
            item.pop('description_zh', None)
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)
            item.pop('code_jp', None)
            item.pop('description_jp', None)
    elif lang == "zh_tw":
        # 处理繁体中文
        for item in p_class:
            if 'code_zh_tw' in item:
                item['code'] = item['code_zh_tw']
            if 'description_zh_tw' in item:
                item['description'] = item['description_zh_tw']
            # 移除中文字段，避免暴露给前端多余的数据
            item.pop('code_zh', None)
            item.pop('description_zh', None)
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)
            item.pop('code_jp', None)
            item.pop('description_jp', None)
    elif lang == "jp":
        # 处理日文
        for item in p_class:
            if 'code_jp' in item:
                item['code'] = item['code_jp']
            if 'description_jp' in item:
                item['description'] = item['description_jp']
            # 移除其他语言字段，避免暴露给前端多余的数据
            item.pop('code_zh', None)
            item.pop('description_zh', None)
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)
            item.pop('code_jp', None)
            item.pop('description_jp', None)
    else:
        # 处理英文 - 移除所有其他语言字段
        for item in p_class:
            item.pop('code_zh', None)
            item.pop('description_zh', None)
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)
            item.pop('code_jp', None)
            item.pop('description_jp', None)

    return {'code': 200, 'promotion_class': p_class}


@app.get("/promotion_api/promotion/promotion_default")
async def read_promotion_defult(class_id: str, subclass_id: str = "0", org_id: str = Query(None, description="ORG ID"),
                                user_id=Depends(get_current_user)):
    try:
        p_default = app_config.template_config['promotion_template_default']
        if p_default:
            p_default = p_default[class_id][subclass_id]

        return {'code': 200, 'template_default': p_default}
    except Exception as e:
        print('promotion_template_default error')
        app_logger.error(f"Error reading promotion default: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_default_p")
async def read_promotion_defult_p(class_id: str, subclass_id: str = "0",
                                  org_id: str = Query(None, description="ORG ID"), user_id=Depends(get_current_user)):
    try:
        p_default = app_config.template_config_org[org_id].get('promotion_template_default_p') if org_id else \
            app_config.template_config['promotion_template_default_p']
        if p_default:
            # 检查 class_id 是否存在
            if class_id not in p_default:
                return {'code': 301, "msg": f"Class ID {class_id} not found"}

            class_data = p_default[class_id]

            # 检查 subclass_id 是否存在
            if subclass_id not in class_data:
                # 如果指定的 subclass_id 不存在，返回默认值（subclass_id=0）
                if 0 in class_data:
                    p_default = class_data[0]
                else:
                    return {'code': 301, "msg": f"Subclass ID {subclass_id} not found for class {class_id}"}
            else:
                p_default = class_data[subclass_id]

        return {'code': 200, 'template_default': p_default}
    except Exception as e:
        print('promotion_template_default error')
        app_logger.error(f"Error reading promotion default: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_level")
async def read_promotion_level(user_id=Depends(get_current_user)):
    p_level = app_config.dict_config['promotion_level']
    return {'code': 200, 'promotion_level': p_level}


@app.get("/promotion_api/promotion/promotion_type")
async def read_promotion_type(user_id=Depends(get_current_user)):
    p_type = app_config.dict_config['promotion_type']
    return {'code': 200, 'promotion_type': p_type}


@app.get("/promotion_api/promotion/promotion_template")
async def read_promotion_template(
        class_id: str,
        org_id: str = Query(None, description="ORG ID"),
        lang: str = Query("en"),
        user_id=Depends(get_current_user)
):
    app_logger.info(f"read_promotion_template called with class_id={class_id}, lang={lang}, user_id={user_id}")

    p_template = app_config.template_config_org[org_id].get('promotion_template') if org_id else \
        app_config.template_config['promotion_template']
    filtered_data = [item.copy() for item in p_template if item['class_id'] == class_id]

    # 根据语言偏好设置返回相应的描述
    if lang == "zh":
        app_logger.info("Processing Simplified Chinese language request")
        for item in filtered_data:
            if 'code_zh' in item:
                item['code'] = item['code_zh']
            if 'description_zh' in item:
                item['description'] = item['description_zh']
            # 移除中文字段，避免暴露给前端多余的数据
            item.pop('code_zh', None)
            item.pop('description_zh', None)
    elif lang == "zh_tw":
        app_logger.info("Processing Traditional Chinese language request")
        for item in filtered_data:
            if 'code_zh_tw' in item:
                item['code'] = item['code_zh_tw']
            if 'description_zh_tw' in item:
                item['description'] = item['description_zh_tw']
            # 移除中文字段，避免暴露给前端多余的数据
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)
    elif lang == "jp":
        app_logger.info("Processing Japanese language request")
        for item in filtered_data:
            if 'code_jp' in item:
                item['code'] = item['code_jp']
            if 'description_jp' in item:
                item['description'] = item['description_jp']
            # 移除日语字段，避免暴露给前端多余的数据
            item.pop('code_jp', None)
            item.pop('description_jp', None)
    else:
        app_logger.info("Processing English language request - removing Chinese fields")
        # 处理英文 - 移除中文字段
        for item in filtered_data:
            # 移除中文字段，避免暴露给前端多余的数据
            item.pop('code_zh', None)
            item.pop('description_zh', None)
            item.pop('code_zh_tw', None)
            item.pop('description_zh_tw', None)

    app_logger.info(f"Returning {len(filtered_data)} items after language processing")
    return {'code': 200, 'promotion_template': filtered_data}


@app.get("/promotion_api/promotion/promotion_condition")
async def read_promotion_condition(user_id=Depends(get_current_user)):
    p_condition = app_config.dict_config['promotion_condition']
    return {'code': 200, 'promotion_condition': p_condition}


@app.get("/promotion_api/promotion/promotion_result")
async def read_promotion_result(user_id=Depends(get_current_user)):
    p_result = app_config.dict_config['promotion_result']
    return {'code': 200, 'promotion_condition': p_result}


@app.get("/promotion_api/promotion/promotion_group")
async def read_promotion_group(user_id=Depends(get_current_user)):
    p_group = app_config.dict_config['promotion_group']
    return {'code': 200, 'promotion_group': p_group}


@app.post("/promotion_api/promotion/submit")
async def submit_promotion(
        promotionsubmit: PromotionSubmit,
        lang: str = Query("en"),
        org_id: str = Query(None, description="ORG ID"),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        app_logger.info(f"Submit Promotion: {repr(promotionsubmit.model_dump())}")

        if promotionsubmit.promotion.promotion_id:
            promotion_id = promotionsubmit.promotion.promotion_id
            await update_promotion(session, promotionsubmit.promotion, user_id)
            # await update_promotion_condition(session, promotionsubmit.promotion.promotion_id,
            #                                  promotionsubmit.promotion_condition, user_id)
            # app_logger.info(f"Submit Update promotion condition: {promotion_id}")
            # await update_promotion_result(session, promotionsubmit.promotion.promotion_id,
            #                               promotionsubmit.promotion_result, user_id)
            await delete_promotion_condition(session, promotion_id)
            await delete_promotion_result(session, promotion_id)
            await delete_promotion_item_segments(session, promotion_id)
            await delete_promotion_location_segments(session, promotion_id)
            await delete_promotion_customer_segments(session, promotion_id)
        else:
            new_promotion = await create_promotion(session, promotionsubmit.promotion, user_id, org_id)
            promotion_id = new_promotion.promotion_id
            # await create_promotion_condition(session, promotion_id, promotionsubmit.promotion,
            #                                  promotionsubmit.promotion_condition)
            # await create_promotion_result(session, promotion_id, promotionsubmit.promotion,
            #                               promotionsubmit.promotion_result)

        await create_promotion_condition(session, promotion_id, promotionsubmit.promotion,
                                         promotionsubmit.promotion_condition)
        await create_promotion_result(session, promotion_id, promotionsubmit.promotion,
                                      promotionsubmit.promotion_result)

        await create_promotion_item_segments(session, promotion_id, user_id,
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

        return {'code': 200, "promotion_id": promotion_id, "msg": get_message("promotion_submitted", lang)}
    except Exception as e:
        app_logger.error(f"Error submitting promotion: {str(e)}")
        return {'code': 301, "msg": str(e)}


@app.delete("/promotion_api/promotion/delete")
async def delete_promo(
        promotion_id: int,
        lang: str = Query("en"),
        session=Depends(get_db)
):
    try:
        res_promo = await get_promotion_by_id(session, promotion_id)

        if res_promo.last_export_time:
            return {'code': 301, "msg": get_message("promotion_exported_cannot_delete", lang)}

        await delete_promotion(session, promotion_id)
        await delete_promotion_item_segments(session, promotion_id)
        await delete_promotion_location_segments(session, promotion_id)
        return {'code': 200, "msg": get_message("promotion_deleted", lang)}
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.post("/promotion_api/promotion/update_status")
async def set_promotion_status(
        promotion_id: int,
        promotion_status: Segment_Status,
        lang: str = Query("en"),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        await update_promotion_status(session, promotion_id, promotion_status.value)
        return {'code': 200, "msg": get_message("promotion_status_updated", lang)}
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion_list")
async def read_promotion_list(
        key_word: str = None,
        promotion_status: Data_Status = Data_Status.ALL,
        org_id: str = None,
        page: int = 1,
        page_size: int = 30,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:

        return await get_promotion_list(session, key_word, promotion_status.value, org_id, page, page_size)
    except Exception as e:
        return {'code': 301, "msg": str(e)}


@app.get("/promotion_api/promotion/promotion")
async def read_promotion(
        promotion_id: int,
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:
        promotion_header, promotion_condition, promotion_result, promotion_item_segments, promotion_location_segments, promotion_customer_segments, promotion_import = await asyncio.gather(
            get_promotion_by_id(session, promotion_id),
            get_promotion_condition_by_id(session, promotion_id),
            get_promotion_result_by_id(session, promotion_id),
            get_promotion_item_segments_by_id(session, promotion_id),
            get_promotion_location_segments_by_id(session, promotion_id),
            get_promotion_customer_segments_by_id(session, promotion_id),
            get_promotion_import_by_id(session, promotion_id)
        )
        promotion_org_join = await get_promotion_org_join_by_id(session, promotion_id)
        locs_data = await get_location_detail_by_promotionId(promotion_id, session)
        df_locs = locs_data['data']
    except Exception as e:
        app_logger.error(f"Error reading promotion: {str(e)}")
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
        'promotion_org_data': promotion_org_join,
        'promotion_import': promotion_import
    }


@app.post("/promotion_api/promotion/import_promotion_segments")
async def import_promotion_segments(
        submit: Union[dict, str, None] = Body(None),
        uFile: UploadFile = File(None),
        preview: bool = Query(False, description="是否为预览模式"),
        org_id: str = Query(None),
        lang: str = Query("en"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    根据Excel中的价格分组导入商品段并关联到促销

    Excel需包含两列: item_id 和 price
    系统将根据price值自动分组，并为每组创建一个商品段
    """
    try:
        if not preview:
            if isinstance(submit, str):
                try:
                    submit_data = json.loads(submit)
                except json.JSONDecodeError:
                    raise ValueError("submit parameter is not valid JSON")
            else:
                submit_data = submit

            # 转换为PromotionSubmit_v1模型
            try:
                submit_model = schemas.PromotionSubmit_v1(**submit_data)
            except Exception as e:
                raise ValueError(f"Failed to validate submit data as PromotionSubmit_v1: {str(e)}")

            if submit_model.promotion.promotion_id:
                promotion_id = submit_model.promotion.promotion_id
                await update_promotion(session, submit_model.promotion, user_id)
                await delete_promotion_location_segments(session, promotion_id)
                await delete_promotion_customer_segments(session, promotion_id)
            else:
                new_promotion = await create_promotion(session, submit_model.promotion, user_id, org_id)
                promotion_id = new_promotion.promotion_id

            if submit_model.promotion_customer_segments:
                await create_promotion_customer_segments(session, promotion_id, submit_model.promotion,
                                                         submit_model.promotion_customer_segments)
            if submit_model.promotion_location_segments:
                await create_promotion_location_segments(session, promotion_id, submit_model.promotion,
                                                         submit_model.promotion_location_segments)
            if submit_model.promotion_org_data:
                await create_promotion_org_data(session, promotion_id,
                                                submit_model.promotion_org_data)

            if uFile is None:
                return {'code': 200, "promotion_id": promotion_id, "msg": get_message("promotion_submitted", lang)}

        df = await validate_and_read_file(uFile)
        df = await _clean_and_standardize_data(df)

        if preview:
            return await _generate_preview_response(df)

        # 验证促销存在性
        promotion = await _validate_promotion_exists(session, promotion_id)

        # 处理数据导入
        result = await _process_import_data(session, promotion_id, df, user_id, uFile.filename)

        return result

    except Exception as e:
        session.rollback()
        app_logger.error(f"import_promotion_segments failed: {repr(e)}. User ID: {user_id}, File: {uFile.filename}",
                         exc_info=True)
        return {'code': 301, "msg": str(e)}


async def _generate_preview_response(df):
    """生成预览响应"""
    price_groups = df.groupby('price')
    groups_info = []
    for price, group in price_groups:
        groups_info.append({
            "price": float(price),
            "item_count": len(group),
            "items": group[['item_id', 'price']].to_dict('records')
        })

    return {
        'code': 200,
        'preview': True,
        'data': df.to_dict('records'),
        'total_items': len(df),
        'total_groups': len(price_groups)
    }


async def _validate_promotion_exists(session, promotion_id):
    """验证促销是否存在"""
    promotion = await get_promotion_by_id(session, promotion_id)
    if not promotion:
        raise ValueError(f"Promotion with id {promotion_id} not found.")
    return promotion


async def _process_import_data(session, promotion_id, df, user_id, filename):
    """处理数据导入"""
    # 清理现有数据
    await _cleanup_existing_data(session, promotion_id)

    # 按价格分组处理
    price_groups = df.groupby('price')

    created_segments = []
    segment_ids = []
    set_id_counter = 1

    promotion_import = PromotionImport(
        promotion_id=promotion_id,
        file_name=filename,
        count_success=len(df),
        create_time=datetime.now(),
        create_user=user_id
    )
    session.add(promotion_import)

    for price, group in price_groups:
        # 创建段和关联数据
        segment_data = await _create_segment_for_price_group(
            session, promotion_id, price, group, set_id_counter, user_id
        )

        created_segments.append(segment_data["segment_info"])
        segment_ids.append(segment_data["segment_id"])
        set_id_counter += 1

    session.commit()

    return {
        'code': 200,
        'preview': False,
        'promotion_id': promotion_id,
        'msg': f"Successfully imported {len(segment_ids)} segments with {len(df)} items",
        'created_segments': created_segments,
        'segment_ids': segment_ids
    }


async def _cleanup_existing_data(session, promotion_id):
    await delete_promotion_condition(session, promotion_id)
    await delete_promotion_result(session, promotion_id)
    await delete_promotion_item_segments(session, promotion_id)
    await delete_promotion_import(session, promotion_id)


async def _create_segment_for_price_group(session, promotion_id, price, group, set_id, user_id):
    """为价格组创建段和关联数据"""
    # 创建段名称
    segment_name = f"segment (price: {price})"[:30]
    segment_desc = f"Auto-created segment (price: {price})"[:60]

    # 创建商品段
    new_segment = SegmentsItem(
        segment_id=generate_segment_id(session, "Item"),
        name=segment_name,
        description=segment_desc,
        create_type='import',
        segment_status='active',
        condition_type='or',
        public=0,
        create_time=datetime.now(),
        create_user=user_id,
        sub_count=len(group)
    )
    session.add(new_segment)

    # 创建段详情
    await _create_segment_details(session, new_segment.segment_id, group)

    # 创建促销条件
    new_condition = PromotionCondition(
        promotion_id=promotion_id,
        set_id=set_id,
        condition_type='Quantity',
        threshold_style='Every Quantity',
        MinQty=1,
        create_time=datetime.now()
    )
    session.add(new_condition)

    # 创建促销结果
    new_result = PromotionResult(
        promotion_id=promotion_id,
        set_id=set_id,
        apply_type='Line',
        action_qty=1,
        discount_type='NEW_PRICE',
        overlap=0,
        discount_value=price,
        create_time=datetime.now()
    )
    session.add(new_result)

    # 创建促销商品段关联
    promo_segment = PromotionItemSegments(
        promotion_id=promotion_id,
        set_id=set_id,
        segment_id=new_segment.segment_id,
        include=1,
        item_type='Condition',
        create_time=datetime.now(),
        create_user=user_id
    )
    session.add(promo_segment)

    return {
        "segment_id": new_segment.segment_id,
        "segment_info": {
            "segment_id": new_segment.segment_id,
            "segment_name": segment_name,
            "price": float(price),
            "item_count": len(group),
            "set_id": set_id
        }
    }


async def _create_segment_details(session, segment_id, group):
    """创建段详情"""
    segment_details = []

    for idx, (_, row) in enumerate(group.iterrows()):
        detail = SegmentsItemDetail(
            segment_id=segment_id,
            item_id=row['item_id'],
            create_time=datetime.now()
        )
        segment_details.append(detail)

        # 每1000条记录批量添加一次以提高性能
        if (idx + 1) % 1000 == 0:
            session.add_all(segment_details)
            segment_details = []

    # 添加剩余的记录
    if segment_details:
        session.add_all(segment_details)


@app.get("/promotion_api/promotion/promotion_dashboard")
async def read_promotion_dashboard(org_id: str = '',
                                   session=Depends(get_db), user_id=Depends(get_current_user)
                                   ):
    try:
        res_promo = await service.promotion.get_promotion_dashboard(session, org_id)
        res_item = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.item, org_id)
        res_location = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.location,
                                                                                 org_id)
        res_customer = await service.segments_service.get_segment_item_dashboard(session, schemas.Segment_Type.customer,
                                                                                 org_id)
        app_logger.info(res_item)
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
        app_logger.info(data)
        return {'code': 200, 'data': data}
    except Exception as e:
        app_logger.error(f"promotion_dashboard: {repr(e)}")
        return {'code': 301, "msg": str(e)}

#
if __name__ == '__main__':
    import uvicorn

    port = app_config.dict_config.get('SERVER_PORT', 8000)
    uvicorn.run(app, host="0.0.0.0", port=8000)

