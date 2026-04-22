import json

import pandas as pd
from fastapi import APIRouter, Query, UploadFile, File, HTTPException, status, Body, Depends

from models.model import SegmentsItem, SegmentsCustomer, SegmentsCustomerDetail, SegmentsLocation, \
    SegmentsLocationDetail, SegmentsItemDetail
from schemas.schemas import SegmentSubmit
from service import get_db
from typing import List, Union
import io

from service.segments_service import get_item_segment_by_name, get_customer_segment_by_name, \
    get_location_segment_by_name, create_segment_location, create_segment_item, create_segment_customer, \
    delete_segment_item_detail, delete_segment_customer_detail, delete_segment_location_detail, \
    create_item_segments_detail, create_customer_segments_detail, create_location_segments_detail, update_segment_item, \
    update_segment_customer, update_segment_location, delete_segment_import, create_segment_import, \
    get_segments_item_detail, get_setgments_location_detail, get_segments_customer_detail, get_segments_item_list, \
    get_segments_location_list, get_segments_customer_list, get_item_segment_by_id, get_item_segment_condition_by_id, \
    get_location_segment_by_id, get_location_segment_condition_by_id, get_customer_segment_by_id, \
    get_customer_segment_condition_by_id, get_item_segment_schedule_by_id, get_segment_import_by_id, \
    delete_segment_item_condition, create_segment_item_condition, delete_segment_location_condition, \
    create_segment_location_condition, delete_segment_customer_condition, create_segment_customer_condition, \
    delete_segment_schedule, create_segment_schedule, get_item_segments_in_use_by_id, delete_segment_item, \
    get_location_segments_in_use_by_id, delete_segment_location, get_customer_segments_in_use_by_id, \
    delete_segment_customer, update_segment_item_status, update_segment_location_status, update_segment_customer_status, \
    get_store_list_by_org_id
from utils.segment_etl import run_segment_cleaning
from utils.translator import get_message
from utils.upload_utils import validate_and_read_file, standardize_columns, validate_and_enrich_upload_data

from utils.logger import app_logger
from utils.app_config import app_config

router = APIRouter(tags=["segments"])
from core.security import get_current_user

from enums import Segment_Type, Segment_Status, Data_Status
from starlette.responses import StreamingResponse


@router.post("/submit_segment_v2")
async def upload_segment_v2(submit: Union[dict, str, None] = Body(None),
                            uFile: UploadFile = File(None),
                            preview: bool = Query(False, description="是否为预览模式"),
                            org_id: str = Query(None),
                            lang: str = Query("en"),
                            session=Depends(get_db), user_id=Depends(get_current_user)):
    try:
        app_logger.info(f"Starting submit_segment with upload_segment: {submit}, preview: {preview}")

        if isinstance(submit, str):
            try:
                submit_data = json.loads(submit)
            except json.JSONDecodeError:
                raise ValueError("submit parameter is not valid JSON")
        else:
            submit_data = submit

        if uFile:
            upload_data = await validate_and_read_file(uFile)
            upload_data = standardize_columns(upload_data, lang)
            file_name = uFile.filename
            if preview:
                if upload_data.empty:
                    return {'code': 303, "msg": "Uploaded file is empty."}
                return {'code': 200,
                        'preview': True,
                        'data': upload_data.to_dict('records'),
                        'total_items': len(upload_data)}
        else:
            upload_data = pd.DataFrame()

        segment_type = submit_data.get("segment_type")

        upload_data = await validate_and_enrich_upload_data(upload_data, segment_type, org_id)

        # item_data = load_item_data_from_db(segment_type, org_id)
        #
        # # Check for missing item_ids and complete missing fields
        # if not upload_data.empty and segment_type == 'item':
        #     # Add a new column to mark errors
        #     if 'item_id' in upload_data.columns:
        #         key_column = 'item_id'
        #     elif 'sku' in upload_data.columns:
        #         key_column = 'sku'
        #
        #     item_data_filtered = item_data[['item_id', 'name', 'sku', 'description', 'merch_level_1']].rename(
        #         columns={
        #             'name': 'item_name',
        #             'description': 'item_description',
        #             'merch_level_1': 'item_department'
        #         }
        #     )
        #
        #     upload_data = upload_data.drop_duplicates(subset=key_column, keep='first')
        #     upload_data = upload_data.merge(item_data_filtered, on=key_column, how='left')
        #
        #     upload_data['error_flag'] = 0
        #     upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')
        #     upload_data[key_column] = upload_data[key_column].astype(str)
        #     missing_items = ~upload_data[key_column].isin(item_data[key_column].astype(str))
        #     upload_data.loc[missing_items, 'error_flag'] = 1
        # elif not upload_data.empty and segment_type == 'location':
        #     loc_data_filtered = item_data[['rtl_loc_id', 'store_name', 'location_type', 'city']]
        #
        #     upload_data = upload_data.drop_duplicates(subset='rtl_loc_id', keep='first')
        #     upload_data['rtl_loc_id'] = upload_data['rtl_loc_id'].astype('Int64')
        #     upload_data = upload_data.merge(loc_data_filtered, on='rtl_loc_id', how='left')
        #
        #     upload_data['error_flag'] = 0
        #     upload_data['error_flag'] = upload_data['error_flag'].astype('Int64')
        #     missing_items = ~upload_data['rtl_loc_id'].isin(loc_data_filtered['rtl_loc_id'])
        #     upload_data.loc[missing_items, 'error_flag'] = 1

        segment_id = submit_data["segment"].get("segment_id", None)
        name = submit_data["segment"].get("name")
        description = submit_data["segment"].get("description")

        segment_classes = {
            "item": (SegmentsItem, SegmentsItemDetail, get_item_segment_by_name, create_segment_item),
            "customer": (
                SegmentsCustomer, SegmentsCustomerDetail, get_customer_segment_by_name, create_segment_customer),
            "location": (
                SegmentsLocation, SegmentsLocationDetail, get_location_segment_by_name, create_segment_location)
        }

        if segment_type not in segment_classes:
            return {'code': 305, "msg": "Invalid segment type."}
        else:
            app_logger.info(f"Segment classes: {segment_classes}")

        SegmentClass, DetailClass, get_segment_by_name_func, create_segment_func = segment_classes[segment_type]

        app_logger.debug(f"Segment ID: {segment_id}")
        if segment_id:
            insert_segment_id = segment_id
        else:
            segment = SegmentClass(
                name=name,
                description=description,
                sub_count=0,
                segment_status='active',
                public=0,
                create_type='import'
            )
            existing_segment = await get_segment_by_name_func(session, name=name)
            app_logger.debug(f"Existing segment: {existing_segment}")
            if existing_segment:
                return {'code': 300, "msg": get_message("segment_name_exists", lang)}
            insert_segment = await create_segment_func(session, segment, user_id, org_id)
            app_logger.info(f"Inserted segment: {insert_segment}")
            insert_segment_id = insert_segment.segment_id

        app_logger.debug(f"insert_segment_id : {insert_segment_id}")
        sub_count = 0

        if not upload_data.empty:
            valid_data = upload_data[upload_data['error_flag'] != 1].copy()
            app_logger.info(f"Valid data: {valid_data}")
            error_count = upload_data[upload_data['error_flag'] == 1].shape[0]

            delete_detail_func_map = {
                "item": delete_segment_item_detail,
                "customer": delete_segment_customer_detail,
                "location": delete_segment_location_detail
            }

            await delete_detail_func_map[segment_type](session, segment_id)

            detail_creation_map = {
                "item": create_item_segments_detail,
                "customer": create_customer_segments_detail,
                "location": create_location_segments_detail
            }
            app_logger.info(f"Uploading {segment_type} segment details...")
            detail_creator = detail_creation_map[segment_type]
            details = [
                DetailClass(segment_id=insert_segment_id, **row.to_dict())
                for _, row in valid_data.iterrows()
            ]
            await detail_creator(session, insert_segment_id, details)
            app_logger.info(f"Uploaded {len(details)} {segment_type} segment details.")
            await delete_segment_import(session, segment_id=insert_segment_id, segment_type=segment_type)
            await create_segment_import(session, insert_segment_id, segment_type, file_name, len(details), error_count)
            sub_count = len(details)

        update_segment_map = {
            "item": update_segment_item,
            "customer": update_segment_customer,
            "location": update_segment_location
        }

        update_segment_func = update_segment_map[segment_type]
        segment_data = {
            'segment_id': insert_segment_id,
            'name': name,
            'description': description,
            'org_id': org_id,
            'segment_status': 'active',
            'create_type': 'import'
        }

        if sub_count > 0:
            segment_data['sub_count'] = sub_count

        segment = SegmentClass(**segment_data)
        await update_segment_func(session, insert_segment_id, segment)

        return {'code': 200, "segment_id": insert_segment_id, "msg": "Segment submitted successfully."}
    except Exception as e:
        app_logger.error(f"Error submitting segment: {str(e)}")
        return {'code': 301, "msg": str(e)}


@router.get("/download_segments")
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
        app_logger.error(f"Error downloading segment data: {str(e)}")
        return {'code': 500, "msg": f"Error downloading data: {str(e)}"}


@router.post("/upload_segment")
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
            app_logger.error(f"upload_segment: Failed to parse Excel file. User ID: {user_id}, File: {file_name}")
            return {'code': 304, "msg": "Failed to parse Excel file."}

        if upload_data.empty:
            return {'code': 303, "msg": "Uploaded file is empty."}

        # upload_data = standardize_columns(upload_data)

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
        app_logger.error(f"upload_segment: Uploaded file is empty. User ID: {user_id}, File: {file_name}")
        return {'code': 303, "msg": "Uploaded file is empty."}
    except Exception as e:
        app_logger.error(f"upload_segment: {repr(e)}. User ID: {user_id}, File: {file_name}")
        return {'code': 301, "msg": str(e)}


@router.get("/segments_list")
async def read_segments_list(
        segment_type: Segment_Type,
        key_word: str = None,
        segment_status: Data_Status = Data_Status.ALL,
        org_id: str = Query(None, description="ORG ID"),
        list_type: int = Query(1, description="列表类型", ge=1),
        page: int = Query(1, description="页码", ge=1),
        page_size: int = Query(30, description="每页数量", ge=1, le=100),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    if segment_type == Segment_Type.item:
        return await get_segments_item_list(session, key_word, segment_status.value, org_id, list_type, page, page_size)
    elif segment_type == Segment_Type.location:
        return await get_segments_location_list(session, key_word, segment_status.value, org_id, page, page_size)
    elif segment_type == Segment_Type.customer:
        return await get_segments_customer_list(session, key_word, segment_status.value, org_id, page, page_size)
    else:
        return {'code': 300, "msg": "Invalid segment type."}


@router.get("/segments")
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
        app_logger.error(f"Error Get Segments: {str(e)}")
        return {'code': 301, "msg": str(e)}


@router.post("/submit")
async def submit_segments(
        segment: SegmentSubmit,
        org_id: str = Query(None, description="ORG ID"),
        lang: str = Query("en"),
        session=Depends(get_db), user_id=Depends(get_current_user)
):
    try:

        app_logger.info(f"Received segment data: {repr(segment.model_dump())}")
        if segment.segment_type.value == Segment_Type.item.value:
            item_segment = segment.segment
            if item_segment.segment_id:
                await update_segment_item(session, item_segment.segment_id, item_segment)
                await delete_segment_item_condition(session, item_segment.segment_id)
                insert_segment_id = item_segment.segment_id
            else:
                existing_segment = await get_item_segment_by_name(session, name=item_segment.name, org_id=org_id)
                if existing_segment:
                    return {'code': 300, "msg": get_message("segment_name_exists", lang)}
                insert_segment = await create_segment_item(session, item_segment, user_id, org_id)
                insert_segment_id = insert_segment.segment_id

            await create_segment_item_condition(session, insert_segment_id, segment.segment_condition)
        elif segment.segment_type == Segment_Type.location.value:
            location_segment = segment.segment
            if location_segment.segment_id:
                insert_segment_id = location_segment.segment_id
                await update_segment_location(session, location_segment.segment_id, location_segment)
                await delete_segment_location_condition(session, location_segment.segment_id)
            else:
                existing_segment = await get_location_segment_by_name(session, name=location_segment.name,
                                                                      org_id=org_id)
                if existing_segment:
                    return {'code': 300, "msg": get_message("segment_name_exists", lang)}
                insert_segment = await create_segment_location(session, location_segment, user_id, org_id)
                insert_segment_id = insert_segment.segment_id
            await create_segment_location_condition(session, insert_segment_id, segment.segment_condition)
        elif segment.segment_type == Segment_Type.customer.value:
            customer_segment = segment.segment
            if customer_segment.segment_id:
                insert_segment_id = customer_segment.segment_id
                await update_segment_customer(session, customer_segment.segment_id, customer_segment)
                await delete_segment_customer_condition(session, customer_segment.segment_id)
            else:
                existing_segment = await get_customer_segment_by_name(session, name=customer_segment.name,
                                                                      org_id=org_id)
                if existing_segment:
                    return {'code': 300, "msg": get_message("segment_name_exists", lang)}
                insert_segment = await create_segment_customer(session, customer_segment, user_id, org_id)
                insert_segment_id = insert_segment.segment_id
            await create_segment_customer_condition(session, insert_segment_id, segment.segment_condition)
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        await delete_segment_schedule(session, insert_segment_id, segment.segment_type.value)
        await create_segment_schedule(session, insert_segment_id, segment.segment_type.value, segment.segment_schedule)
        if segment.segment.create_type == 'condition':
            await run_segment_cleaning(segment.segment_type.value, insert_segment_id, segment.segment.condition_type,
                                       org_id, session)
        return {'code': 200, "segment_id": insert_segment_id, "msg": get_message("segment_submitted", lang)}
    except Exception as e:
        app_logger.error(f"Error Submit Segments: {str(e)}")
        return {'code': 300, "msg": str(e)}


@router.delete("/delete")
async def delete_segments(
        segment_id: int,
        segment_type: Segment_Type,
        lang: str = Query("en"),
        session=Depends(get_db),
        user_id: int = Depends(get_current_user)
):
    try:
        if segment_type == Segment_Type.item:
            in_use = await get_item_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": get_message('segment_in_use', lang)}
            await delete_segment_item(session, segment_id)
            await delete_segment_item_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        elif segment_type == Segment_Type.location:
            in_use = await get_location_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": get_message('segment_in_use', lang)}
            await delete_segment_location(session, segment_id)
            await delete_segment_location_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        elif segment_type == Segment_Type.customer:
            in_use = await get_customer_segments_in_use_by_id(session, segment_id)
            if in_use:
                return {'code': 301, "msg": get_message('segment_in_use', lang)}
            await delete_segment_customer(session, segment_id)
            await delete_segment_customer_condition(session, segment_id)
            await delete_segment_schedule(session, segment_id, segment_type.value)
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        data = {'code': 200, "msg": get_message('segment_deleted', lang)}
        app_logger.info(data)
        return data
    except Exception as e:
        app_logger.error(f"Error Delete Segments: {str(e)}")
        return {'code': 301, "msg": str(e)}


@router.post("/update_status")
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


@router.get("/details")
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
            show_column = app_config.dict_config['item_column']
        elif segment_type == Segment_Type.location:
            segments_detail = await get_setgments_location_detail(session, segment_id, key_word, page, page_size)
            show_column = app_config.dict_config['location_column']
        elif segment_type == Segment_Type.customer:
            segments_detail = await get_segments_customer_detail(session, segment_id, key_word, page, page_size)
            show_column = app_config.dict_config['customer_column']
        else:
            return {'code': 300, "msg": "Invalid segment type."}

        return {
            "code": 200,
            "show_column": show_column,
            "segments_detail": segments_detail if segment_id > 0 else [],
        }
    except Exception as e:
        app_logger.error(f"Error reading segments details: {str(e)}")
        return {'code': 301, "msg": str(e)}


@router.get("/get_store_list")
async def read_store_list(key_word: str = None,
                          org_id: str = Query(None, description="ORG ID"),
                          page: int = 1,
                          page_size: int = 40,
                          session=Depends(get_db), user_id=Depends(get_current_user)):
    try:
        store_list = await get_store_list_by_org_id(session, key_word, org_id, page, page_size)
        return {'code': 200, 'store_list': store_list}
    except Exception as e:
        app_logger.error(f"Error reading store list: {str(e)}")
        return {'code': 301, "msg": str(e)}
