import json

import pandas as pd
from fastapi import APIRouter, Query, UploadFile, File, HTTPException, status, Body, Depends

from models.model import SegmentsItem, SegmentsCustomer, SegmentsCustomerDetail, SegmentsLocation, \
    SegmentsLocationDetail, SegmentsItemDetail
from service import get_db
from typing import List, Union

from service.segments_service import get_item_segment_by_name, get_customer_segment_by_name, \
    get_location_segment_by_name, create_segment_location, create_segment_item, create_segment_customer, \
    delete_segment_item_detail, delete_segment_customer_detail, delete_segment_location_detail, \
    create_item_segments_detail, create_customer_segments_detail, create_location_segments_detail, update_segment_item, \
    update_segment_customer, update_segment_location, delete_segment_import, create_segment_import
from utils.upload_utils import validate_and_read_file, standardize_columns

from utils.logger import app_logger

router = APIRouter(tags=["segments"])


@router.post("/submit_segment_v2")
async def upload_segment(submit: Union[dict, str, None] = Body(None),
                         uFile: UploadFile = File(None),
                         preview: bool = Query(False, description="是否为预览模式"),
                         session=Depends(get_db)):
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
            upload_data = standardize_columns(upload_data)
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

        SegmentClass, DetailClass, get_segment_by_name_func, create_segment_func = segment_classes[segment_type]

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
            if existing_segment:
                return {'code': 300, "msg": f"{name} segment with this name already exists."}
            insert_segment = await create_segment_func(session, segment)
            insert_segment_id = insert_segment.segment_id

        sub_count = 0
        if not upload_data.empty:
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

            detail_creator = detail_creation_map[segment_type]
            details = [
                DetailClass(segment_id=insert_segment_id, **row.to_dict())
                for _, row in upload_data.iterrows()
            ]
            await detail_creator(session, insert_segment_id, details)

            await delete_segment_import(session, segment_id=insert_segment_id, segment_type=segment_type)
            await create_segment_import(session, insert_segment_id, segment_type, file_name, len(details))
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
            'segment_status': 'active',
            'create_type': 'import'
        }

        if sub_count > 0:
            segment_data['sub_count'] = sub_count

        segment = SegmentClass(**segment_data)
        await update_segment_func(session, insert_segment_id, segment)

        return {'code': 200, "segment_id": insert_segment_id, "msg": "Segment submitted successfully."}
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
