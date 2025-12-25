from fastapi import APIRouter, HTTPException, Depends, Request
from service.promotion import process_promotion_data, process_promotion_termination
from service.segments_service import get_segments_item_detail, get_segments_by_phone, process_segment_data
from worker_api.worker_schemas import WorkerCallBack
from service.worker import get_worker_next_task, update_worker_task
from service import get_db
import hashlib
import hmac
import time

from utils.logger import app_logger

router = APIRouter()


def verify_signature(headers: dict, secret_key: str) -> bool:
    """
    验签函数（使用 HMAC-SHA256 签名）
    :param headers: 请求头中的参数字典（不包含 signature 自身）
    :param secret_key: 约定的密钥
    :return: 是否通过验签
    """

    # 获取签名和时间戳
    signature = headers.get("x-signature")
    timestamp = headers.get("x-timestamp")

    if not signature or not timestamp:
        app_logger.warning("Missing signature or timestamp in headers")
        return False

    # 验证时间戳是否为有效数字
    try:
        request_time = int(timestamp)
        app_logger.info(f"Request timestamp: {request_time}")
    except ValueError:
        app_logger.warning("Invalid timestamp format")
        return False

    #
    # current_time = int(time.time())
    # app_logger.debug(f"Current timestamp: {current_time}")
    # if abs(current_time - request_time) > 300:  # 5分钟
    #     app_logger.warning("Timestamp out of valid range")
    #     return False
    allowed_keys = ["x-timestamp", "location-id", "terminal-id"]
    sorted_params = sorted(
        (k.lower(), v) for k, v in headers.items()
        if k.lower() in allowed_keys
    )
    param_str = "&".join([f"{k}={v}" for k, v in sorted_params])
    app_logger.info(f"Parameters string for signature: {param_str}")

    expected_sign = hmac.new(
        secret_key.encode("utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    app_logger.info(f"Expected signature: {expected_sign}")
    return hmac.compare_digest(signature, expected_sign)


async def verify_header_signature(request: Request):
    """
    从请求头中提取参数并进行验签
    """
    app_logger.info(f"Verifying header request body: {dict(request).items()}")
    headers = {k.lower(): v for k, v in dict(request.headers).items()}

    logged_headers = {k: v for k, v in headers.items()
                      if k.lower() not in ["authorization"]}
    app_logger.info(f"Verifying header signature with headers: {logged_headers}")

    secret_key = "5faa8e3b095f41480cab2f4b6b70d0cd"

    if not verify_signature(headers, secret_key):
        app_logger.error("Header signature verification failed")
        raise HTTPException(status_code=400, detail="signature verification failed")

# dependencies=[Depends(verify_header_signature)]
@router.get("/worker_api/get_promotion_by_phone")
async def get_promotion_by_phone(phone_number: str, session=Depends(get_db)):
    try:
        promotion_data = await get_segments_by_phone(session, phone_number)

        return {
            "code": 300 if promotion_data is None else 200,
            "msg": 'no data' if promotion_data is None else '',
            "data": promotion_data
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": str(e)
        }

# , dependencies=[Depends(verify_header_signature)]
@router.get("/worker_api/get_data")
async def get_task_data(location_id: int, terminal_id: int, session=Depends(get_db)):
    """
    获取任务（带 Header 验签）
    :param session:
    :param location_id:
    :param terminal_id:
    :return:
    """

    try:
        app_logger.info(f"Getting task data for location_id: {location_id}, terminal_id: {terminal_id}")
        worker_next_task = await get_worker_next_task(session, location_id, terminal_id)
        if worker_next_task is None:
            return {
                "code": 300,
                "msg": "no data"
            }
        session_id = worker_next_task.session_id
        data_key = worker_next_task.data_key
        data_type = worker_next_task.data_type
        termination = worker_next_task.termination

        if data_type == 'promotion':
            try:
                if termination == 1:
                    data_detail = await process_promotion_termination(data_key, session, location_id)
                else:
                    data_detail = await process_promotion_data(data_key, session, location_id)
            except Exception as e:
                data_detail = []
                await update_worker_task(session, location_id, terminal_id, session_id, 'E',
                                         f"Error in Process promotion Data {str(e)}")
                app_logger.error(f"Error in process_promotion_data: {str(e)}", exc_info=True)
        elif data_type == 'segment_item':
            data_detail = await process_segment_data(data_key, session)
        else:
            return {"code": 301, "msg": "data_type is not support"}

        task_data = {
            "code": 200,
            "msg": "",
            "data_header": {'data_type': data_type, 'session_id': session_id},
            "data_detail": data_detail
        }

        app_logger.info(f"Returning task data: {task_data}")
        return task_data
    except Exception as e:
        app_logger.error(f"Error in get_task_data: {str(e)}", exc_info=True)
        return {"code": 500, "msg": str(e)}


@router.post("/worker_api/call_back")
async def call_back(data: WorkerCallBack, session=Depends(get_db)):
    """
     获取任务（带 Header 验签）
    :return:
    """
    try:
        # 更新当前任务状态
        await update_worker_task(session, data.location_id, data.terminal_id, data.session_id, data.status, data.msg)

        # 获取下一个任务
        worker_next_task = await get_worker_next_task(session, data.location_id, data.terminal_id)

        # 构造返回结果
        response = {
            'code': 200,
            'message': 'success',
            'next_session': worker_next_task.session_id if worker_next_task else None
        }
    except Exception as e:
        # 捕获所有异常并返回错误信息
        response = {
            'code': 500,
            'message': f'error: {str(e)}',
            'next_session': None
        }

    return response


@router.post("/worker_api/call_back_data")
async def call_back_data(data: dict):
    """
    任务回调
    :param data:
    :return:
    """
    return {'code': 200, "message": "success", "next_session": None}
