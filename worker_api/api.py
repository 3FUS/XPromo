from fastapi import APIRouter, HTTPException

from service.promotion import process_promotion_data
from service.segments_service import get_segments_item_detail, get_segments_by_phone, process_segment_data
from worker_api.worker_schemas import WorkerCallBack

from service.worker import get_worker_next_task, update_worker_task

router = APIRouter()

from fastapi import Depends, Request
import hashlib

from service import get_db
#
# import yaml
#
# mapping_file = open('./config/mapping.yaml', 'r', encoding='utf-8')
# mapping_config = yaml.safe_load(mapping_file)
# promotion_mapping = mapping_config.get("promotion_mapping", {})
# segment_mapping = mapping_config.get("segment_mapping", {})

def verify_signature(headers: dict, secret_key: str) -> bool:
    """
    验签函数（示例使用简单拼接 + MD5 签名）
    :param headers: 请求头中的参数字典（不包含 signature 自身）
    :param signature: 客户端传入的签名值
    :param secret_key: 约定的密钥
    :return: 是否通过验签
    """
    # 排序参数 key 并拼接成字符串
    sorted_params = sorted(headers.items())
    param_str = "&".join([f"{k}={v}" for k, v in sorted_params]) + f"&secret_key={secret_key}"

    # 使用 hashlib 或其他方式生成签名（这里以 md5 为例）
    sign = hashlib.md5(param_str.encode("utf-8")).hexdigest()

    # # 按首字母排序后进行MD5加密
    # sign_params = {
    #     "callerService": x_caller_service,
    #     "contextPath": context_path,
    #     "timestamp": x_caller_timestamp,
    #     "v": version,
    #     # "serviceSecret": config["serviceSecret"],
    #     "requestPath": rest_path
    # }
    # sorted_params = sorted(sign_params.items())
    # sign_string = config["serviceSecret"] + "".join([f"{k}{v}" for k, v in sorted_params]) + config["serviceSecret"]
    # expected_sign = md5(sign_string.encode()).hexdigest().upper()  # 修改: 最终生成的MD5签名被转换为大写形式
    #
    return sign == headers.get("signature")


async def verify_header_signature(request: Request):
    """
    从请求头中提取参数并进行验签
    """
    headers = dict(request.headers)
    secret_key = "123456"  # 建议从配置文件读取

    if not verify_signature(headers, secret_key):
        raise HTTPException(status_code=400, detail="验签失败")

#
# async def get_segment_data(segment_id: int, session):
#     data_detail = []
#     item_data = await get_segments_item_detail(session, segment_id, None, page=1, page_size=1000)
#
#     segment_status = 'active'
#     begin_date = '1900-01-01 00:00:00'
#
#     ITM_ITEM_DEAL_PROP = []
#     if segment_status == 'active':
#         if item_data['total'] > 0:
#             for item in item_data['data']:
#                 ITM_ITEM_DEAL_PROP.append({
#                     **segment_mapping["ITM_ITEM_DEAL_PROP"],
#                     "item_id": item.item_id,
#                     "effective_date": begin_date,
#                     "itm_deal_property_code": f"ITM_PROP_{segment_id}"
#                 })
#     data_detail.append(
#         {'table': 'ITM_ITEM_DEAL_PROP', 'table_key': ['organization_id', 'itm_deal_property_code'],
#          "action": "DELETE_AND_INSERT",
#          "data": ITM_ITEM_DEAL_PROP})
#     return data_detail


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

        worker_next_task = await get_worker_next_task(session, location_id, terminal_id)
        if worker_next_task is None:
            return {
                "code": 300,
                "msg": "no data"
            }
        session_id = worker_next_task.session_id
        data_key = worker_next_task.data_key
        data_type = worker_next_task.data_type

        if data_type == 'promotion':
            data_detail = await process_promotion_data(data_key, session, location_id)
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

        return task_data
    except Exception as e:
        print(e)
        return {"code": 500, "msg": f"error{e}"}


@router.post("/worker_api/call_back")
async def call_back(data: WorkerCallBack, session=Depends(get_db)):
    """
    任务回调
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
