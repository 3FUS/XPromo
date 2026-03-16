from fastapi import APIRouter, Depends, Request, HTTPException, status
from typing import Optional
from datetime import datetime

from sqlalchemy import and_

from models.model import SamCompetitorSales
from schemas.competitor_sales import CompetitorSalesCreate

from service import get_db
from service.competitor_sales_service import create_competitor_sale
import hashlib
import hmac

from utils.logger import app_logger

router = APIRouter(prefix="/sales", tags=["sales"])

COMPETITOR_SALES_SIGNATURE_KEY = "64afbfe1cb2245818251bc8f8e08aa90"


def generate_competitor_sales_signature(store_code: str, sale_date: str) -> str:
    """
    生成竞争品牌销售数据的签名

    Args:
        store_code: 店铺代码
        sale_date: 销售日期 (格式: YYYY-MM-DD)

    Returns:
        HMAC-SHA256 签名字符串
    """
    # 构造待签名的字符串
    sign_string = f"store_code={store_code}&sale_date={sale_date}"

    # 使用 HMAC-SHA256 生成签名
    signature = hmac.new(
        COMPETITOR_SALES_SIGNATURE_KEY.encode('utf-8'),
        sign_string.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()

    app_logger.info(f"Generated signature for store_code={store_code}, sale_date={sale_date}: {signature}")
    return signature


def verify_competitor_sales_signature(store_code: str, sale_date: str, signature: str) -> bool:
    # 生成期望的签名
    expected_signature = generate_competitor_sales_signature(store_code, sale_date)
    app_logger.info(f"Expected signature for store_code={store_code}, sale_date={sale_date}: {expected_signature}")
    # 使用安全比较防止时序攻击
    is_valid = hmac.compare_digest(signature, expected_signature)

    app_logger.info(
        f"Signature verification for store_code={store_code}, sale_date={sale_date}: {'PASS' if is_valid else 'FAIL'}")
    return is_valid


async def verify_header_signature(request: Request):
    try:
        # body = await request.json()
        # store_code = body.get('store_code')
        # sale_date = body.get('sale_date')
        # signature = body.get('signature')
        app_logger.info(f"Verifying header request body: {request.headers}")
        store_code = request.headers.get('store_code')
        sale_date = request.headers.get('sale_date')
        signature = request.headers.get('signature')

        if not store_code or not sale_date or not signature:
            raise HTTPException(status_code=400, detail=f"signature verification failed: missing parameters")

        app_logger.info(f"Verifying signature for store_code={store_code}, sale_date={sale_date}")

        is_valid = verify_competitor_sales_signature(str(store_code), str(sale_date), str(signature))

        if is_valid:
            return {
                "code": 200,
                "msg": "签名验证通过",
            }
        else:
            raise HTTPException(status_code=400, detail=f"signature verification failed: invalid signature")
    except Exception as e:
        app_logger.error(f"验证签名失败: {str(e)}")
        raise HTTPException(status_code=400, detail=f"signature verification failed: {str(e)}")


@router.post("/submit", dependencies=[Depends(verify_header_signature)], summary="创建竞争品牌销售记录")
async def create_sale(
        sale_data: CompetitorSalesCreate,
        session=Depends(get_db)
):
    """
    创建单条竞争品牌销售记录

    Args:
        sale_data: 销售记录数据
        session: 数据库会话
        user_id: 当前用户ID

    Returns:
        创建成功的销售记录
    """
    try:

        result = await create_competitor_sale(session, sale_data)
        return {
            "code": 200,
            "msg": "竞争品牌销售记录创建成功",
            "data": result
        }
    except Exception as e:
        app_logger.error(f"创建竞争品牌销售记录失败: {str(e)}")
        return {
            "code": 301,
            "msg": f"创建失败: {str(e)}"
        }


@router.post("/page_load", dependencies=[Depends(verify_header_signature)])
async def page_load():
    return {
        "code": 200,
        "msg": ""
    }


@router.get("/brands", summary="获取竞争品牌清单")
async def get_competitor_brands():
    """
    获取可用的竞争品牌清单
    """
    brands = [
        "TUMI",
        "Victorinox",
        "LOJEL",
        "Crown (Travel Station)",
        "Eminnet",
        "愛力 (Echolac)",
        "祥銓",
        "ACE",
        "Doris",
        "DELSEY",
        "Departure",
        "PIQUAERO",
        "pacsafe",
        "Porter",
        "KIPLING"
    ]

    return {
        "code": 200,
        "msg": "品牌清单获取成功",
        "data": {
            "brands": brands,
            "count": len(brands)
        }
    }


@router.get("/check-data-exists", dependencies=[Depends(verify_header_signature)], summary="检查数据是否存在")
async def check_data_exists(
        store_code: int,
        sale_date: str,
        session=Depends(get_db)
):
    """
    回调接口，用于检查指定的店铺代码和销售日期是否有记录存在于数据库中

    Args:
        store_code: 店铺代码
        sale_date: 销售日期 (格式: YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS)
        session: 数据库会话

    Returns:
        包含是否存在记录的信息
    """
    try:
        # 将输入的日期字符串转换为datetime对象
        if len(sale_date) <= 10:  # 如果只有日期部分
            parsed_date = datetime.strptime(sale_date, '%Y-%m-%d')
        else:
            parsed_date = datetime.strptime(sale_date, '%Y-%m-%d')

        # 查询数据库中是否有匹配的记录
        count = session.query(SamCompetitorSales).filter(
            and_(
                SamCompetitorSales.location_id == store_code,
                SamCompetitorSales.sale_date == parsed_date
            )
        ).count()

        exists = count > 0

        return {
            "code": 200,
            "msg": "Successful",
            "data": {
                "store_code": store_code,
                "sale_date": sale_date,
                "exists": exists,
                "record_count": count
            }
        }
    except ValueError:
        return {
            "code": 400,
            "msg": "日期格式错误，请使用 YYYY-MM-DD 格式",
            "data": {
                "store_code": store_code,
                "sale_date": sale_date,
                "exists": False,
                "record_count": 0
            }
        }
    except Exception as e:
        app_logger.error(f"检查数据存在性失败: {str(e)}")
        return {
            "code": 500,
            "msg": f"查询失败: {str(e)}",
            "data": {
                "store_code": store_code,
                "sale_date": sale_date,
                "exists": False,
                "record_count": 0
            }
        }
