from fastapi import APIRouter, Query, HTTPException, Depends
from typing import Optional
from datetime import datetime

from service import get_db
from service.commission_pattern_service import (
    get_commission_pattern_list,
    get_commission_pattern_by_id,
    create_commission_pattern,
    update_commission_pattern,
    delete_commission_pattern,
    get_commission_pattern_category_list,
    create_commission_pattern_category,
    delete_or_deactivate_commission_pattern_category,
    get_commission_pattern_brand_list,
    create_commission_pattern_brand,
    delete_or_deactivate_commission_pattern_brand
)
from schemas.commission_pattern import (
    CommissionPatternCreate,
    CommissionPatternUpdate,
    CommissionPatternQueryParams,
    CommissionPatternCategoryCreate,
    CommissionPatternBrandCreate
)
from models.sam_commissionpattern import CommissionPattern

from service.worker import create_worker_task
from core.security import get_current_user
from utils.logger import app_logger

router = APIRouter(prefix="/commission_pattern", tags=["commission_pattern"])


# ==================== CommissionPattern 路由 ====================

@router.get("/list")
async def list_commission_patterns(
        key_word: Optional[str] = Query(None, description="关键字，用于模糊查询location_id、brand_code、category_code"),
        status: Optional[str] = Query('ALL', description="状态筛选"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    获取佣金模式列表（支持分页和筛选）
    """
    try:
        params = CommissionPatternQueryParams(
            key_word=key_word,
            status=status,
            page=page,
            page_size=page_size
        )
        result = await get_commission_pattern_list(session, params)
        return {'code': 200, 'data': result}
    except Exception as e:
        app_logger.error(f"Error listing commission patterns: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.get("/get_detail_by_pattern_id")
async def get_commission_pattern(
        commission_pattern_id: int,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    根据ID获取佣金模式详情
    """
    try:
        pattern = await get_commission_pattern_by_id(session, commission_pattern_id)
        if not pattern:
            return {'code': 404, 'msg': 'Commission pattern not found'}
        return {'code': 200, 'data': pattern}
    except Exception as e:
        app_logger.error(f"Error getting commission pattern: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.post("/submit")
async def create_new_commission_pattern(
        pattern_data: CommissionPatternCreate,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    创建佣金模式
    """
    try:
        new_pattern = await create_commission_pattern(session, pattern_data)
        return {'code': 200, 'msg': 'Commission pattern created successfully', 'data': new_pattern}
    except ValueError as e:
        return {'code': 400, 'msg': str(e)}
    except Exception as e:
        app_logger.error(f"Error creating commission pattern: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.post("/update")
async def update_existing_commission_pattern(
        commission_pattern_id: int,
        pattern_data: CommissionPatternUpdate,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    更新佣金模式
    """
    try:
        updated_pattern = await update_commission_pattern(session, commission_pattern_id, pattern_data)
        if not updated_pattern:
            return {'code': 404, 'msg': 'Commission pattern not found'}
        return {'code': 200, 'msg': 'Commission pattern updated successfully', 'data': updated_pattern}
    except Exception as e:
        app_logger.error(f"Error updating commission pattern: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.get("/export")
async def export_commission_patterns(
        commission_pattern_ids: str = Query(..., description="佣金模式ID，支持单个ID或多个ID（逗号分隔）"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """

    """
    try:
        # 解析ID列表，支持单个ID或多个ID（逗号分隔）
        id_list = [int(id.strip()) for id in commission_pattern_ids.split(',') if id.strip()]

        if not id_list:
            return {'code': 400, 'msg': 'No valid commission pattern IDs provided'}

        # 根据ID查询佣金模式
        patterns = session.query(CommissionPattern).filter(
            CommissionPattern.commission_pattern_id.in_(id_list)
        ).all()

        if not patterns:
            return {'code': 404, 'msg': 'No commission patterns found for the provided IDs'}

        # 为每个commission_pattern_id创建worker任务
        session_ids = []
        for pattern in patterns:
            # 为单个location_id创建worker任务
            sessionId = await create_worker_task(
                session,
                [pattern.location_id],
                'commission_pattern',
                str(pattern.commission_pattern_id)
            )
            session_ids.append(sessionId)

            app_logger.info(
                f"Export commission pattern: ID={pattern.commission_pattern_id}, LocationID={pattern.location_id}, SessionID={sessionId}")

        return {'code': 200, 'msg': 'Commission patterns exported successfully', 'data': session_ids}
    except ValueError as e:
        app_logger.error(f"Invalid commission pattern ID format: {str(e)}")
        return {'code': 400, 'msg': f'Invalid ID format: {str(e)}'}
    except Exception as e:
        app_logger.error(f"Error exporting commission patterns: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.delete("/{commission_pattern_id}")
async def delete_existing_commission_pattern(
        commission_pattern_id: int,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    删除佣金模式
    """
    try:
        success = await delete_commission_pattern(session, commission_pattern_id)
        if not success:
            return {'code': 404, 'msg': 'Commission pattern not found'}
        return {'code': 200, 'msg': 'Commission pattern deleted successfully'}
    except Exception as e:
        app_logger.error(f"Error deleting commission pattern: {str(e)}")
        return {'code': 500, 'msg': str(e)}


# ==================== CommissionPatternCategory 路由 ====================

@router.get("/category/list")
async def list_commission_pattern_categories(
        status: Optional[str] = Query('active', description="状态筛选"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    获取佣金模式分类列表
    """
    try:
        result = await get_commission_pattern_category_list(session, status, page, page_size)
        return {'code': 200, 'data': result}
    except Exception as e:
        app_logger.error(f"Error listing commission pattern categories: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.post("/category/")
async def create_new_commission_pattern_category(
        category_data: CommissionPatternCategoryCreate,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    创建佣金模式分类
    """
    try:
        new_category = await create_commission_pattern_category(session, category_data)
        return {'code': 200, 'msg': 'Category created successfully', 'data': new_category}
    except ValueError as e:
        return {'code': 400, 'msg': str(e)}
    except Exception as e:
        app_logger.error(f"Error creating commission pattern category: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.delete("/category/{category_code}")
async def delete_commission_pattern_category_route(
        category_code: str,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    删除或停用佣金模式分类
    - 如果分类正在被使用，则将其状态改为 inactive
    - 如果分类未被使用，则直接删除
    """
    try:
        result = await delete_or_deactivate_commission_pattern_category(session, category_code, user_id)
        if not result['success']:
            return {'code': 404, 'msg': result['message']}
        return {'code': 200, 'msg': result['message'], 'action': result['action']}
    except Exception as e:
        app_logger.error(f"Error deleting commission pattern category: {str(e)}")
        return {'code': 500, 'msg': str(e)}


# ==================== CommissionPatternBrand 路由 ====================

@router.get("/brand/list")
async def list_commission_pattern_brands(
        status: Optional[str] = Query(None, description="状态筛选"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    获取佣金模式品牌列表
    """
    try:
        result = await get_commission_pattern_brand_list(session, status, page, page_size)
        return {'code': 200, 'data': result}
    except Exception as e:
        app_logger.error(f"Error listing commission pattern brands: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.post("/brand/")
async def create_new_commission_pattern_brand(
        brand_data: CommissionPatternBrandCreate,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    创建佣金模式品牌
    """
    try:
        new_brand = await create_commission_pattern_brand(session, brand_data)
        return {'code': 200, 'msg': 'Brand created successfully', 'data': new_brand}
    except ValueError as e:
        return {'code': 400, 'msg': str(e)}
    except Exception as e:
        app_logger.error(f"Error creating commission pattern brand: {str(e)}")
        return {'code': 500, 'msg': str(e)}


@router.delete("/brand/{brand_code}")
async def delete_commission_pattern_brand_route(
        brand_code: int,
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    """
    删除或停用佣金模式品牌
    - 如果品牌正在被使用，则将其状态改为 inactive
    - 如果品牌未被使用，则直接删除
    """
    try:
        result = await delete_or_deactivate_commission_pattern_brand(session, brand_code, user_id)
        if not result['success']:
            return {'code': 404, 'msg': result['message']}
        return {'code': 200, 'msg': result['message'], 'action': result['action']}
    except Exception as e:
        app_logger.error(f"Error deleting commission pattern brand: {str(e)}")
        return {'code': 500, 'msg': str(e)}
