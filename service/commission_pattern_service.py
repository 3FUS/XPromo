from utils.logger import app_logger
from sqlalchemy.orm import Session
from models.sam_commissionpattern import CommissionPattern, CommissionPatternCategory, CommissionPatternBrand
from schemas.commission_pattern import (
    CommissionPatternCreate,
    CommissionPatternUpdate,
    CommissionPatternCategoryCreate,
    CommissionPatternBrandCreate,
    CommissionPatternQueryParams
)
from models.model import LOC_ORG_HIERARCHY, WorkerTask
from datetime import datetime
from typing import List, Optional, Dict, Any
from sqlalchemy import case, func, text
from service.segments_service import generate_segment_id


# ==================== CommissionPattern 服务 ====================

async def get_commission_pattern_list(
        session: Session,
        params: CommissionPatternQueryParams
) -> Dict[str, Any]:
    """
    获取佣金模式列表（支持分页和筛选）
    """
    try:
        # 使用 join 关联分类和品牌表
        query = session.query(
            CommissionPattern,
            CommissionPatternCategory.category_name,
            CommissionPatternBrand.brand_name,
            LOC_ORG_HIERARCHY.DESCRIPTION,
            func.sum(case((WorkerTask.status == 'N', 1), else_=0)).label('count_N'),
            func.sum(case((WorkerTask.status == 'E', 1), else_=0)).label('count_E'),
            func.sum(case((WorkerTask.status == 'D', 1), else_=0)).label('count_D')
        ).outerjoin(
            CommissionPatternCategory,
            CommissionPattern.category_code == CommissionPatternCategory.category_code
        ).outerjoin(
            CommissionPatternBrand,
            CommissionPattern.brand_code == CommissionPatternBrand.brand_code
        ).outerjoin(
            LOC_ORG_HIERARCHY,
            (CommissionPattern.location_id == LOC_ORG_HIERARCHY.ORG_VALUE) &
            (LOC_ORG_HIERARCHY.ORG_CODE == 'STORE')
        ).outerjoin(
            WorkerTask,
            CommissionPattern.last_session_id == WorkerTask.session_id
        ).group_by(CommissionPattern, CommissionPatternCategory.category_name, CommissionPatternBrand.brand_name,
                   LOC_ORG_HIERARCHY.DESCRIPTION)

        # 关键字模糊查询
        if params.key_word:
            key_word = f"%{params.key_word}%"
            query = query.filter(
                (CommissionPattern.location_id.like(key_word)) |
                (CommissionPattern.brand_code.like(key_word)) |
                (CommissionPattern.category_code.like(key_word))
            )

        # 状态筛选
        if params.status != 'ALL':
            query = query.filter(CommissionPattern.status == params.status)

        # 获取总数
        total = query.count()

        # 分页查询
        items = query.order_by(
            CommissionPattern.create_time.desc()
        ).offset((params.page - 1) * params.page_size).limit(params.page_size).all()

        # 格式化结果，包含分类名称和品牌名称
        formatted_data = []
        for pattern, category_name, brand_name, store_name,count_N, count_E, count_D in items:
            pattern_dict = {
                "commission_pattern_id": pattern.commission_pattern_id,
                "location_id": pattern.location_id,
                "store_name": store_name,
                "brand_code": pattern.brand_code,
                "brand_name": brand_name,
                "category_code": pattern.category_code,
                "category_name": category_name,
                "start_date": pattern.start_date,
                "end_date": pattern.end_date,
                "p_value": pattern.p_value,
                "s_value": pattern.s_value,
                "status": pattern.status,
                'export_status_counts': {
                    'New': count_N or 0,
                    'Error': count_E or 0,
                    'Done': count_D or 0
                },
                "last_export_time": pattern.last_export_time,
                "create_time": pattern.create_time,
                "create_user": pattern.create_user,
                "update_time": pattern.update_time,
                "update_user": pattern.update_user
            }
            formatted_data.append(pattern_dict)

        return {
            "total": total,
            "page": params.page,
            "page_size": params.page_size,
            "data": formatted_data
        }
    except Exception as e:
        app_logger.error(f"Error getting commission pattern list: {str(e)}")
        raise e

async def update_commission_pattern_export_time(session, commission_pattern_id, last_export_time, last_session_id):
    updated_data = session.query(CommissionPattern).filter(CommissionPattern.commission_pattern_id == commission_pattern_id).first()
    if updated_data:
        updated_data.last_export_time = last_export_time
        updated_data.last_session_id = last_session_id
        session.commit()
        session.refresh(updated_data)
    return updated_data

async def get_commission_pattern_by_id(
        session: Session,
        commission_pattern_id: int
) -> Optional[CommissionPattern]:
    """
    根据ID获取佣金模式详情
    """
    try:
        result = session.query(
            CommissionPattern,
            CommissionPatternCategory.category_name,
            CommissionPatternBrand.brand_name
        ).outerjoin(
            CommissionPatternCategory,
            CommissionPattern.category_code == CommissionPatternCategory.category_code
        ).outerjoin(
            CommissionPatternBrand,
            CommissionPattern.brand_code == CommissionPatternBrand.brand_code
        ).filter(
            CommissionPattern.commission_pattern_id == commission_pattern_id
        ).first()

        if not result:
            return None
        pattern, category_name, brand_name = result

        return {
            "commission_pattern_id": pattern.commission_pattern_id,
            "location_id": pattern.location_id,
            "brand_code": pattern.brand_code,
            "brand_name": brand_name,
            "category_code": pattern.category_code,
            "category_name": category_name,
            "start_date": pattern.start_date,
            "end_date": pattern.end_date,
            "p_value": pattern.p_value,
            "s_value": pattern.s_value,
            "status": pattern.status,
            "last_export_time": pattern.last_export_time,
            "create_time": pattern.create_time,
            "create_user": pattern.create_user,
            "update_time": pattern.update_time,
            "update_user": pattern.update_user
        }
    except Exception as e:
        app_logger.error(f"Error getting commission pattern by id: {str(e)}")
        raise e


async def create_commission_pattern(
        session: Session,
        pattern_data: CommissionPatternCreate
) -> CommissionPattern:
    """
    创建佣金模式
    """
    try:
        new_pattern = CommissionPattern(
            commission_pattern_id=generate_segment_id(session, "CommissionPattern"),
            location_id=pattern_data.location_id,
            brand_code=pattern_data.brand_code,
            category_code=pattern_data.category_code,
            start_date=pattern_data.start_date,
            end_date=pattern_data.end_date,
            p_value=pattern_data.p_value,
            s_value=pattern_data.s_value,
            status=pattern_data.status,
            create_time=datetime.now(),
            create_user=pattern_data.create_user
        )

        session.add(new_pattern)
        session.commit()
        session.refresh(new_pattern)

        app_logger.info(f"Created commission pattern: id={new_pattern.commission_pattern_id}, "
                        f"location_id={pattern_data.location_id}, brand_code={pattern_data.brand_code}")
        return new_pattern
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating commission pattern: {str(e)}")
        raise e


async def update_commission_pattern(
        session: Session,
        commission_pattern_id: int,
        pattern_data: CommissionPatternUpdate
) -> Optional[CommissionPattern]:
    """
    更新佣金模式
    """
    try:
        pattern = session.query(CommissionPattern).filter(
            CommissionPattern.commission_pattern_id == commission_pattern_id
        ).first()

        if not pattern:
            return None

        # 更新字段
        if pattern_data.location_id is not None:
            pattern.location_id = pattern_data.location_id
        if pattern_data.brand_code is not None:
            pattern.brand_code = pattern_data.brand_code
        if pattern_data.category_code is not None:
            pattern.category_code = pattern_data.category_code
        if pattern_data.start_date is not None:
            pattern.start_date = pattern_data.start_date
        if pattern_data.end_date is not None:
            pattern.end_date = pattern_data.end_date
        if pattern_data.p_value is not None:
            pattern.p_value = pattern_data.p_value
        if pattern_data.s_value is not None:
            pattern.s_value = pattern_data.s_value
        if pattern_data.status is not None:
            pattern.status = pattern_data.status

        pattern.update_time = datetime.now()
        pattern.update_user = pattern_data.update_user

        session.commit()
        session.refresh(pattern)

        app_logger.info(f"Updated commission pattern: id={commission_pattern_id}")
        return pattern
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error updating commission pattern: {str(e)}")
        raise e


async def delete_commission_pattern(
        session: Session,
        commission_pattern_id: int
) -> bool:
    """
    删除佣金模式
    """
    try:
        pattern = session.query(CommissionPattern).filter(
            CommissionPattern.commission_pattern_id == commission_pattern_id
        ).first()

        if not pattern:
            return False

        session.delete(pattern)
        session.commit()

        app_logger.info(f"Deleted commission pattern: id={commission_pattern_id}")
        return True
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting commission pattern: {str(e)}")
        raise e


async def get_commission_pattern_by_business_keys(
        session: Session,
        location_id: int,
        brand_code: int,
        category_code: str,
        start_date: datetime
) -> Optional[CommissionPattern]:
    """
    根据业务键获取佣金模式（用于重复检查）
    """
    try:
        return session.query(CommissionPattern).filter(
            CommissionPattern.location_id == location_id,
            CommissionPattern.brand_code == brand_code,
            CommissionPattern.category_code == category_code,
            CommissionPattern.start_date == start_date
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting commission pattern by business keys: {str(e)}")
        raise e


# ==================== CommissionPatternCategory 服务 ====================

async def get_commission_pattern_category_list(
        session: Session,
        status: Optional[str] = 'active',
        page: int = 1,
        page_size: int = 30
) -> Dict[str, Any]:
    """
    获取佣金模式分类列表
    """
    try:
        query = session.query(CommissionPatternCategory)

        if status:
            query = query.filter(CommissionPatternCategory.status == status)

        total = query.count()

        items = query.order_by(
            CommissionPatternCategory.sort_order.asc(),
            CommissionPatternCategory.create_time.desc()
        ).offset((page - 1) * page_size).limit(page_size).all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": items
        }
    except Exception as e:
        app_logger.error(f"Error getting commission pattern category list: {str(e)}")
        raise e


async def get_commission_pattern_category_by_code(
        session: Session,
        category_code: str
) -> Optional[CommissionPatternCategory]:
    """
    根据分类代码获取分类详情
    """
    try:
        return session.query(CommissionPatternCategory).filter(
            CommissionPatternCategory.category_code == category_code
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting commission pattern category by code: {str(e)}")
        raise e


async def create_commission_pattern_category(
        session: Session,
        category_data: CommissionPatternCategoryCreate
) -> CommissionPatternCategory:
    """
    创建佣金模式分类
    """
    try:
        # 检查是否已存在
        existing = await get_commission_pattern_category_by_code(
            session, category_data.category_code
        )
        if existing:
            if existing.status == 'active':
                existing.category_name = category_data.category_name
                existing.update_time = datetime.now()
                existing.update_user = category_data.create_user
                session.commit()
                session.refresh(existing)
                app_logger.info(f"update category name: {category_data.category_name}")
                return existing
                # raise ValueError("Category already exists")
            existing.status = 'active'
            existing.category_name = category_data.category_name
            existing.sort_order = category_data.sort_order
            existing.update_time = datetime.now()
            existing.update_user = category_data.create_user
            session.commit()
            session.refresh(existing)
            app_logger.info(f"Reactivated commission pattern category: {category_data.category_name}")
            return existing

        new_category = CommissionPatternCategory(
            category_code=category_data.category_code,
            category_name=category_data.category_name,
            status=category_data.status,
            sort_order=category_data.sort_order,
            create_time=datetime.now(),
            create_user=category_data.create_user
        )

        session.add(new_category)
        session.commit()
        session.refresh(new_category)

        app_logger.info(f"Created commission pattern category: {category_data.category_name}")
        return new_category
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating commission pattern category: {str(e)}")
        raise e


async def delete_or_deactivate_commission_pattern_category(
        session: Session,
        category_code: str,
        update_user: str = "system"
) -> Dict[str, Any]:
    """
    删除或停用佣金模式分类
    - 如果分类正在被使用，则将其状态改为 inactive
    - 如果分类未被使用，则直接删除
    """
    try:
        category = await get_commission_pattern_category_by_code(session, category_code)

        if not category:
            return {"success": False, "message": "Category not found"}

        # 检查是否有佣金模式在使用此分类
        usage_count = session.query(func.count(CommissionPattern.commission_pattern_id)).filter(
            CommissionPattern.category_code == category_code
        ).scalar()

        if usage_count > 0:
            # 正在使用，将状态改为 inactive
            category.status = 'inactive'
            session.commit()
            session.refresh(category)
            app_logger.info(f"Commission pattern category {category_code} is in use by {usage_count} patterns. "
                            f"Status changed to inactive.")
            return {
                "success": True,
                "message": f"Category is in use by {usage_count} patterns. Status changed to inactive.",
                "action": "deactivated",
                "usage_count": usage_count
            }
        else:
            # 未使用，直接删除
            session.delete(category)
            session.commit()
            app_logger.info(f"Deleted commission pattern category: {category_code}")
            return {
                "success": True,
                "message": "Category deleted successfully",
                "action": "deleted"
            }
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting or deactivating commission pattern category: {str(e)}")
        raise e


# ==================== CommissionPatternBrand 服务 ====================

async def get_commission_pattern_brand_list(
        session: Session,
        status: Optional[str] = 'active',
        page: int = 1,
        page_size: int = 30
) -> Dict[str, Any]:
    """
    获取佣金模式品牌列表
    """
    try:
        query = session.query(CommissionPatternBrand)

        if status:
            query = query.filter(CommissionPatternBrand.status == status)

        total = query.count()

        items = query.order_by(
            CommissionPatternBrand.sort_order.asc(),
            CommissionPatternBrand.create_time.desc()
        ).offset((page - 1) * page_size).limit(page_size).all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": items
        }
    except Exception as e:
        app_logger.error(f"Error getting commission pattern brand list: {str(e)}")
        raise e


async def get_commission_pattern_brand_by_code(
        session: Session,
        brand_code: str
) -> Optional[CommissionPatternBrand]:
    """
    根据品牌代码获取品牌详情
    """
    try:
        return session.query(CommissionPatternBrand).filter(
            CommissionPatternBrand.brand_code == brand_code
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting commission pattern brand by code: {str(e)}")
        raise e


async def create_commission_pattern_brand(
        session: Session,
        brand_data: CommissionPatternBrandCreate
) -> CommissionPatternBrand:
    """
    创建佣金模式品牌
    """
    try:
        existing = await get_commission_pattern_brand_by_code(
            session, brand_data.brand_code
        )
        if existing:
            if existing.status == 'active':
                existing.brand_name = brand_data.brand_name
                existing.update_time = datetime.now()
                existing.update_user = brand_data.create_user
                session.commit()
                session.refresh(existing)
                app_logger.info(f"update brand name: {brand_data.brand_name}")
                return existing
            existing.status = 'active'
            existing.brand_name = brand_data.brand_name
            existing.sort_order = brand_data.sort_order
            existing.update_time = datetime.now()
            existing.update_user = brand_data.create_user
            session.commit()
            session.refresh(existing)
            app_logger.info(f"Reactivated commission pattern brand: {brand_data.brand_name}")
            return existing

        new_brand = CommissionPatternBrand(
            brand_code=brand_data.brand_code,
            brand_name=brand_data.brand_name,
            status=brand_data.status,
            sort_order=brand_data.sort_order,
            create_time=datetime.now(),
            create_user=brand_data.create_user
        )

        session.add(new_brand)
        session.commit()
        session.refresh(new_brand)

        app_logger.info(f"Created commission pattern brand: {brand_data.brand_name}")
        return new_brand
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating commission pattern brand: {str(e)}")
        raise e

async def delete_or_deactivate_commission_pattern_brand(
        session: Session,
        brand_code: str,
        update_user: str = "system"
) -> Dict[str, Any]:
    """
    删除或停用佣金模式品牌
    - 如果品牌正在被使用，则将其状态改为 inactive
    - 如果品牌未被使用，则直接删除
    """
    try:
        brand = await get_commission_pattern_brand_by_code(session, brand_code)

        if not brand:
            return {"success": False, "message": "Brand not found"}

        # 检查是否有佣金模式在使用此品牌
        usage_count = session.query(func.count(CommissionPattern.commission_pattern_id)).filter(
            CommissionPattern.brand_code == brand_code
        ).scalar()

        if usage_count > 0:
            # 正在使用，将状态改为 inactive
            brand.status = 'inactive'
            session.commit()
            session.refresh(brand)
            app_logger.info(f"Commission pattern brand {brand_code} is in use by {usage_count} patterns. "
                            f"Status changed to inactive.")
            return {
                "success": True,
                "message": f"Brand is in use by {usage_count} patterns. Status changed to inactive.",
                "action": "deactivated",
                "usage_count": usage_count
            }
        else:
            # 未使用，直接删除
            session.delete(brand)
            session.commit()
            app_logger.info(f"Deleted commission pattern brand: {brand_code}")
            return {
                "success": True,
                "message": "Brand deleted successfully",
                "action": "deleted"
            }
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting or deactivating commission pattern brand: {str(e)}")
        raise e


import yaml

mapping_file = open('./config/mapping.yaml', 'r', encoding='utf-8')
mapping_config = yaml.safe_load(mapping_file)
commission_mapping = mapping_config.get("commission_mapping", {})


async def process_commission_pattern_data(commission_pattern_id: int, session, location_id):
    data_detail = []

    result_data = await get_commission_pattern_by_id(session, commission_pattern_id)

    if not result_data:
        app_logger.warning(f"Commission pattern not found for id: {commission_pattern_id}")
        return data_detail

    COM_CODE_VALUE = []
    if result_data.get('p_value') is not None:
        com_value_p = {
            **commission_mapping["COM_CODE_VALUE_P"],
            "code": result_data.get('category_code'),
            "property_code": 'P',
            "decimal_value": float(result_data.get('p_value'))
        }
        COM_CODE_VALUE.append(com_value_p)

    if result_data.get('s_value') is not None:
        com_value_s = {
            **commission_mapping["COM_CODE_VALUE_P"],
            "code": result_data.get('category_code'),
            "property_code": 'B',
            "decimal_value": float(result_data.get('s_value'))
        }
        COM_CODE_VALUE.append(com_value_s)

    current_time = datetime.now()
    status = result_data.get('status')
    start_date = result_data.get('start_date')
    end_date = result_data.get('end_date')

    is_active = (status == 'active') and (start_date <= current_time <= end_date)
    action = "INSERT_AND_UPDATE" if is_active else "DELETE"
    data_detail.append(
        {'table': 'COM_CODE_VALUE_P', 'table_key': ['organization_id', 'category', 'code', 'property_code'],
         "action": action,
         "data": COM_CODE_VALUE})

    data_detail.append(
        {'table': 'COM_CODE_VALUE', 'table_key': ['organization_id', 'category', 'code'],
         "action": "INSERT_AND_UPDATE",
         "data": [{**commission_mapping["COM_CODE_VALUE"], "code": result_data.get('category_code'),
                   "description": result_data.get('category_name')}]})

    data_detail.append(
        {'table': 'COM_TRANS_PROMPT_PROPERTIES', 'table_key': ['organization_id', 'trans_prompt_property_code'],
         "action": "INSERT_AND_UPDATE",
         "data": [{**commission_mapping["COM_TRANS_PROMPT_PROPERTIES"],
                   "trans_prompt_property_code": "SAM_COMMISSIONPATTERN"}]})

    # data_detail.append(
    #     {'table': 'COM_TRANSLATIONS', 'table_key': ['organization_id', 'locale', 'translation_key'],
    #      "action": "INSERT_AND_UPDATE",
    #      "data": [{**commission_mapping["COM_TRANSLATIONS"], "translation_key": result_data.get('category_code'),
    #                "translation": result_data.get('category_name')}]})

    return data_detail
