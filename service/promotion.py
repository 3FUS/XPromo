from datetime import datetime

import pandas as pd
from sqlalchemy import case, func, text
from sqlalchemy import and_, or_

from models.model import Promotion, PromotionCondition, PromotionResult, PromotionItemSegments, \
    PromotionLocationSegments, PromotionCustomerSegments, SegmentsItem, SegmentsLocation, SegmentsCustomer, \
    PromotionNextSequence, SegmentsCustomerDetail, SegmentsLocationDetail, WorkerTask, PromotionOrgJoin, \
    LOC_ORG_HIERARCHY, PromotionImport

from sqlalchemy.orm import Session
from service.utils import resolve_permissions_with_inheritance
import yaml

from utils.logger import app_logger

from utils.app_config import app_config
from utils.translator import get_message


def get_promotion_config(org_id=None, config_type='promotion_class'):
    """获取最新配置"""
    template = app_config.template_config_org[org_id][config_type] if org_id else app_config.template_config[
        config_type]
    return template


def generate_promotion_id(session: Session, sequence_type: str = 'promotion'):
    # 获取当前的 last_segment_id
    sequence = session.query(PromotionNextSequence).filter_by(sequence_type=sequence_type).first()

    if not sequence:
        # 如果 sequence 不存在，创建一个新的记录
        sequence = PromotionNextSequence(sequence_type=sequence_type, next_sequence=80000)
        session.add(sequence)
        session.commit()
        session.refresh(sequence)

    # 获取当前的 last_segment_id 并递增
    current_id = sequence.next_sequence
    sequence.next_sequence += 1
    session.commit()

    return current_id


# 新增: create_promotion 方法
async def create_promotion(session: Session, promotion: Promotion, user_id='', org_id=None):
    try:
        new_promotion = Promotion(
            org_id=org_id,
            promotion_id=generate_promotion_id(session),
            name=promotion.name,
            description=promotion.description,
            class_id=promotion.class_id,
            iteration_cap=promotion.iteration_cap,
            promotion_status=promotion.promotion_status,
            promotion_group=promotion.promotion_group,
            promotion_level=promotion.promotion_level,
            promotion_type=promotion.promotion_type,
            start_date=promotion.start_date,
            end_date=promotion.end_date,
            create_time=datetime.now(),
            create_user=user_id
        )
        if Promotion.subclass_id:
            new_promotion.subclass_id = promotion.subclass_id
        if promotion.coupon_code:
            new_promotion.coupon_code = promotion.coupon_code
        if hasattr(promotion, 'price_tag'):
            new_promotion.price_tag = promotion.price_tag
        if hasattr(promotion, 'stackable'):
            new_promotion.stackable = promotion.stackable
        session.add(new_promotion)
        session.commit()
        session.refresh(new_promotion)
        return new_promotion
    except Exception as e:
        app_logger.error(f"Error create_promotion: {e}")
        session.rollback()
        raise e


# 新增: update_promotion 方法
async def update_promotion(session: Session, promotion_data: Promotion, user_id=''):
    try:
        updated_promotion = session.query(Promotion).filter(
            Promotion.promotion_id == promotion_data.promotion_id).first()
        if updated_promotion:
            updated_promotion.name = promotion_data.name
            updated_promotion.description = promotion_data.description
            updated_promotion.class_id = promotion_data.class_id
            updated_promotion.promotion_status = promotion_data.promotion_status
            updated_promotion.promotion_group = promotion_data.promotion_group
            updated_promotion.promotion_level = promotion_data.promotion_level
            updated_promotion.promotion_type = promotion_data.promotion_type.value
            if promotion_data.coupon_code:  # 仅在 coupon_code 不为空时更新
                updated_promotion.coupon_code = promotion_data.coupon_code
            else:
                updated_promotion.coupon_code = None
            updated_promotion.iteration_cap = promotion_data.iteration_cap

            if hasattr(promotion_data, 'price_tag') and promotion_data.price_tag is not None:
                updated_promotion.price_tag = promotion_data.price_tag
            else:
                updated_promotion.price_tag = None

            if hasattr(promotion_data, 'stackable') and promotion_data.stackable is not None:
                updated_promotion.stackable = promotion_data.stackable
            else:
                updated_promotion.stackable = None
            updated_promotion.start_date = promotion_data.start_date
            updated_promotion.end_date = promotion_data.end_date
            updated_promotion.update_time = datetime.now()
            updated_promotion.update_user = user_id
            session.commit()
            session.refresh(updated_promotion)
        return updated_promotion
    except Exception as e:
        app_logger.error(f"Error updating promotion: {e}")
        session.rollback()
        raise e


async def update_promotion_status(session, promotion_id, promotion_status):
    updated_promotion = session.query(Promotion).filter(Promotion.promotion_id == promotion_id).first()
    if updated_promotion:
        updated_promotion.promotion_status = promotion_status
        session.commit()
        session.refresh(updated_promotion)
    return updated_promotion


async def update_promotion_export_time(session, promotion_id, last_export_time, last_session_id):
    updated_promotion = session.query(Promotion).filter(Promotion.promotion_id == promotion_id).first()
    if updated_promotion:
        updated_promotion.last_export_time = last_export_time
        updated_promotion.last_session_id = last_session_id
        session.commit()
        session.refresh(updated_promotion)
    return updated_promotion


async def delete_promotion(session, promotion_id=None):
    deleted_promotion = session.query(Promotion).filter(Promotion.promotion_id == promotion_id).first()
    if deleted_promotion:
        session.delete(deleted_promotion)
        session.commit()
    return deleted_promotion


# 新增: create_promotion_condition 方法
async def create_promotion_condition(session: Session, promotion_id: int, promotion: Promotion,
                                     promotion_conditions: PromotionCondition):
    try:
        # app_logger.info(f"promotion_conditions: {repr(promotion_conditions.model_dump())}")
        for promotion_condition in promotion_conditions:
            new_promotion_condition = PromotionCondition(
                promotion_id=promotion_id,
                set_id=promotion_condition.set_id,
                condition_type=promotion_condition.condition_type,
                threshold_style=promotion_condition.threshold_style,
                MinQty=promotion_condition.MinQty,
                MaxQty=promotion_condition.MinQty if promotion_condition.threshold_style == 'Every Quantity' else promotion_condition.MaxQty,
                MinItemTotal=promotion_condition.MinItemTotal,
                create_time=datetime.now(),
                create_user=promotion.create_user

            )
            session.add(new_promotion_condition)
        session.commit()
        session.refresh(new_promotion_condition)
        return new_promotion_condition
    except Exception as e:
        app_logger.error(f"Error create_promotion_condition: {e}")
        session.rollback()
        raise e


async def delete_promotion_condition(session, promotion_id):
    try:
        query = session.query(PromotionCondition)
        query.filter(PromotionCondition.promotion_id == promotion_id).delete()
        session.commit()
    except Exception as e:
        app_logger.error(f"Error deleting delete_promotion_condition: {e}")
        session.rollback()


async def delete_promotion_result(session, promotion_id):
    try:
        query = session.query(PromotionResult)
        query.filter(PromotionResult.promotion_id == promotion_id).delete()
        session.commit()
    except Exception as e:
        app_logger.error(f"Error deleting update_promotion_result: {e}")


# 新增: create_promotion_result 方法
async def create_promotion_result(session: Session, promotion_id: int, promotion: Promotion,
                                  promotion_results: PromotionResult):
    try:

        for promotion_result in promotion_results:
            new_promotion_result = PromotionResult(
                promotion_id=promotion_id,
                set_id=promotion_result.set_id,
                overlap=promotion_result.overlap,
                apply_type=promotion_result.apply_type,
                discount_type=promotion_result.discount_type if promotion_result.is_active == 1 else None,
                action_qty=None if promotion_result.action_qty == 0 else promotion_result.action_qty,
                discount_value=None if promotion_result.is_active == 0 else promotion_result.discount_value,
                is_active=promotion_result.is_active,
                create_time=datetime.now(),
                create_user=promotion.create_user
            )
            session.add(new_promotion_result)
        session.commit()
        session.refresh(new_promotion_result)
        return new_promotion_result
    except Exception as e:
        app_logger.error(f"Error create_promotion_result: {e}")
        session.rollback()
        raise e


async def update_promotion_condition(session: Session, promotion_id: int, promotion_conditions: [PromotionCondition],
                                     update_user: str):
    # updated_promotion_condition = session.query(PromotionCondition).filter(
    #     PromotionCondition.promotion_id == promotion.promotion_id).first()
    try:
        for condition in promotion_conditions:
            updated_promotion_condition = session.query(PromotionCondition).filter(
                PromotionCondition.promotion_id == promotion_id,
                PromotionCondition.set_id == condition.set_id
            ).first()

            if updated_promotion_condition:
                updated_promotion_condition.condition_type = condition.condition_type
                updated_promotion_condition.threshold_style = condition.threshold_style
                updated_promotion_condition.MinQty = condition.MinQty
                updated_promotion_condition.MaxQty = condition.MinQty if condition.threshold_style == 'Every Quantity' else condition.MaxQty
                updated_promotion_condition.MinItemTotal = condition.MinItemTotal
                updated_promotion_condition.update_time = datetime.now()
                updated_promotion_condition.update_user = update_user
        session.commit()
        session.refresh(updated_promotion_condition)
        return updated_promotion_condition
    except Exception as e:
        app_logger.error(f"Error update_promotion_condition: {e}")
        session.rollback()
        raise e


async def update_promotion_result(session: Session, promotion_id: int, promotion_results: PromotionResult,
                                  update_user: str):
    # updated_promotion_result = session.query(PromotionResult).filter(
    #     PromotionResult.promotion_id == promotion.promotion_id).first()

    for result in promotion_results:
        updated_promotion_result = session.query(PromotionResult).filter(
            PromotionResult.promotion_id == promotion_id,
            PromotionResult.set_id == result.set_id
        ).first()

        if updated_promotion_result:
            updated_promotion_result.overlap = result.overlap
            updated_promotion_result.apply_type = result.apply_type
            updated_promotion_result.discount_type = result.discount_type if result.is_active == 1 else None
            updated_promotion_result.action_qty = None if result.action_qty == 0 else result.action_qty
            updated_promotion_result.discount_value = result.discount_value if result.is_active == 1 else None
            updated_promotion_result.is_active = result.is_active
            updated_promotion_result.update_time = datetime.now()
            updated_promotion_result.update_user = update_user
    session.commit()
    session.refresh(updated_promotion_result)
    return updated_promotion_result


# 新增: create_promotion_item_segments 方法
async def create_promotion_item_segments(session: Session, promotion_id: int, create_user: str,
                                         promotion_item_segments):
    for promotion_item_segment in promotion_item_segments:
        new_promotion_item_segments = PromotionItemSegments(
            promotion_id=promotion_id,
            set_id=promotion_item_segment.set_id,
            segment_id=promotion_item_segment.segment_id,
            include=promotion_item_segment.include,
            item_type=promotion_item_segment.item_type.value,
            create_time=datetime.now(),
            create_user=create_user
        )
        session.add(new_promotion_item_segments)
    session.commit()
    session.refresh(new_promotion_item_segments)
    return new_promotion_item_segments


async def delete_promotion_item_segments(session, promotion_id=None):
    query = session.query(PromotionItemSegments)
    if promotion_id is not None:
        query = query.filter(PromotionItemSegments.promotion_id == promotion_id)
    deleted_promotion_item = query.all()
    if deleted_promotion_item:
        for promotion_item in deleted_promotion_item:
            session.delete(promotion_item)
        session.commit()
    return deleted_promotion_item


async def delete_promotion_import(session, promotion_id=None):
    query = session.query(PromotionImport)
    if promotion_id is not None:
        query = query.filter(PromotionImport.promotion_id == promotion_id)
        deleted_promotion_import = query.all()
        if deleted_promotion_import:
            for promotion_import in deleted_promotion_import:
                session.delete(promotion_import)
            session.commit()


async def delete_promotion_location_segments(session, promotion_id=None):
    query = session.query(PromotionLocationSegments)
    if promotion_id is not None:
        query = query.filter(PromotionLocationSegments.promotion_id == promotion_id)
    deleted_promotion_location = query.all()
    if deleted_promotion_location:
        for promotion_location in deleted_promotion_location:
            session.delete(promotion_location)
        session.commit()
    return deleted_promotion_location


async def delete_promotion_customer_segments(session, promotion_id=None):
    query = session.query(PromotionCustomerSegments)
    if promotion_id is not None:
        query = query.filter(PromotionCustomerSegments.promotion_id == promotion_id)
    deleted_promotion_customer = query.all()
    if deleted_promotion_customer:
        for promotion_customer in deleted_promotion_customer:
            session.delete(promotion_customer)
        session.commit()
    return deleted_promotion_customer


# 新增: create_promotion_location_segments 方法
async def create_promotion_location_segments(session: Session, promotion_id: int, promotion: Promotion,
                                             promotion_location_segments: PromotionLocationSegments):
    with session.begin_nested():  # 或者使用 session.begin() 如果没有嵌套事务需求
        session.query(PromotionOrgJoin).filter(PromotionOrgJoin.promotion_id == promotion_id).delete()

        for promotion_location_segment in promotion_location_segments:
            new_promotion_location_segments = PromotionLocationSegments(
                promotion_id=promotion_id,
                segment_id=promotion_location_segment.segment_id,
                include=promotion_location_segment.include,
                create_time=datetime.now(),
                create_user=promotion.create_user

            )
            session.add(new_promotion_location_segments)
        session.commit()
    session.refresh(new_promotion_location_segments)
    return new_promotion_location_segments


async def create_promotion_org_data(session: Session, promotion_id: int, org_data):
    try:
        with session.begin_nested():  # 或者使用 session.begin() 如果没有嵌套事务需求
            session.query(PromotionOrgJoin).filter(PromotionOrgJoin.promotion_id == promotion_id).delete()
            for item in org_data:
                parts = item.split(':')
                new_org_data = PromotionOrgJoin(
                    promotion_id=promotion_id,
                    org_code=parts[0],
                    org_value=parts[1],
                    create_time=datetime.now()
                )
                session.add(new_org_data)
            session.commit()
        return {"code": 200, "message": "Permissions updated successfully"}
    except Exception as e:
        session.rollback()
        raise ValueError(f"Failed to update org permissions: {str(e)}")


# 新增: create_promotion_customer_segments 方法
async def create_promotion_customer_segments(session: Session, promotion_id: int, promotion: Promotion,
                                             promotion_customer_segments: PromotionCustomerSegments):
    for promotion_customer_segment in promotion_customer_segments:
        new_promotion_customer_segments = PromotionCustomerSegments(
            promotion_id=promotion_id,
            segment_id=promotion_customer_segment.segment_id,
            include=promotion_customer_segment.include,
            create_time=datetime.now(),
            create_user=promotion.create_user
        )
        session.add(new_promotion_customer_segments)
    session.commit()
    session.refresh(new_promotion_customer_segments)
    return new_promotion_customer_segments


# def get_class_code_by_id(class_id, org_id=None):
#     class_id_to_code = get_promotion_config(org_id, 'promotion_template')
#     return class_id_to_code.get(class_id, None)
#
#
# def get_subclass_code_by_id(class_id, subclass_id, org_id=None):
#     for item in get_promotion_config(org_id, 'promotion_template'):
#         if item['class_id'] == class_id and item['subclass_id'] == str(subclass_id):
#             return item['code']
#     return None


async def get_promotion_list(session, key_word=None, promotion_status=None, org_id=None, page=1, page_size=30):
    # query = session.query(Promotion)

    query = session.query(
        Promotion,
        func.sum(case((WorkerTask.status == 'N', 1), else_=0)).label('count_N'),
        func.sum(case((WorkerTask.status == 'E', 1), else_=0)).label('count_E'),
        func.sum(case((WorkerTask.status == 'D', 1), else_=0)).label('count_D')
    ).outerjoin(
        WorkerTask,
        Promotion.last_session_id == WorkerTask.session_id
    ).group_by(Promotion)

    if key_word:
        key_word = f"%{key_word}%"  # 添加通配符以支持模糊查询
        query = query.filter(
            (Promotion.promotion_id.like(key_word)) |
            (Promotion.name.like(key_word)) |
            (Promotion.description.like(key_word)) |
            (Promotion.create_user.like(key_word))
        )
    if promotion_status != 'ALL':
        query = query.filter(Promotion.promotion_status == promotion_status)

    if org_id:
        query = query.filter(Promotion.org_id == org_id)

    query = query.order_by(Promotion.create_time.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()

    result = []
    now = datetime.now()

    promotion_template_list = get_promotion_config(org_id, 'promotion_template')

    promotion_template_dict = {
        (t['class_id'], t['subclass_id']): t
        for t in promotion_template_list
    }

    promotion_class_list = get_promotion_config(org_id, 'promotion_class')

    class_code_map = {
        item['class_id']: item['code']
        for item in promotion_class_list
    }

    try:
        for item, count_N, count_E, count_D in items:

            template = promotion_template_dict.get((item.class_id, str(item.subclass_id)), {})
            app_logger.debug("template: %s", template)
            online_calculation = template.get('online_calculation', 0)
            import_flag = template.get('import', 0)
            price_tag = template.get('price_tag', 0)
            subclass_code = template.get('code', '')

            class_code = class_code_map.get(item.class_id, '')

            total_tasks = (count_N or 0) + (count_E or 0) + (count_D or 0)

            # 判断状态灯
            if total_tasks == 0:
                status_light = 'gray'  # 没有任务
            elif (count_E or 0) == total_tasks:
                status_light = 'red'  # 全部Error
            elif (count_D or 0) == total_tasks:
                status_light = 'green'  # 全部Done
            elif (count_N or 0) == total_tasks:
                status_light = 'gray'  # 全部New
            elif (count_E or 0) > 0 and (count_D or 0) > 0:
                status_light = 'orange'  # 部分Error部分Done
            elif (count_D or 0) > 0:
                status_light = 'light_green'  # 部分Done
            else:
                status_light = 'gray'  # 默认

            time_stats = (
                'Closed' if item.promotion_status == 'inactive' else
                'In Progress' if item.start_date <= now <= item.end_date else
                'Completed' if item.end_date < now else 'Not Started'
            )

            result.append({
                'promotion_id': item.promotion_id,
                'name': item.name,
                'description': item.description,
                'promotion_type': item.promotion_type,
                'promotion_status': item.promotion_status,
                'class_id': item.class_id,
                'subclass_id': item.subclass_id,
                'promotion_group': item.promotion_group,
                'promotion_level': item.promotion_level,
                'coupon_code': item.coupon_code,
                'start_date': item.start_date.strftime('%Y-%m-%d %H:%M') if item.start_date else None,
                'end_date': item.end_date.strftime('%Y-%m-%d %H:%M') if item.end_date else None,
                'export_time': item.last_export_time.strftime('%Y-%m-%d %H:%M') if item.last_export_time else None,
                'export_status_counts': {
                    'New': count_N or 0,
                    'Error': count_E or 0,
                    'Done': count_D or 0
                },
                'export_light': status_light,
                'create_time': item.create_time.strftime('%Y-%m-%d %H:%M') if item.create_time else None,
                'create_user': item.create_user,
                'update_time': item.update_time.strftime('%Y-%m-%d %H:%M') if item.update_time else None,
                'update_user': item.update_user,
                'time_stats': time_stats,
                'class_code': class_code,
                'subclass_code': subclass_code,
                'online_calculation': online_calculation,
                'import_flag': import_flag,
                'price_tag': price_tag
            })
    except Exception as e:
        app_logger.error("Error processing item: %s", e)
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": result
    }


async def get_promotion_by_id(session, promotion_id):
    promotion = session.query(Promotion).filter(Promotion.promotion_id == promotion_id).first()
    return promotion


async def get_promotion_condition_by_id(session, promotion_id):
    promotion_condition = session.query(PromotionCondition).filter(
        PromotionCondition.promotion_id == promotion_id).all()
    return promotion_condition


async def get_promotion_result_by_id(session, promotion_id):
    promotion_result = session.query(PromotionResult).filter(PromotionResult.promotion_id == promotion_id).all()
    return promotion_result


async def get_promotion_item_segments_by_id(session, promotion_id):
    promotion_item_segments = (session.query(
        SegmentsItem.segment_id,
        PromotionItemSegments.set_id,
        SegmentsItem.name,
        SegmentsItem.description,
        SegmentsItem.sub_count,
        PromotionItemSegments.item_type,
        PromotionItemSegments.include)
                               .join(PromotionItemSegments,
                                     SegmentsItem.segment_id == PromotionItemSegments.segment_id)
                               .filter(PromotionItemSegments.promotion_id == promotion_id))
    result = [
        {
            "segment_id": segment.segment_id,
            "name": segment.name,
            "set_id": segment.set_id,
            "description": segment.description,
            "sub_count": segment.sub_count,
            "item_type": segment.item_type,
            "include": segment.include
        }
        for segment in promotion_item_segments.all()
    ]

    return result


async def get_promotion_org_join_by_id(session, promotion_id):
    promotion_org = session.query(PromotionOrgJoin).filter(
        PromotionOrgJoin.promotion_id == promotion_id)

    result = [
        f"{segment.org_code}:{segment.org_value}"
        for segment in promotion_org.all()
    ]
    return result


async def get_promotion_location_segments_by_id(session, promotion_id):
    promotion_location_segments = (session.query(
        SegmentsLocation.segment_id,
        SegmentsLocation.name,
        SegmentsLocation.description,
        SegmentsLocation.sub_count,
        PromotionLocationSegments.include)
                                   .join(PromotionLocationSegments,
                                         SegmentsLocation.segment_id == PromotionLocationSegments.segment_id)
                                   .filter(PromotionLocationSegments.promotion_id == promotion_id))

    result = [
        {
            "segment_id": segment.segment_id,
            "name": segment.name,
            "description": segment.description,
            "sub_count": segment.sub_count,
            "include": segment.include
        }
        for segment in promotion_location_segments.all()
    ]

    return result


async def get_promotion_location_detail_by_id(session, promotion_id):
    promotion_location_segments = (session.query(
        PromotionLocationSegments.segment_id,
        SegmentsLocationDetail.rtl_loc_id,
        PromotionLocationSegments.include)
                                   .join(PromotionLocationSegments,
                                         SegmentsLocationDetail.segment_id == PromotionLocationSegments.segment_id)
                                   .filter(PromotionLocationSegments.promotion_id == promotion_id))

    # result = [
    #     {
    #         "segment_id": segment.segment_id,
    #         "name": segment.rtl_loc_id,
    #         "include": segment.include
    #     }
    #     for segment in promotion_location_segments.all()
    # ]

    return promotion_location_segments


async def get_promotion_location_detail_by_id_v2(session, promotion_id):
    org_permissions = session.query(PromotionOrgJoin).filter(
        PromotionOrgJoin.promotion_id == promotion_id
    ).all()

    if not org_permissions:
        return []

    raw_permissions = {(perm.org_code, perm.org_value) for perm in org_permissions}

    all_nodes = session.query(LOC_ORG_HIERARCHY).all()

    resolved_permissions = resolve_permissions_with_inheritance(session, raw_permissions)

    store_list = [
        {"rtl_loc_id": int(node.ORG_VALUE)} for node in all_nodes
        if node.ORG_CODE == 'STORE' and (node.ORG_CODE, node.ORG_VALUE) in resolved_permissions
    ]

    return store_list


async def get_promotion_location_detail_by_id_v3(session, promotion_id):
    try:
        locations = session.query(WorkerTask.location_id).distinct(WorkerTask.location_id).filter(
            WorkerTask.data_type == 'promotion',
            WorkerTask.data_key == str(promotion_id)
        ).all()

        # 将查询结果转换为字典列表格式
        result = [
            {"rtl_loc_id": location.location_id}
            for location in locations
        ]
        app_logger.info(f"get promotion location detail promotion_id: {promotion_id}, 结果: {result}")
        return result
    except Exception as e:
        app_logger.error(f"获取促销位置详情时发生错误，promotion_id: {promotion_id}, 错误: {str(e)}", exc_info=True)
        return []


async def get_location_detail_by_promotionId(promotion_id: int, session) -> dict:
    """
    获取促销位置详情

    Args:
        promotion_id: 促销ID
        session: 数据库会话

    Returns:
        dict: 包含位置数据、数据类型和终止位置的字典
    """
    app_logger.info(f"[get_location_detail_by_promotion_id] 开始获取促销位置详情, promotion_id: {promotion_id}")

    try:
        res_location = await get_promotion_location_detail_by_id(session, promotion_id)

        df_locs = pd.DataFrame(res_location)

        if df_locs.empty:
            app_logger.info(f"[get_location_detail_by_promotion_id], promotion_id: {promotion_id}")
            res_location = await get_promotion_location_detail_by_id_v2(session, promotion_id)
            df_locs = pd.DataFrame(res_location)
            data_type = "hierarchy"

        else:
            excluded_locs = df_locs[df_locs['include'] == 0]['rtl_loc_id'].unique()
            data_type = "segment"
            df_locs = df_locs[~df_locs['rtl_loc_id'].isin(excluded_locs)]

        app_logger.info(f"[get_location_detail_by_promotion_id], df_locs: {df_locs}")

        bef_locs = await get_promotion_location_detail_by_id_v3(session, promotion_id)
        de_bef_locs = pd.DataFrame(bef_locs)

        app_logger.info(f"[get_location_detail_by_promotion_id], de_bef_locs: {de_bef_locs}")

        if df_locs.empty:
            termination_locs = de_bef_locs
            app_logger.info("df_locs is empty")
        elif not de_bef_locs.empty:
            termination_locs = de_bef_locs[~de_bef_locs['rtl_loc_id'].isin(df_locs['rtl_loc_id'].unique())]
            app_logger.info("get termination_locs")
        else:
            termination_locs = pd.DataFrame()
            app_logger.info("de_bef_locs and df_locs is empty")

        app_logger.info(f"[get_location_detail_by_promotion_id], termination_locs: {termination_locs}")

        return {"data": df_locs, "data_type": data_type, "termination_locs": termination_locs}

    except Exception as e:
        app_logger.error(
            f"[get_location_detail_by_promotion_id] 处理过程中发生错误, promotion_id: {promotion_id}, 错误: {str(e)}",
            exc_info=True)
        return {"data": pd.DataFrame(), "data_type": "unknown", "termination_locs": pd.DataFrame()}


async def get_promotionId_by_segmentId(segment_id: int, session):
    result = session.query(Promotion.promotion_id) \
        .join(PromotionItemSegments, Promotion.promotion_id == PromotionItemSegments.promotion_id) \
        .join(SegmentsItem, SegmentsItem.segment_id == PromotionItemSegments.segment_id) \
        .filter(
        and_(
            Promotion.promotion_status == 'active',
            or_(
                Promotion.start_date >= datetime.now(),
                Promotion.end_date >= datetime.now()
            ),
            SegmentsItem.segment_id == segment_id
        )
    ).distinct(Promotion.promotion_id).all()

    return [{"promotion_id": item.promotion_id} for item in result]


async def get_promotion_customer_segments_by_id(session, promotion_id):
    promotion_customer_segments = (session.query(
        SegmentsCustomer.segment_id,
        SegmentsCustomer.name,
        SegmentsCustomer.description,
        SegmentsCustomer.sub_count,
        PromotionCustomerSegments.include)
                                   .join(PromotionCustomerSegments,
                                         SegmentsCustomer.segment_id == PromotionCustomerSegments.segment_id)
                                   .filter(PromotionCustomerSegments.promotion_id == promotion_id))

    result = [
        {
            "segment_id": segment.segment_id,
            "name": segment.name,
            "description": segment.description,
            "sub_count": segment.sub_count,
            "include": segment.include
        }
        for segment in promotion_customer_segments.all()
    ]

    return result


async def get_promotion_import_by_id(session, promotion_id):
    promotion_import = session.query(PromotionImport).filter(
        PromotionImport.promotion_id == promotion_id).all()
    return promotion_import


async def get_promotionId_segments_by_phone(session, phone_number):
    promotionId_list = (session.query(
        Promotion.promotion_id
    ).distinct(Promotion.promotion_id)
                        .join(
        PromotionCustomerSegments,
        Promotion.promotion_id == PromotionCustomerSegments.promotion_id)
                        .join(
        SegmentsCustomerDetail,
        SegmentsCustomerDetail.segment_id == PromotionCustomerSegments.segment_id)
                        .filter(SegmentsCustomerDetail.cust_phone == phone_number))

    result = [
        {
            "promotion_id": Id_list.promotion_id
        }
        for Id_list in promotionId_list.all()
    ]

    return result


mapping_file = open('./config/mapping.yaml', 'r', encoding='utf-8')
mapping_config = yaml.safe_load(mapping_file)
promotion_mapping = mapping_config.get("promotion_mapping", {})


#
# PROMOTION_MNT_DEFAULT = get_dict_condition()['promotion_template_default_p']
#
# app_logger.info(
#     f"PROMOTION_MNT_DEFAULT: {PROMOTION_MNT_DEFAULT}"
# )


async def process_promotion_termination(promotion_id: int, session, location_id):
    data_detail = []
    try:
        # 获取基础促销数据
        app_logger.debug(f"获取促销基础数据，promotion_id: {promotion_id}")
        promotion_data = await get_promotion_by_id(session, promotion_id)
        if not promotion_data:
            app_logger.warning(f"未找到促销数据，promotion_id: {promotion_id}")
            return data_detail

        app_logger.debug(f"获取促销结果数据，promotion_id: {promotion_id}")
        promotion_result_data = await get_promotion_result_by_id(session, promotion_id)

        promotion_status = promotion_data.promotion_status
        begin_date = promotion_data.start_date
        end_date = promotion_data.end_date
        name = promotion_data.name
        promotion_group = promotion_data.promotion_group
        level_id = promotion_data.promotion_level

        iteration_cap = promotion_data.iteration_cap

        discount_type = None
        discount_value = None
        apply_type = None

        # 安全获取结果数据
        if promotion_result_data:
            discount_type = promotion_result_data[0].discount_type
            discount_value = promotion_result_data[0].discount_value
            apply_type = promotion_result_data[0].apply_type

        PRC_DEAL = []

        if promotion_status in ['active', 'inactive']:
            app_logger.debug(f"处理促销状态数据，status: {promotion_status}")

            DEAL = {
                **promotion_mapping["DEAL"],
                "deal_id": promotion_id,
                "description": name,
                "consumable": promotion_group,
                "act_deferred": 1,
                "effective_date": begin_date.strftime('%Y-%m-%d %H:%M:%S') if begin_date else None,
                "end_date": end_date.strftime('%Y-%m-%d %H:%M:%S') if end_date else None,
                "iteration_cap": iteration_cap,
                "trans_deal_flag": 0 if apply_type == 'Line' else 1,
                "group_id": f"{level_id}" if apply_type == 'Line' and level_id and level_id > 0 else None,
                "sort_order": level_id if level_id is not None and apply_type == 'Transaction' else 0
            }

            if apply_type == 'Transaction':
                DEAL['trwide_action'] = discount_type
                DEAL['trwide_amount'] = discount_value

            PRC_DEAL.append(DEAL)
            data_detail.append(
                {'table': 'PRC_DEAL', 'table_key': ['organization_id', 'deal_id'], "action": "INSERT_AND_UPDATE",
                 "data": PRC_DEAL})

            # data_detail.append(
            #     {'table': 'PRC_DEAL_LOC', 'table_key': ['organization_id', 'deal_id'], "action": "DELETE",
            #      "data": [{
            #          **promotion_mapping["PRC_DEAL_LOC"],
            #          "deal_id": promotion_id,
            #          "rtl_loc_id": location_id
            #      }]})

        return data_detail
    except Exception as e:
        app_logger.error(f"Error in process_promotion_termination: {str(e)}", exc_info=True)


def extract_unique_set_ids(promotion_condition_data):
    """
    从 promotion_condition_data 中提取所有唯一的 set_id

    Args:
        promotion_condition_data: 包含 set_id 属性的对象列表

    Returns:
        list: 包含唯一 set_id 的字典列表，格式为 [{"set_id": value}, ...]
    """
    unique_set_ids = set()
    for condition in promotion_condition_data:
        unique_set_ids.add(condition.set_id)

    return [{"set_id": set_id} for set_id in sorted(unique_set_ids)]


async def process_promotion_data(promotion_id: int, session, location_id):
    """
    处理促销数据，生成下游系统所需的格式化数据

    Args:
        promotion_id (int): 促销ID
        session: 数据库会话
        location_id: 位置ID

    Returns:
        list: 格式化的促销数据列表
    """
    app_logger.info(f"开始处理促销数据，promotion_id: {promotion_id}, location_id: {location_id}")

    try:
        # 获取基础促销数据
        promotion_data = await get_promotion_by_id(session, promotion_id)
        if not promotion_data:
            app_logger.warning(f"未找到促销数据，promotion_id: {promotion_id}")
            return []

        promotion_result_data = await get_promotion_result_by_id(session, promotion_id)
        promotion_condition_data = await get_promotion_condition_by_id(session, promotion_id)
        promotion_item_segments_data_all = await get_promotion_item_segments_by_id(session, promotion_id)
        promotion_cust_segments_data = await get_promotion_customer_segments_by_id(session, promotion_id)

        # 过滤掉 ALL ITEM
        promotion_item_segments_data = [
            item for item in promotion_item_segments_data_all
            if item['name'] != 'ALL ITEM'
        ]

        # 提取配置信息
        config = _extract_promotion_config(promotion_data, promotion_result_data, promotion_condition_data)
        item_set = _get_item_set_type(promotion_data.org_id, config['class_id'], config['subclass_id'])

        # 初始化数据容器
        data_containers = {
            'PRC_DEAL': [],
            'PRC_DEAL_P': [],
            'PRC_DEAL_ITEM': [],
            'PRC_DEAL_FIELD_TEST': [],
            'PRC_DEAL_LOC': [],
            'PRC_DEAL_TRIG': [],
            'DSC_COUPON_XREF': []
        }

        # 只处理激活或非激活状态的促销
        if config['promotion_status'] not in ['active', 'inactive']:
            app_logger.info(f"促销状态不需要处理，promotion_id: {promotion_id}, status: {config['promotion_status']}")
            return []

        # 构建各类数据
        _build_deal_data(data_containers, promotion_id, config, item_set)
        if config['stackable'] >= 0:
            _build_deal_p_data(data_containers, promotion_id, config['stackable'], config['subclass_id'],
                               config['set_ids'])
        _build_deal_item_data(data_containers, promotion_id, config, item_set, promotion_condition_data,
                              promotion_result_data)
        _build_deal_field_test_data(data_containers, promotion_id, config['subclass_id'], item_set,
                                    promotion_item_segments_data)
        _build_deal_loc_data(data_containers, promotion_id, config['subclass_id'], config['set_ids'], location_id)
        _build_deal_trig_data(data_containers, promotion_id, config['subclass_id'], config['set_ids'],
                              promotion_cust_segments_data, config['promotion_type'], config['coupon_code'],
                              config['promotion_status'])

        # 组装最终结果
        data_detail = _assemble_data_detail(data_containers, promotion_id, config['subclass_id'], config['set_ids'])

        app_logger.info(f"完成促销数据处理，promotion_id: {promotion_id}, 生成数据项数: {len(data_detail)}")
        return data_detail

    except Exception as e:
        app_logger.error(f"处理促销数据时发生错误，promotion_id: {promotion_id}, 错误: {str(e)}", exc_info=True)
        raise e


def _extract_promotion_config(promotion_data, promotion_result_data, promotion_condition_data):
    """提取促销配置信息"""
    config = {
        'promotion_status': promotion_data.promotion_status,
        'begin_date': promotion_data.start_date,
        'end_date': promotion_data.end_date,
        'name': promotion_data.name,
        'promotion_group': promotion_data.promotion_group,
        'level_id': promotion_data.promotion_level,
        'promotion_type': promotion_data.promotion_type,
        'coupon_code': promotion_data.coupon_code,
        'class_id': promotion_data.class_id,
        'subclass_id': promotion_data.subclass_id,
        'iteration_cap': promotion_data.iteration_cap,
        'stackable': promotion_data.stackable,
        'discount_type': None,
        'discount_value': None,
        'apply_type': None,
        'overlap': None,
        'action_qty': None,
        'condition_type': None,
        'qty_min': None,
        'qty_max': None,
        'MinItemTotal': None,
        'set_ids': []
    }

    # 安全获取结果数据
    if promotion_result_data:
        first_result = promotion_result_data[0]
        config.update({
            'discount_type': first_result.discount_type,
            'discount_value': first_result.discount_value,
            'apply_type': first_result.apply_type,
            'overlap': first_result.overlap,
            'action_qty': first_result.action_qty
        })

    # 安全获取条件数据
    if promotion_condition_data:
        first_condition = promotion_condition_data[0]
        config.update({
            'condition_type': first_condition.condition_type,
            'qty_min': first_condition.MinQty,
            'qty_max': first_condition.MaxQty,
            'MinItemTotal': first_condition.MinItemTotal
        })
        config['set_ids'] = extract_unique_set_ids(promotion_condition_data)

    return config


def _get_item_set_type(org_id, class_id, subclass_id):
    """获取 item_set 类型"""
    promotion_config = get_promotion_config(org_id, 'promotion_template_default_p')
    return promotion_config[class_id][subclass_id].get('item_set', 2)


def _build_deal_data(data_containers, promotion_id, config, item_set):
    """构建 DEAL 数据"""
    deal_template = {
        **promotion_mapping["DEAL"],
        "deal_id": promotion_id,
        "description": config['name'],
        "consumable": config['promotion_group'],
        "act_deferred": 0 if config['promotion_status'] == 'active' else 1,
        "effective_date": config['begin_date'].strftime('%Y-%m-%d %H:%M:%S') if config['begin_date'] else None,
        "end_date": config['end_date'].strftime('%Y-%m-%d %H:%M:%S') if config['end_date'] else None,
        "iteration_cap": config['iteration_cap'],
        "trans_deal_flag": 0 if config['apply_type'] == 'Line' else 1,
        "group_id": f"{config['level_id']}" if config['apply_type'] == 'Line' and config['level_id'] and config[
            'level_id'] > 0 else None,
        "sort_order": config['level_id'] if config['level_id'] is not None and config[
            'apply_type'] == 'Transaction' else 0
    }

    if config['apply_type'] == 'Transaction':
        deal_template['trwide_action'] = config['discount_type']
        deal_template['trwide_amount'] = config['discount_value']

    # 根据 subclass_id 和 item_set 决定是否需要拆分
    if config['subclass_id'] == '99' and item_set == 2:
        for set_id_info in config['set_ids']:
            deal_copy = deal_template.copy()
            deal_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
            data_containers['PRC_DEAL'].append(deal_copy)
    else:
        data_containers['PRC_DEAL'].append(deal_template)


def _build_deal_p_data(data_containers, promotion_id, stackable, subclass_id, set_ids):
    """构建 DEAL_P 数据"""
    deal_p_template = {
        **promotion_mapping["DEAL_P"],
        "deal_id": promotion_id,
        "string_value": 'Enable' if stackable else 'Disable'
    }

    if subclass_id == '99':
        for set_id_info in set_ids:
            deal_p_copy = deal_p_template.copy()
            deal_p_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
            data_containers['PRC_DEAL_P'].append(deal_p_copy)
    else:
        data_containers['PRC_DEAL_P'].append(deal_p_template)


def _build_deal_item_data(data_containers, promotion_id, config, item_set, promotion_condition_data,
                          promotion_result_data):
    """构建 DEAL_ITEM 数据"""
    if item_set == 1:
        _build_deal_item_type_1(data_containers, promotion_id, config)
    elif item_set == 2:
        _build_deal_item_type_2(data_containers, promotion_id, config, promotion_condition_data, promotion_result_data)
    elif item_set == 0:
        _build_deal_item_type_0(data_containers, promotion_id, config, promotion_condition_data, promotion_result_data)


def _build_deal_item_type_1(data_containers, promotion_id, config):
    """构建 item_set 类型 1 的 DEAL_ITEM"""
    deal_item = {
        **promotion_mapping["DEAL_ITEM_1"],
        "deal_id": promotion_id,
        "item_ordinal": 1,
        "consumable": 1 if config['overlap'] == 0 else 0,
        "qty_min": config['qty_min'] if config['qty_min'] else 1,
        "qty_max": config['qty_max'] if config['qty_min'] else 9999,
        "min_item_total": config['MinItemTotal'] if config['condition_type'] == 'Amount' else None,
        "deal_action": config['discount_type'] if config['apply_type'] == 'Line' else None,
        "action_arg": config['discount_value'] if config['apply_type'] == 'Line' else None,
        "action_arg_qty": config['action_qty'] if config['action_qty'] and config['action_qty'] > 0 else None
    }
    data_containers['PRC_DEAL_ITEM'].append(deal_item)


def _build_deal_item_type_2(data_containers, promotion_id, config, promotion_condition_data, promotion_result_data):
    """构建 item_set 类型 2 的 DEAL_ITEM"""
    # 添加条件数据
    for condition in promotion_condition_data:
        deal_item = {
            **promotion_mapping["DEAL_ITEM_1"],
            "deal_id": promotion_id,
            "item_ordinal": condition.set_id,
            "consumable": 1 if config['overlap'] == 0 else 0,
            "qty_min": condition.MinQty if condition.MinQty else 1,
            "qty_max": condition.MaxQty if condition.MinQty else 9999,
            "min_item_total": condition.MinItemTotal if condition.condition_type == 'Amount' else None
        }
        data_containers['PRC_DEAL_ITEM'].append(deal_item)

    # 添加结果数据
    for result in promotion_result_data:
        deal_item = {
            **promotion_mapping["DEAL_ITEM_2"],
            "deal_id": promotion_id,
            "item_ordinal": result.set_id,
            "consumable": 1 if result.overlap == 0 else 0,
            "qty_min": result.action_qty if result.action_qty and result.action_qty > 0 else 1,
            "qty_max": result.action_qty if result.action_qty and result.action_qty > 0 else 99999,
            "deal_action": result.discount_type if result.apply_type == 'Line' else None,
            "action_arg": result.discount_value if result.apply_type == 'Line' else None,
            "action_arg_qty": result.action_qty if result.action_qty and result.action_qty > 0 else None
        }
        data_containers['PRC_DEAL_ITEM'].append(deal_item)


def _build_deal_item_type_0(data_containers, promotion_id, config, promotion_condition_data, promotion_result_data):
    """构建 item_set 类型 0 的 DEAL_ITEM"""
    result_by_set_id = {result.set_id: result for result in promotion_result_data}

    for condition in promotion_condition_data:
        set_id = condition.set_id
        result_data = result_by_set_id.get(set_id)

        if result_data:
            deal_item = {
                **promotion_mapping["DEAL_ITEM_1"],
                "deal_id": promotion_id if config['subclass_id'] != '99' else f"{promotion_id}:{set_id}",
                "item_ordinal": set_id,
                "consumable": 1 if config['overlap'] == 0 else 0,
                "qty_min": condition.MinQty if condition.MinQty is not None else 1,
                "qty_max": condition.MaxQty if condition.MaxQty is not None else 9999,
                "min_item_total": condition.MinItemTotal if condition.condition_type == 'Amount' else None,
                "deal_action": result_data.discount_type if result_data.apply_type == 'Line' else None,
                "action_arg": result_data.discount_value if result_data.apply_type == 'Line' else None,
                "action_arg_qty": result_data.action_qty if result_data.action_qty is not None and result_data.action_qty > 0 else None
            }
            data_containers['PRC_DEAL_ITEM'].append(deal_item)


def _build_deal_field_test_data(data_containers, promotion_id, subclass_id, item_set, promotion_item_segments_data):
    """构建 DEAL_FIELD_TEST 数据"""
    from collections import defaultdict

    grouped_items = defaultdict(list)
    for item in promotion_item_segments_data:
        grouped_items[item['set_id']].append(item)

    for set_id in sorted(grouped_items.keys()):
        equal_items = [item for item in grouped_items[set_id] if item['include'] == 1]
        not_equal_items = [item for item in grouped_items[set_id] if item['include'] == 0]

        serial_number = 1

        if equal_items:
            _process_equal_items(data_containers, promotion_id, subclass_id, item_set,
                                 equal_items, not_equal_items, serial_number)
        elif not_equal_items:
            _process_not_equal_items_only(data_containers, promotion_id, subclass_id, item_set,
                                          not_equal_items, serial_number)


def _process_equal_items(data_containers, promotion_id, subclass_id, item_set,
                         equal_items, not_equal_items, start_serial):
    """处理包含 equal items 的情况"""
    serial_number = start_serial

    for item in equal_items:
        item_condition_seq = 1
        item_type = 1 if item['item_type'] == 'Condition' else 2

        if item_set in [1, 0] and item_type != 1:
            continue

        deal_field_test = {
            **promotion_mapping["DEAL_ITEM_TEST"],
            "deal_id": promotion_id if subclass_id != '99' else f"{promotion_id}:{item['set_id']}",
            "item_ordinal": item['set_id'],
            "item_condition_group": serial_number,
            "item_condition_seq": item_condition_seq,
            "item_field": f"ITEM_PROPERTY:ITM_PROP_{item['segment_id']}",
            "match_rule": 'EQUAL'
        }
        data_containers['PRC_DEAL_FIELD_TEST'].append(deal_field_test)

        # 添加 not equal items
        for not_item in not_equal_items:
            item_condition_seq += 1
            not_item_type = 1 if not_item['item_type'] == 'Condition' else 2

            if item_set in [1, 0] and not_item_type != 1:
                continue

            deal_field_test = {
                **promotion_mapping["DEAL_ITEM_TEST"],
                "deal_id": promotion_id if subclass_id != '99' else f"{promotion_id}:{not_item['set_id']}",
                "item_ordinal": not_item['set_id'],
                "item_condition_group": serial_number,
                "item_condition_seq": item_condition_seq,
                "item_field": f"ITEM_PROPERTY:ITM_PROP_{not_item['segment_id']}",
                "match_rule": 'NOT_EQUAL'
            }
            data_containers['PRC_DEAL_FIELD_TEST'].append(deal_field_test)

        serial_number += 1


def _process_not_equal_items_only(data_containers, promotion_id, subclass_id, item_set,
                                  not_equal_items, start_serial):
    """处理只有 not equal items 的情况"""
    serial_number = start_serial

    for not_item in not_equal_items:
        item_condition_seq = 1
        item_type = 1 if not_item['item_type'] == 'Condition' else 2

        if item_set in [1, 0] and item_type != 1:
            continue

        deal_field_test = {
            **promotion_mapping["DEAL_ITEM_TEST"],
            "deal_id": promotion_id if subclass_id != '99' else f"{promotion_id}:{not_item['set_id']}",
            "item_ordinal": not_item['set_id'],
            "item_condition_group": serial_number,
            "item_condition_seq": item_condition_seq,
            "item_field": f"ITEM_PROPERTY:ITM_PROP_{not_item['segment_id']}",
            "match_rule": 'NOT_EQUAL'
        }
        data_containers['PRC_DEAL_FIELD_TEST'].append(deal_field_test)
        serial_number += 1


def _build_deal_loc_data(data_containers, promotion_id, subclass_id, set_ids, location_id):
    """构建 DEAL_LOC 数据"""
    deal_loc_template = {
        **promotion_mapping["PRC_DEAL_LOC"],
        "deal_id": promotion_id,
        "rtl_loc_id": location_id
    }

    if subclass_id == '99':
        for set_id_info in set_ids:
            deal_loc_copy = deal_loc_template.copy()
            deal_loc_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
            data_containers['PRC_DEAL_LOC'].append(deal_loc_copy)
    else:
        data_containers['PRC_DEAL_LOC'].append(deal_loc_template)


def _build_deal_trig_data(data_containers, promotion_id, subclass_id, set_ids,
                          promotion_cust_segments_data, promotion_type, coupon_code, promotion_status):
    """构建 DEAL_TRIG 和 COUPON_XREF 数据"""
    # 处理客户分群触发器
    for cust in promotion_cust_segments_data:
        deal_trig = {
            **promotion_mapping["PRC_DEAL_TRIG"],
            "deal_id": promotion_id,
            "deal_trigger": f"SEGMENT:{'' if cust['include'] else '~'}{cust['segment_id']}"
        }

        if subclass_id == '99':
            for set_id_info in set_ids:
                deal_trig_copy = deal_trig.copy()
                deal_trig_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
                data_containers['PRC_DEAL_TRIG'].append(deal_trig_copy)
        else:
            data_containers['PRC_DEAL_TRIG'].append(deal_trig)

    # 处理优惠券触发器
    if promotion_type and coupon_code:
        deal_trig = {
            **promotion_mapping["PRC_DEAL_TRIG"],
            "deal_id": promotion_id,
            "deal_trigger": f"COUPON:INPUT_COUPON:{coupon_code}"
        }

        if subclass_id == '99':
            for set_id_info in set_ids:
                deal_trig_copy = deal_trig.copy()
                deal_trig_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
                data_containers['PRC_DEAL_TRIG'].append(deal_trig_copy)
        else:
            data_containers['PRC_DEAL_TRIG'].append(deal_trig)

        # 添加优惠券交叉引用数据
        data_containers['DSC_COUPON_XREF'].append({
            **promotion_mapping["DSC_COUPON_XREF"],
            "coupon_serial_nbr": coupon_code,
            "expiration_date": '2029-01-01' if promotion_status == 'active' else '2019-01-01'
        })


def _assemble_data_detail(data_containers, promotion_id, subclass_id, set_ids):
    """组装最终的数据详情"""
    data_detail = []

    # PRC_DEAL
    if data_containers['PRC_DEAL']:
        data_detail.append({
            'table': 'PRC_DEAL',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL']
        })

    # PRC_DEAL_P
    if data_containers['PRC_DEAL_P']:
        data_detail.append({
            'table': 'PRC_DEAL_P',
            'table_key': ['organization_id', 'deal_id', 'property_code'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL_P']
        })

    # PRC_DEAL_ITEM
    if data_containers['PRC_DEAL_ITEM']:
        data_detail.append({
            'table': 'PRC_DEAL_ITEM',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL_ITEM']
        })

    # PRC_DEAL_FIELD_TEST
    if data_containers['PRC_DEAL_FIELD_TEST']:
        data_detail.append({
            'table': 'PRC_DEAL_FIELD_TEST',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL_FIELD_TEST']
        })
    else:
        mock_deal_item = {
            **promotion_mapping["DEAL_ITEM_TEST"],
            "deal_id": promotion_id,
        }
        data_detail.append({
            'table': 'PRC_DEAL_FIELD_TEST',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE",
            "data": [mock_deal_item]
        })

    # PRC_DEAL_LOC
    if data_containers['PRC_DEAL_LOC']:
        data_detail.append({
            'table': 'PRC_DEAL_LOC',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL_LOC']
        })

    # PRC_DEAL_TRIG
    if data_containers['PRC_DEAL_TRIG']:
        data_detail.append({
            'table': 'PRC_DEAL_TRIG',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE_AND_INSERT",
            "data": data_containers['PRC_DEAL_TRIG']
        })
    else:
        deal_trig_template = {
            "organization_id": promotion_mapping["PRC_DEAL_TRIG"]["organization_id"],
            "deal_id": promotion_id
        }

        prc_deal_trig_delete = []
        if subclass_id == '99':
            for set_id_info in set_ids:
                deal_trig_copy = deal_trig_template.copy()
                deal_trig_copy["deal_id"] = f"{promotion_id}:{set_id_info['set_id']}"
                prc_deal_trig_delete.append(deal_trig_copy)
        else:
            prc_deal_trig_delete.append(deal_trig_template)

        data_detail.append({
            'table': 'PRC_DEAL_TRIG',
            'table_key': ['organization_id', 'deal_id'],
            "action": "DELETE",
            "data": prc_deal_trig_delete
        })
        app_logger.info(f"促销数据无触发器，promotion_id: {promotion_id}")

    # DSC_COUPON_XREF
    if data_containers['DSC_COUPON_XREF']:
        data_detail.append({
            'table': 'DSC_COUPON_XREF',
            'table_key': ['organization_id', 'coupon_serial_nbr'],
            "action": "INSERT_AND_UPDATE",
            "data": data_containers['DSC_COUPON_XREF']
        })

    return data_detail


async def get_promotion_export_status(session: Session, promotion_id: int, key_word: str = None, page: int = 1,
                                      page_size: int = 10, lang='en'):
    # 构建基础查询
    query = (
        session.query(
            WorkerTask.location_id,
            LOC_ORG_HIERARCHY.DESCRIPTION,
            WorkerTask.terminal_id,
            WorkerTask.status,
            WorkerTask.msg,
            WorkerTask.update_time,
            WorkerTask.session_id
        )
        .join(Promotion, Promotion.last_session_id == WorkerTask.session_id)
        .outerjoin(LOC_ORG_HIERARCHY,
                   and_(WorkerTask.location_id == LOC_ORG_HIERARCHY.ORG_VALUE,
                        LOC_ORG_HIERARCHY.ORG_CODE == 'STORE'))
        .filter(Promotion.promotion_id == promotion_id)
    )

    # 添加模糊查询条件
    if key_word:
        query = query.filter(
            or_(
                WorkerTask.location_id.like(f"%{key_word}%"),
                LOC_ORG_HIERARCHY.DESCRIPTION.like(f"%{key_word}%")
            )
        )

    # 获取总记录数
    total = query.count()

    # 添加分页和排序（MSSQL分页必须使用ORDER BY）
    results = (
        query.order_by(WorkerTask.location_id.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    # 格式化结果
    formatted_results = [
        {
            "location_id": item.location_id,
            "description": item.DESCRIPTION,
            "terminal_id": item.terminal_id,
            "status_color": item.status,
            "status": get_message("status_error", lang) if item.status == "E" else get_message("status_done",
                                                                                               lang) if item.status == "D" else get_message(
                "status_pending", lang) if item.status == "N" else item.status,
            "msg": item.msg,
            "download_time": item.update_time.strftime('%Y-%m-%d %H:%M') if item.update_time else None,
            "session_id": item.session_id
        }
        for item in results
    ]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": formatted_results
    }


async def get_promotion_dashboard(session: Session, org_id: str):
    sql = text("""
            SELECT COUNT(DISTINCT a.promotion_id) as 'Total',
								COUNT(DISTINCT case when end_date<GETDATE() then a.promotion_id ELSE NULL END) AS 'Completed',
								COUNT(DISTINCT case when start_date>GETDATE() then a.promotion_id ELSE NULL END) AS 'Not_Started',
								COUNT(DISTINCT case when GETDATE() BETWEEN start_date AND end_date then a.promotion_id ELSE NULL END) AS 'In_Progress',
								COUNT(DISTINCT CASE promotion_type WHEN 'Product' THEN a.promotion_id ELSE NULL END) AS 'Product',
								COUNT(DISTINCT CASE promotion_type WHEN 'Coupon' THEN a.promotion_id ELSE NULL END) AS 'Coupon',
								COUNT(DISTINCT CASE apply_type WHEN 'Line' THEN a.promotion_id ELSE NULL END) AS 'Line',
								COUNT(DISTINCT CASE apply_type WHEN 'Transaction' THEN a.promotion_id ELSE NULL END) AS 'Transaction',
								COUNT(DISTINCT CASE discount_type WHEN 'PERCENT_OFF' THEN a.promotion_id ELSE NULL END) AS 'PERCENT_OFF',
								COUNT(DISTINCT CASE discount_type WHEN 'CURRENCY_OFF' THEN a.promotion_id ELSE NULL END) AS 'CURRENCY_OFF',
								COUNT(DISTINCT CASE discount_type WHEN 'NEW_PRICE' THEN a.promotion_id ELSE NULL END) AS 'NEW_PRICE'
						FROM 
								promotions a 
						INNER JOIN 
								promotions_result b 
						ON 
								a.promotion_id = b.promotion_id WHERE a.promotion_status='active' AND a.org_id=:org_id
        """)

    try:
        result = session.execute(sql, {"org_id": org_id})
        data = result.fetchone()
        if data is None:
            return {"Total": 0, "Completed": 0, "Not_Started": 0, "In_Progress": 0, "Product": 0, "Coupon": 0,
                    "Line": 0, "Transaction": 0, "Percent_off": 0, "Amount_off": 0, "Fix_Price": 0}
        return {"Total": data[0], "Completed": data[1], "Not_Started": data[2], "In_Progress": data[3],
                "Product": data[4], "Coupon": data[5], "Line": data[6], "Transaction": data[7], "Percent_off": data[8],
                "Amount_off": data[9], "Fix_Price": data[10]}
    except Exception as e:
        # 处理异常，例如记录日志或抛出自定义异常
        raise Exception(f"An error occurred while executing the SQL query: {e}")
