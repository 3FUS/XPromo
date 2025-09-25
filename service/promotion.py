from datetime import datetime

from sqlalchemy import case, func, text

from models.model import Promotion, PromotionCondition, PromotionResult, PromotionItemSegments, \
    PromotionLocationSegments, PromotionCustomerSegments, SegmentsItem, SegmentsLocation, SegmentsCustomer, \
    PromotionNextSequence, SegmentsCustomerDetail, SegmentsLocationDetail, WorkerTask, PromotionOrgJoin, \
    LOC_ORG_HIERARCHY

from sqlalchemy.orm import Session
from service.utils import resolve_permissions_with_inheritance
import yaml

file = open('config/config.yaml', 'r', encoding='utf-8')
dict_condition = yaml.safe_load(file)


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
async def create_promotion(session: Session, promotion: Promotion, user_id=''):
    new_promotion = Promotion(
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
    session.add(new_promotion)
    session.commit()
    session.refresh(new_promotion)
    return new_promotion


# 新增: update_promotion 方法
async def update_promotion(session: Session, promotion_data: Promotion, user_id=''):
    updated_promotion = session.query(Promotion).filter(Promotion.promotion_id == promotion_data.promotion_id).first()
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
        updated_promotion.start_date = promotion_data.start_date
        updated_promotion.end_date = promotion_data.end_date
        updated_promotion.update_time = datetime.now()
        updated_promotion.update_user = user_id
        session.commit()
        session.refresh(updated_promotion)
    return updated_promotion


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


# 新增: create_promotion_result 方法
async def create_promotion_result(session: Session, promotion_id: int, promotion: Promotion,
                                  promotion_results: PromotionResult):
    for promotion_result in promotion_results:
        new_promotion_result = PromotionResult(
            promotion_id=promotion_id,
            set_id=promotion_result.set_id,
            overlap=promotion_result.overlap,
            apply_type=promotion_result.apply_type,
            discount_type=promotion_result.discount_type,
            action_qty=None if promotion_result.action_qty == 0 else promotion_result.action_qty,
            discount_value=promotion_result.discount_value,
            create_time=datetime.now(),
            create_user=promotion.create_user
        )
        session.add(new_promotion_result)
    session.commit()
    session.refresh(new_promotion_result)
    return new_promotion_result


async def update_promotion_condition(session: Session, promotion_id: int, promotion_conditions: [PromotionCondition],
                                     update_user: str):
    # updated_promotion_condition = session.query(PromotionCondition).filter(
    #     PromotionCondition.promotion_id == promotion.promotion_id).first()
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
            updated_promotion_result.discount_type = result.discount_type
            updated_promotion_result.action_qty = None if result.action_qty == 0 else result.action_qty
            updated_promotion_result.discount_value = result.discount_value
            updated_promotion_result.update_time = datetime.now()
            updated_promotion_result.update_user = update_user
    session.commit()
    session.refresh(updated_promotion_result)
    return updated_promotion_result


# 新增: create_promotion_item_segments 方法
async def create_promotion_item_segments(session: Session, promotion_id: int, promotion: Promotion,
                                         promotion_item_segments):
    for promotion_item_segment in promotion_item_segments:
        new_promotion_item_segments = PromotionItemSegments(
            promotion_id=promotion_id,
            set_id=promotion_item_segment.set_id,
            segment_id=promotion_item_segment.segment_id,
            include=promotion_item_segment.include,
            item_type=promotion_item_segment.item_type.value,
            create_time=datetime.now(),
            create_user=promotion.create_user
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


class_id_to_code = {item['class_id']: item['code'] for item in dict_condition['promotion_class']}


def get_class_code_by_id(class_id):
    return class_id_to_code.get(class_id, None)


async def get_promotion_list(session, key_word=None, promotion_status=None, page=1, page_size=30):
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
            (Promotion.name.like(key_word)) |
            (Promotion.description.like(key_word)) |
            (Promotion.create_user.like(key_word))
        )
    if promotion_status != 'ALL':
        query = query.filter(Promotion.promotion_status == promotion_status)

    query = query.order_by(Promotion.create_time.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()

    result = []
    now = datetime.now()
    for item, count_N, count_E, count_D in items:

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
            'class_code': get_class_code_by_id(item.class_id)
        })

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
        {"rtl_loc_id": node.ORG_VALUE} for node in all_nodes
        if node.ORG_CODE == 'STORE' and (node.ORG_CODE, node.ORG_VALUE) in resolved_permissions
    ]

    return store_list


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

config_file = open('./config/config.yaml', 'r', encoding='utf-8')
config_config = yaml.safe_load(config_file)
PROMOTION_MNT_DEFAULT = config_config['promotion_template_default_p']


async def process_promotion_data(promotion_id: int, session, location_id):
    data_detail = []

    promotion_data = await get_promotion_by_id(session, promotion_id)
    promotion_result_data = await get_promotion_result_by_id(session, promotion_id)
    promotion_condition_data = await get_promotion_condition_by_id(session, promotion_id)
    promotion_item_segments_data = await get_promotion_item_segments_by_id(session, promotion_id)
    promotion_cust_segments_data = await get_promotion_customer_segments_by_id(session, promotion_id)

    promotion_status = promotion_data.promotion_status
    begin_date = promotion_data.start_date
    end_date = promotion_data.end_date
    name = promotion_data.name
    promotion_group = promotion_data.promotion_group
    level_id = promotion_data.promotion_level
    promotion_type = promotion_data.promotion_type
    coupon_code = promotion_data.coupon_code
    class_id = promotion_data.class_id
    subclass_id = promotion_data.subclass_id
    iteration_cap = promotion_data.iteration_cap
    item_set = PROMOTION_MNT_DEFAULT[class_id][subclass_id].get('item_set', 2)

    discount_type = promotion_result_data[0].discount_type
    discount_value = promotion_result_data[0].discount_value
    apply_type = promotion_result_data[0].apply_type
    overlap = promotion_result_data[0].overlap
    action_qty = promotion_result_data[0].action_qty

    condition_type = promotion_condition_data[0].condition_type
    qty_min = promotion_condition_data[0].MinQty
    qty_max = promotion_condition_data[0].MaxQty
    MinItemTotal = promotion_condition_data[0].MinItemTotal

    PRC_DEAL, PRC_DEAL_ITEM, PRC_DEAL_FIELD_TEST, PRC_DEAL_TRIG, DSC_COUPON_XREF = [], [], [], [], []
    if promotion_status in ['active', 'inactive']:

        DEAL = {
            **promotion_mapping["DEAL"],
            "deal_id": promotion_id,
            "description": name,
            "consumable": promotion_group,
            "act_deferred": 0 if promotion_status == 'active' else 1,
            "effective_date": begin_date.strftime('%Y-%m-%d %H:%M:%S'),
            "end_date": end_date.strftime('%Y-%m-%d %H:%M:%S'),
            "iteration_cap": iteration_cap,
            "trans_deal_flag": 0 if apply_type == 'Line' else 1,
            "group_id": f"{level_id}" if apply_type == 'Line' and level_id and level_id > 0 else None,
            "sort_order": level_id if level_id is not None and apply_type == 'Transaction' else 0
        }

        if apply_type == 'Transaction':
            DEAL['trwide_action'] = discount_type
            DEAL['trwide_amount'] = discount_value

        # DEAL = {k: v for k, v in DEAL.items() if v is not None}
        PRC_DEAL.append(DEAL)
        data_detail.append(
            {'table': 'PRC_DEAL', 'table_key': ['organization_id', 'deal_id'], "action": "INSERT_AND_UPDATE",
             "data": PRC_DEAL})

        if item_set == 1:
            DEAL_ITEM_1 = {
                **promotion_mapping["DEAL_ITEM_1"],
                "deal_id": promotion_id,
                "item_ordinal": 1,
                "consumable": 1 if overlap == 0 else 0,
                "qty_min": qty_min if condition_type == 'Quantity' else None,
                "qty_max": qty_max if condition_type == 'Quantity' else None,
                "min_item_total": MinItemTotal if condition_type == 'Amount' else None,
                "deal_action": discount_type if apply_type == 'Line' else None,
                "action_arg": discount_value if apply_type == 'Line' else None,
                "action_arg_qty": action_qty if action_qty and action_qty > 0 else None
            }
            # DEAL_ITEM_1 = {k: v for k, v in DEAL_ITEM_1.items() if v is not None}
            PRC_DEAL_ITEM.append(DEAL_ITEM_1)
        elif item_set == 2:
            for condition in promotion_condition_data:
                DEAL_ITEM_1 = {
                    **promotion_mapping["DEAL_ITEM_1"],
                    "deal_id": promotion_id,
                    "item_ordinal": condition.set_id,
                    "consumable": 1 if overlap == 0 else 0,
                    "qty_min": condition.MinQty if condition.condition_type == 'Quantity' else None,
                    "qty_max": condition.MaxQty if condition.condition_type == 'Quantity' else None,
                    "min_item_total": condition.MinItemTotal if condition.condition_type == 'Amount' else None
                }
                PRC_DEAL_ITEM.append(DEAL_ITEM_1)
            for result in promotion_result_data:
                DEAL_ITEM_2 = {
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
                PRC_DEAL_ITEM.append(DEAL_ITEM_2)

        data_detail.append(
            {'table': 'PRC_DEAL_ITEM', 'table_key': ['organization_id', 'deal_id'], "action": "DELETE_AND_INSERT",
             "data": PRC_DEAL_ITEM})

        serial_number = 1
        for item in promotion_item_segments_data:
            include = 'EQUAL' if item['include'] else 'NOT_EQUAL'
            item_type = 1 if item['item_type'] == 'Condition' else 2
            if item_set == 1 and item_type != 1:
                continue
            DEAL_ITEM_TEST = {
                **promotion_mapping["DEAL_ITEM_TEST"],
                "deal_id": promotion_id,
                "item_ordinal": item['set_id'],
                "item_condition_group": serial_number,
                "item_field": f"ITEM_PROPERTY:ITM_PROP_{item['segment_id']}",
                "match_rule": include
            }
            PRC_DEAL_FIELD_TEST.append(DEAL_ITEM_TEST)
            serial_number += 1
        data_detail.append(
            {'table': 'PRC_DEAL_FIELD_TEST', 'table_key': ['organization_id', 'deal_id'], "action": "DELETE_AND_INSERT",
             "data": PRC_DEAL_FIELD_TEST})

        data_detail.append(
            {'table': 'PRC_DEAL_LOC', 'table_key': ['organization_id', 'deal_id'], "action": "DELETE_AND_INSERT",
             "data": [{
                 **promotion_mapping["PRC_DEAL_LOC"],
                 "deal_id": promotion_id,
                 "rtl_loc_id": location_id
             }]})

        for cust in promotion_cust_segments_data:
            DEAL_TRIG = {
                **promotion_mapping["PRC_DEAL_TRIG"],
                "deal_id": promotion_id,
                "deal_trigger": f"SEGMENT:{'' if cust['include'] else '~'}{cust['segment_id']}"}
            PRC_DEAL_TRIG.append(DEAL_TRIG)

        if promotion_type and coupon_code:
            DEAL_TRIG = {
                **promotion_mapping["PRC_DEAL_TRIG"],
                "deal_id": promotion_id,
                "deal_trigger": f"COUPON:INPUT_COUPON:{coupon_code}"}
            PRC_DEAL_TRIG.append(DEAL_TRIG)

            DSC_COUPON_XREF = [{
                **promotion_mapping["DSC_COUPON_XREF"],
                "coupon_serial_nbr": coupon_code,
                "expiration_date": '2029-01-01' if promotion_status == 'active' else '2019-01-01'
            }]
            data_detail.append(
                {'table': 'DSC_COUPON_XREF', 'table_key': ['organization_id', 'coupon_serial_nbr'],
                 "action": "INSERT_AND_UPDATE",
                 "data": DSC_COUPON_XREF})
        if PRC_DEAL_TRIG:
            data_detail.append(
                {'table': 'PRC_DEAL_TRIG', 'table_key': ['organization_id', 'deal_id'], "action": "DELETE_AND_INSERT",
                 "data": PRC_DEAL_TRIG})

    return data_detail


async def get_promotion_dashboard(session: Session):
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
								a.promotion_id = b.promotion_id WHERE a.promotion_status='active'
        """)

    try:
        result = session.execute(sql)
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
