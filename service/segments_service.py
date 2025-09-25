from models.model import \
    SegmentsItem, SegmentsItemCondition, SegmentsItemDetail, SegmentsSchedule, SegmentsLocation, \
    SegmentsLocationCondition, SegmentsCustomer, \
    SegmentsCustomerCondition, SegmentsImport, SegmentsLocationDetail, SegmentsCustomerDetail, PromotionNextSequence

from models.model import PromotionItemSegments, PromotionLocationSegments, PromotionCustomerSegments, WorkerTask

from schemas import SegmentsItemCreate, SegmentsItemUpdate, Segment_Type
from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy import text, case, func
import yaml

def generate_segment_id(session: Session, sequence_type: str):
    # 获取当前的 last_segment_id
    sequence = session.query(PromotionNextSequence).filter_by(sequence_type=sequence_type).first()

    if not sequence:
        # 如果 sequence 不存在，创建一个新的记录
        sequence = PromotionNextSequence(sequence_type=sequence_type, next_sequence=50000)
        session.add(sequence)
        session.commit()
        session.refresh(sequence)

    # 获取当前的 last_segment_id 并递增
    current_id = sequence.next_sequence
    sequence.next_sequence += 1
    session.commit()

    return current_id


async def get_item_segment_by_name(session, name=None):
    query = session.query(SegmentsItem)
    if name:
        query = query.filter(SegmentsItem.name == name)
    return query.all()


async def get_customer_segment_by_name(session, name=None):
    query = session.query(SegmentsCustomer)
    if name:
        query = query.filter(SegmentsCustomer.name == name)
    return query.all()


async def get_location_segment_by_name(session, name=None):
    query = session.query(SegmentsLocation)
    if name:
        query = query.filter(SegmentsLocation.name == name)
    return query.all()


async def get_item_segment_by_id(session, segment_id):
    query = session.query(SegmentsItem)
    if segment_id:
        query = query.filter(SegmentsItem.segment_id == segment_id)
    return query.all()


async def get_location_segment_by_id(session, segment_id):
    queue = session.query(SegmentsLocation)
    if segment_id:
        queue = queue.filter(SegmentsLocation.segment_id == segment_id)
    return queue.all()


async def get_customer_segment_by_id(session, segment_id):
    query = session.query(SegmentsCustomer)
    if segment_id:
        query = query.filter(SegmentsCustomer.segment_id == segment_id)
    return query.all()


async def get_item_segment_condition_by_id(session, segment_id):
    query = session.query(SegmentsItemCondition)
    if segment_id:
        query = query.filter(SegmentsItemCondition.segment_id == segment_id)
    return query.all()


async def get_customer_segment_condition_by_id(session, segment_id):
    query = session.query(SegmentsCustomerCondition)
    if segment_id:
        query = query.filter(SegmentsCustomerCondition.segment_id == segment_id)
    return query.all()


async def get_location_segment_condition_by_id(session, segment_id):
    query = session.query(SegmentsLocationCondition)
    if segment_id:
        query = query.filter(SegmentsLocationCondition.segment_id == segment_id)
    return query.all()


async def create_segment_item(session: Session, item_segment: SegmentsItemCreate, user_id='system'):
    new_item_segment = SegmentsItem(
        segment_id=generate_segment_id(session, "Item"),  # 添加: 自动生成 segment_id
        name=item_segment.name,
        description=item_segment.description,
        create_type=item_segment.create_type,
        segment_status=item_segment.segment_status,
        condition_type=item_segment.condition_type,
        create_time=datetime.now(),  # 添加: 设置创建时间为当前系统时间
        create_user=user_id,  # 假设创建用户为系统
    )
    session.add(new_item_segment)
    session.commit()
    session.refresh(new_item_segment)
    return new_item_segment


async def create_segment_location(session: Session, location_segment, user_id='system'):
    new_location_segment = SegmentsLocation(
        segment_id=generate_segment_id(session, "Location"),  # 添加: 自动生成 segment_id
        name=location_segment.name,
        description=location_segment.description,
        create_type=location_segment.create_type,
        segment_status=location_segment.segment_status,
        condition_type=location_segment.condition_type,
        create_time=datetime.now(),  # 添加: 设置创建时间为当前系统时间
        create_user=user_id,  # 假设创建用户为系统
    )
    session.add(new_location_segment)
    session.commit()
    session.refresh(new_location_segment)
    return new_location_segment


async def create_segment_customer(session: Session, customer_segment, user_id='system'):
    new_customer_segment = SegmentsCustomer(
        segment_id=generate_segment_id(session, "Customer"),  # 添加: 自动生成 segment_id
        name=customer_segment.name,
        description=customer_segment.description,
        create_type=customer_segment.create_type,
        segment_status=customer_segment.segment_status,
        condition_type=customer_segment.condition_type,
        create_time=datetime.now(),  # 添加: 设置创建时间为当前系统时间
        create_user=user_id,  # 假设创建用户为系统
    )
    session.add(new_customer_segment)
    session.commit()
    session.refresh(new_customer_segment)
    return new_customer_segment


async def update_segment_item(session: Session, item_segment_id: int, item_segment: SegmentsItemUpdate):
    updated_segment = session.query(SegmentsItem).filter(SegmentsItem.segment_id == item_segment_id).first()
    if updated_segment:
        updated_segment.name = item_segment.name
        updated_segment.description = item_segment.description
        updated_segment.create_type = item_segment.create_type
        updated_segment.segment_status = item_segment.segment_status
        updated_segment.condition_type = item_segment.condition_type
        updated_segment.sub_count = item_segment.sub_count
        updated_segment.update_time = datetime.now()  # 添加: 更新时间为当前系统时间
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


# async def update_segment_item(session: Session, item_segment_id: int, item_segment: SegmentsItemUpdate):
#     updated_segment = session.query(SegmentsItem).filter(SegmentsItem.segment_id == item_segment_id).first()
#
#     if not updated_segment:
#         return None  # 或抛出异常，视业务需求而定
#
#     update_data = item_segment.dict(exclude_unset=True)
#
#     for field in update_data:
#         setattr(updated_segment, field, update_data[field])
#     updated_segment.update_time = datetime.now()
#
#     session.commit()
#     session.refresh(updated_segment)
#     return updated_segment
async def update_segment_location(session: Session, location_segment_id: int, location_segment):
    updated_segment = session.query(SegmentsLocation).filter(SegmentsLocation.segment_id == location_segment_id).first()
    if updated_segment:
        updated_segment.name = location_segment.name
        updated_segment.description = location_segment.description
        updated_segment.create_type = location_segment.create_type
        updated_segment.segment_status = location_segment.segment_status
        updated_segment.condition_type = location_segment.condition_type
        updated_segment.sub_count = location_segment.sub_count
        updated_segment.update_time = datetime.now()  # 添加: 更新时间为当前系统时间
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


async def update_segment_customer(session: Session, customer_segment_id: int, customer_segment):
    updated_segment = session.query(SegmentsCustomer).filter(SegmentsCustomer.segment_id == customer_segment_id).first()
    if updated_segment:
        updated_segment.name = customer_segment.name
        updated_segment.description = customer_segment.description
        updated_segment.create_type = customer_segment.create_type
        updated_segment.segment_status = customer_segment.segment_status
        updated_segment.condition_type = customer_segment.condition_type
        updated_segment.sub_count = customer_segment.sub_count
        updated_segment.update_time = datetime.now()
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


async def delete_segment_item(session: Session, item_segment_id: int):
    deleted_segment = session.query(SegmentsItem).filter(SegmentsItem.segment_id == item_segment_id).first()
    if deleted_segment:
        session.delete(deleted_segment)
        session.commit()
    return deleted_segment


async def delete_segment_location(session: Session, segment_id: int):
    deleted_segment = session.query(SegmentsLocation).filter(SegmentsLocation.segment_id == segment_id).first()
    if deleted_segment:
        session.delete(deleted_segment)
        session.commit()
    return deleted_segment


async def delete_segment_customer(session: Session, segment_id: int):
    deleted_segment = session.query(SegmentsCustomer).filter(SegmentsCustomer.segment_id == segment_id).first()
    if deleted_segment:
        session.delete(deleted_segment)
        session.commit()
    return deleted_segment


async def create_segment_item_condition(session, segment_id, item_segment_conditions):
    created_conditions = []
    try:
        for item_segment_condition in item_segment_conditions:
            db_item_segment_condition = SegmentsItemCondition(
                segment_id=segment_id,
                condition_name=item_segment_condition.condition_name,
                condition_type=item_segment_condition.condition_type,
                condition_value=item_segment_condition.condition_value,
                create_user=item_segment_condition.create_user,
                create_time=datetime.now()
            )
            session.add(db_item_segment_condition)
            created_conditions.append(db_item_segment_condition)

        session.commit()

        for condition in created_conditions:
            session.refresh(condition)
        return created_conditions

    except Exception as e:
        session.rollback()
        raise e


async def create_segment_location_condition(session, segment_id, location_segment_conditions):
    created_conditions = []
    for location_segment_condition in location_segment_conditions:
        db_location_segment_condition = SegmentsLocationCondition(
            segment_id=segment_id,
            condition_name=location_segment_condition.condition_name,
            condition_type=location_segment_condition.condition_type,
            condition_value=location_segment_condition.condition_value,
            create_user=location_segment_condition.create_user,
            create_time=datetime.now()
        )
        session.add(db_location_segment_condition)
        created_conditions.append(db_location_segment_condition)

    session.commit()
    return created_conditions


async def create_segment_customer_condition(session, segment_id, customer_segment_conditions):
    created_conditions = []
    for customer_segment_condition in customer_segment_conditions:
        db_customer_segment_condition = SegmentsCustomerCondition(
            segment_id=segment_id,
            condition_name=customer_segment_condition.condition_name,
            condition_type=customer_segment_condition.condition_type,
            condition_value=customer_segment_condition.condition_value,
            create_user=customer_segment_condition.create_user,
            create_time=datetime.now()
        )
        session.add(db_customer_segment_condition)
        created_conditions.append(db_customer_segment_condition)

    session.commit()
    return created_conditions


async def delete_segment_item_condition(session, segment_id=None):
    query = session.query(SegmentsItemCondition)
    if segment_id is not None:
        query = query.filter(SegmentsItemCondition.segment_id == segment_id)
        deleted_count = query.delete(synchronize_session=False)
        session.commit()
        return deleted_count


async def delete_segment_location_condition(session, segment_id=None):
    query = session.query(SegmentsLocationCondition)
    if segment_id is not None:
        query = query.filter(SegmentsLocationCondition.segment_id == segment_id)
        deleted_count = query.delete(synchronize_session=False)
        session.commit()
        return deleted_count


async def delete_segment_customer_condition(session, segment_id=None):
    query = session.query(SegmentsCustomerCondition)
    if segment_id is not None:
        query = query.filter(SegmentsCustomerCondition.segment_id == segment_id)
    deleted_conditions = query.all()
    if deleted_conditions:
        for condition in deleted_conditions:
            session.delete(condition)
        session.commit()
    return deleted_conditions


# 新增: create_segment_item_schedule 方法
async def create_segment_schedule(session, segment_id, segment_type, schedule):
    new_schedule = SegmentsSchedule(
        segment_id=segment_id,
        segment_type=segment_type,
        schedule_type=schedule.schedule_type,
        schedule_value=schedule.schedule_value,
        schedule_time=schedule.schedule_time,
        create_time=datetime.now(),
        create_user=schedule.create_user,

    )
    session.add(new_schedule)
    session.commit()
    session.refresh(new_schedule)
    return new_schedule


# 新增: delete_segment_item_schedule 方法
async def delete_segment_schedule(session, segment_id, segment_type):
    """
    根据 segment_id 和 segment_type 删除 SegmentsItemSchedule 记录。

    :param session: 数据库会话对象
    :param segment_id: 段ID
    :param segment_type: 段类型
    :return: 被删除的 SegmentsItemSchedule 记录对象，如果没有找到则返回 None
    """
    deleted_schedule = session.query(SegmentsSchedule).filter(
        SegmentsSchedule.segment_id == segment_id,
        SegmentsSchedule.segment_type == segment_type
    ).first()
    if deleted_schedule:
        session.delete(deleted_schedule)
        session.commit()
    return deleted_schedule


# 新增: read_item_segments 方法

async def get_item_segment_detail_by_id(session, segment_id):
    query = session.query(SegmentsItemDetail)
    if segment_id:
        query = query.filter(SegmentsItemDetail.segment_id == segment_id)
    return query.all()


async def get_item_segment_schedule_by_id(session, segment_id, segment_type):
    query = session.query(SegmentsSchedule)
    if segment_id:
        query = query.filter(SegmentsSchedule.segment_id == segment_id,
                             SegmentsSchedule.segment_type == segment_type)
    return query.all()


async def update_segment_item_status(session, segment_id, segment_status):
    updated_segment = session.query(SegmentsItem).filter(SegmentsItem.segment_id == segment_id).first()
    if updated_segment:
        updated_segment.segment_status = segment_status
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


async def update_segment_some(segment_type, session, segment_id, data):
    model_map = {
        Segment_Type.item: SegmentsItem,
        Segment_Type.location: SegmentsLocation,
        Segment_Type.customer: SegmentsCustomer,
    }

    if segment_type not in model_map:
        raise ValueError(f"Invalid segment_type: {segment_type}")

    model_class = model_map[segment_type]
    updated_segment = session.query(model_class).filter(model_class.segment_id == segment_id).first()

    if updated_segment:
        # 批量提取 data 字段
        export_time = data.get('export_time')
        run_time = data.get('run_time')
        sub_count = data.get('sub_count')
        update_time = data.get('update_time')
        last_session_id = data.get('last_session_id')

        # 更新字段
        if export_time is not None:
            updated_segment.last_export_time = export_time
        if run_time is not None:
            updated_segment.last_run_time = run_time
        if sub_count is not None:
            updated_segment.sub_count = sub_count
        if update_time is not None:
            updated_segment.update_time = update_time
        if last_session_id is not None:
            updated_segment.last_session_id = last_session_id

        try:
            session.commit()
            session.refresh(updated_segment)
        except Exception as e:
            session.rollback()
            raise RuntimeError(f"Database commit failed: {e}") from e

    return updated_segment


async def update_segment_location_status(session, segment_id, segment_status):
    updated_segment = session.query(SegmentsLocation).filter(SegmentsLocation.segment_id == segment_id).first()
    if updated_segment:
        updated_segment.segment_status = segment_status
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


async def update_segment_customer_status(session, segment_id, segment_status):
    updated_segment = session.query(SegmentsCustomer).filter(SegmentsCustomer.segment_id == segment_id).first()
    if updated_segment:
        updated_segment.segment_status = segment_status
        session.commit()
        session.refresh(updated_segment)
    return updated_segment


async def get_segments_item_list(session, key_word=None, segment_status=None, page=1, page_size=30):
    # query = session.query(SegmentsItem)
    try:
        query = session.query(
            SegmentsItem.segment_id,
            SegmentsItem.name,
            SegmentsItem.description,
            SegmentsItem.create_type,
            SegmentsItem.sub_count,
            SegmentsItem.segment_status,
            SegmentsItem.create_time,
            SegmentsItem.create_user,
            SegmentsItem.last_export_time,
            func.sum(case((WorkerTask.status == 'N', 1), else_=0)).label('task_count_N'),
            func.sum(case((WorkerTask.status == 'E', 1), else_=0)).label('task_count_E'),
            func.sum(case((WorkerTask.status == 'D', 1), else_=0)).label('task_count_D')
        ).outerjoin(
            WorkerTask,
            SegmentsItem.last_session_id == WorkerTask.session_id
        ).group_by(SegmentsItem.segment_id,
                   SegmentsItem.name,
                   SegmentsItem.description,
                   SegmentsItem.create_type,
                   SegmentsItem.sub_count,
                   SegmentsItem.segment_status,
                   SegmentsItem.create_time,
                   SegmentsItem.create_user,
                   SegmentsItem.last_export_time)

        if key_word:
            key_word = f"%{key_word}%"  # 添加通配符以支持模糊查询
            query = query.filter(
                (SegmentsItem.name.like(key_word)) |
                (SegmentsItem.description.like(key_word)) |
                (SegmentsItem.create_user.like(key_word))
            )
        if segment_status != 'ALL':
            query = query.filter(SegmentsItem.segment_status == segment_status)

        query = query.order_by(SegmentsItem.create_time.desc())

        total = query.count()
        items = query.offset((page - 1) * page_size).limit(page_size).all()

        formatted_items = [
            {
                "segment_id": item.segment_id,
                "name": item.name,
                "create_type": item.create_type,
                "sub_count": item.sub_count,
                "segment_status": item.segment_status,
                "description": item.description,
                "create_time": item.create_time.strftime('%Y-%m-%d %H:%M'),
                "create_user": item.create_user,
                'export_time': item.last_export_time.strftime('%Y-%m-%d %H:%M') if item.last_export_time else None,
                "export_status_counts": {
                    "New": item.task_count_N or 0,
                    "Error": item.task_count_E or 0,
                    "Done": item.task_count_D or 0}
            }
            for item in items
        ]
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": formatted_items
        }
    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}") from e


async def get_segments_location_list(session, key_word=None, segment_status=None, page=1, page_size=30):
    query = session.query(SegmentsLocation)
    if key_word:
        key_word = f"%{key_word}%"  # 添加通配符以支持模糊查询
        query = query.filter(
            (SegmentsLocation.name.like(key_word)) |
            (SegmentsLocation.description.like(key_word)) |
            (SegmentsLocation.create_user.like(key_word))
        )

    if segment_status != 'ALL':
        query = query.filter(SegmentsLocation.segment_status == segment_status)

    query = query.order_by(SegmentsLocation.create_time.desc())

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()

    formatted_items = [
        {**item.__dict__, 'create_time': item.create_time.strftime('%Y-%m-%d %H:%M')}
        for item in items
    ]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": formatted_items
    }


async def get_segments_customer_list(session, key_word=None, segment_status=None, page=1, page_size=30):
    query = session.query(SegmentsCustomer)
    if key_word:
        key_word = f"%{key_word}%"  # 添加通配符以支持模糊查询
        query = query.filter(
            (SegmentsCustomer.name.like(key_word)) |
            (SegmentsCustomer.description.like(key_word)) |
            (SegmentsCustomer.create_user.like(key_word))
        )

    if segment_status != 'ALL':
        query = query.filter(SegmentsCustomer.segment_status == segment_status)

    query = query.order_by(SegmentsCustomer.create_time.desc())

    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all()
    #
    formatted_items = [
        {**item.__dict__, 'create_time': item.create_time.strftime('%Y-%m-%d %H:%M')}
        for item in items
    ]

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": formatted_items
    }


async def get_segments_item_detail(session, segment_id=None, key_word=None, page=1, page_size=40):
    query = session.query(SegmentsItemDetail)
    if segment_id:
        query = query.filter(SegmentsItemDetail.segment_id == segment_id)
    if key_word:
        key_word = f"%{key_word}%"  # 添加通配符以支持模糊查询
        query = query.filter(
            (SegmentsItemDetail.item_id.like(key_word)) |
            (SegmentsItemDetail.item_name.like(key_word)) |
            (SegmentsItemDetail.item_description.like(key_word)) |
            (SegmentsItemDetail.item_department.like(key_word))
        )
    query = query.order_by(SegmentsItemDetail.create_time.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": items
    }


async def get_setgments_location_detail(session, segment_id, key_word=None, page=1, page_size=40):
    query = session.query(SegmentsLocationDetail)
    if segment_id:
        query = query.filter(SegmentsLocationDetail.segment_id == segment_id)
    if key_word:
        key_word = f"%{key_word}%"
        query = query.filter(
            (SegmentsLocationDetail.rtl_loc_id.like(key_word)) |
            (SegmentsLocationDetail.store_name.like(key_word)) |
            (SegmentsLocationDetail.city.like(key_word))
        )
    query = query.order_by(SegmentsLocationDetail.create_time.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": items
    }


async def get_segments_customer_detail(session, segment_id, key_word=None, page=1, page_size=40):
    query = session.query(SegmentsCustomerDetail)
    if segment_id:
        query = query.filter(SegmentsCustomerDetail.segment_id == segment_id)
    if key_word:
        key_word = f"%{key_word}%"
        query = query.filter(
            (SegmentsCustomerDetail.cust_phone.like(key_word)) |
            (SegmentsCustomerDetail.first_name.like(key_word)) |
            (SegmentsCustomerDetail.last_name.like(key_word))
        )

    query = query.order_by(SegmentsCustomerDetail.create_time.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": items
    }


async def create_item_segments_detail(session, segment_id, item_segment_details
                                      ):
    created_details = []
    for item_segment_detail in item_segment_details:
        db_item_segment_detail = SegmentsItemDetail(
            segment_id=segment_id,
            item_id=item_segment_detail.item_id,
            create_time=datetime.now()
        )
        session.add(db_item_segment_detail)
        created_details.append(db_item_segment_detail)
    session.commit()
    for detail in created_details:
        session.refresh(detail)
    return created_details


async def create_customer_segments_detail(session, segment_id, cust_segment_details
                                          ):
    created_details = []
    for cust_segment_detail in cust_segment_details:
        db_cust_segment_detail = SegmentsCustomerDetail(
            segment_id=segment_id,
            cust_phone=cust_segment_detail.cust_phone,
            create_time=datetime.now()
        )
        session.add(db_cust_segment_detail)
        created_details.append(db_cust_segment_detail)
    session.commit()
    for detail in created_details:
        session.refresh(detail)
    return created_details


async def create_location_segments_detail(session, segment_id, location_segment_details
                                          ):
    created_details = []
    for location_segment_detail in location_segment_details:
        db_location_segment_detail = SegmentsLocationDetail(
            segment_id=segment_id,
            rtl_loc_id=location_segment_detail.rtl_loc_id,
            create_time=datetime.now()
        )
        session.add(db_location_segment_detail)
        created_details.append(db_location_segment_detail)
    session.commit()
    for detail in created_details:
        session.refresh(detail)
    return created_details


async def delete_segment_item_detail(session, segment_id=None):
    query = session.query(SegmentsItemDetail)
    if segment_id is not None:
        query = query.filter(SegmentsItemDetail.segment_id == segment_id)
        deleted_count = query.delete(synchronize_session=False)
        session.commit()
        return deleted_count


async def delete_segment_customer_detail(session, segment_id=None):
    query = session.query(SegmentsCustomerDetail)
    if segment_id is not None:
        query = query.filter(SegmentsCustomerDetail.segment_id == segment_id)
        deleted_count = query.delete(synchronize_session=False)
        session.commit()
        return deleted_count


async def delete_segment_location_detail(session, segment_id=None):
    query = session.query(SegmentsLocationDetail)
    if segment_id is not None:
        query = query.filter(SegmentsLocationDetail.segment_id == segment_id)
        deleted_count = query.delete(synchronize_session=False)
        session.commit()
        return deleted_count


async def get_segment_import_by_id(session, segment_id, segment_type):
    query = session.query(SegmentsImport)
    if segment_id is not None:
        query = query.filter(SegmentsImport.segment_id == segment_id)
        if segment_type is not None:
            query = query.filter(SegmentsImport.segment_type == segment_type)

        return query.first()


async def create_segment_import(session: Session, segment_id, segment_type, file_name, count_success):
    new_segment_import = SegmentsImport(
        segment_id=segment_id,
        segment_type=segment_type,
        file_name=file_name,
        count_success=count_success,
        create_time=datetime.now()  # 添加: 设置创建时间为当前系统时间
    )
    session.add(new_segment_import)
    session.commit()
    session.refresh(new_segment_import)
    return new_segment_import


async def delete_segment_import(session, segment_id=None, segment_type=None):
    query = session.query(SegmentsImport)
    if segment_id is not None:
        query = query.filter(SegmentsImport.segment_id == segment_id)
    if segment_type is not None:
        query = query.filter(SegmentsImport.segment_type == segment_type)

    deleted_count = query.delete(synchronize_session=False)
    session.commit()
    return deleted_count


async def get_segments_by_phone(session, phone_number):
    segments_list = (session.query(
        SegmentsCustomer.segment_id
    ).distinct(SegmentsCustomer.segment_id)
                     .join(
        SegmentsCustomerDetail,
        SegmentsCustomer.segment_id == SegmentsCustomerDetail.segment_id)
                     .filter(SegmentsCustomerDetail.cust_phone == phone_number))

    result = [
        {
            "segment_id": Id_list.segment_id
        }
        for Id_list in segments_list.all()
    ]

    return result


async def get_item_segments_in_use_by_id(session, segment_id):
    promotion_item_segments = (session.query(
        PromotionItemSegments.promotion_id)
                               .join(
        SegmentsItem,
        SegmentsItem.segment_id == PromotionItemSegments.segment_id
    )
                               .filter(SegmentsItem.segment_id == segment_id)
                               )

    result = [
        {"promotion_id": segment.promotion_id}
        for segment in promotion_item_segments.all()
    ]

    return result


async def get_location_segments_in_use_by_id(session, segment_id):
    promotion_location_segments = (session.query(
        PromotionLocationSegments.promotion_id)
                                   .join(
        SegmentsLocation,
        SegmentsLocation.segment_id == PromotionLocationSegments.segment_id
    )
                                   .filter(SegmentsLocation.segment_id == segment_id)
                                   )

    result = [
        {"promotion_id": segment.promotion_id}
        for segment in promotion_location_segments.all()
    ]

    return result


async def get_customer_in_use_by_id(session, segment_id):
    promotion_customer_segments = (session.query(
        PromotionCustomerSegments.promotion_id)
                                   .join(
        SegmentsCustomer,
        SegmentsCustomer.segment_id == PromotionCustomerSegments.segment_id
    )
                                   .filter(SegmentsCustomer.segment_id == segment_id)
                                   )

    result = [
        {"promotion_id": segment.promotion_id}
        for segment in promotion_customer_segments.all()
    ]

    return result


async def get_location_segments_in_use_by_id(session, segment_id):
    promotion_item_segments = (session.query(
        PromotionLocationSegments.promotion_id)
                               .join(
        SegmentsLocation,
        SegmentsLocation.segment_id == PromotionLocationSegments.segment_id
    )
                               .filter(SegmentsLocation.segment_id == segment_id)
                               )

    result = [
        {"promotion_id": segment.promotion_id}
        for segment in promotion_item_segments.all()
    ]

    return result


async def get_customer_segments_in_use_by_id(session, segment_id):
    promotion_item_segments = (session.query(
        PromotionCustomerSegments.promotion_id)
                               .join(
        SegmentsCustomer,
        SegmentsCustomer.segment_id == PromotionCustomerSegments.segment_id
    )
                               .filter(SegmentsCustomer.segment_id == segment_id)
                               )

    result = [
        {"promotion_id": segment.promotion_id}
        for segment in promotion_item_segments.all()
    ]

    return result


async def get_store_list(session, key_word=None, page=1, page_size=40):
    query = session.query(SegmentsLocationDetail.rtl_loc_id, SegmentsLocationDetail.store_name)
    if key_word:
        key_word = f"%{key_word}%"
        query = query.filter(
            (SegmentsLocationDetail.rtl_loc_id.like(key_word)) |
            (SegmentsLocationDetail.store_name.like(key_word))
        )
    query = query.distinct().order_by(SegmentsLocationDetail.rtl_loc_id.desc())
    total = query.count()
    items = query.offset((page - 1) * page_size).limit(page_size).all() if page_size > 0 else query.all()
    data = [{"rtl_loc_id": item.rtl_loc_id, "store_name": item.store_name} for item in items]
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": data
    }


mapping_file = open('./config/mapping.yaml', 'r', encoding='utf-8')
mapping_config = yaml.safe_load(mapping_file)

segment_mapping = mapping_config.get("segment_mapping", {})
async def process_segment_data(segment_id: int, session):
    data_detail = []
    item_data = await get_segments_item_detail(session, segment_id, None, page=1, page_size=1000)

    segment_status = 'active'
    begin_date = '1900-01-01 00:00:00'

    ITM_ITEM_DEAL_PROP = []
    if segment_status == 'active':
        if item_data['total'] > 0:
            for item in item_data['data']:
                ITM_ITEM_DEAL_PROP.append({
                    **segment_mapping["ITM_ITEM_DEAL_PROP"],
                    "item_id": item.item_id,
                    "effective_date": begin_date,
                    "itm_deal_property_code": f"ITM_PROP_{segment_id}"
                })
    data_detail.append(
        {'table': 'ITM_ITEM_DEAL_PROP', 'table_key': ['organization_id', 'itm_deal_property_code'],
         "action": "DELETE_AND_INSERT",
         "data": ITM_ITEM_DEAL_PROP})
    return data_detail


async def get_segment_item_dashboard(session: Session, segment_type=None):
    if segment_type == Segment_Type.item:
        sql = text("""
            SELECT 
                COUNT(DISTINCT a.segment_id) AS 'Total',
                COUNT(DISTINCT case when segment_status='active' then a.segment_id ELSE NULL END) AS 'Active',
                COUNT(DISTINCT case when b.segment_id IS NULL then NULL ELSE a.segment_id END) AS 'In_Use'
            FROM 
                segments_items a 
            LEFT JOIN 
                promotions_item_segments b 
            ON 
                a.segment_id = b.segment_id
        """)
    elif segment_type == Segment_Type.customer:
        sql = text("""
            SELECT 
                COUNT(DISTINCT a.segment_id) AS 'Total',
                COUNT(DISTINCT case when segment_status='active' then a.segment_id ELSE NULL END) AS 'Active',
                COUNT(DISTINCT case when b.segment_id IS NULL then NULL ELSE a.segment_id END) AS 'In_Use'
            FROM 
                segments_customers a 
            LEFT JOIN 
                promotions_customer_segments b 
                            ON 
                a.segment_id = b.segment_id"""
                   )
    elif segment_type == Segment_Type.location:
        sql = text("""
            SELECT 
                COUNT(DISTINCT a.segment_id) AS 'Total',
                COUNT(DISTINCT case when segment_status='active' then a.segment_id ELSE NULL END) AS 'Active',
                COUNT(DISTINCT case when b.segment_id IS NULL then NULL ELSE a.segment_id END) AS 'In_Use'
            FROM 
                segments_locations a 
            LEFT JOIN 
            promotions_location_segments b 
            ON 
                a.segment_id = b.segment_id"""
                   )
    try:
        result = session.execute(sql)
        data = result.fetchone()
        if data is None:
            return {"Total": 0, "Active": 0, "In_Use": 0}
        return {"Total": data[0], "Active": data[1], "In_Use": data[2]}
    except Exception as e:
        # 处理异常，例如记录日志或抛出自定义异常
        raise Exception(f"An error occurred while executing the SQL query: {e}")
