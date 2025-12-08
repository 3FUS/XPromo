from models.model import WorkerTask, PromotionNextSequence,WorkerTerminal
from sqlalchemy.orm import Session
from datetime import datetime


def generate_next_id(session: Session, sequence_type: str):
    # 获取当前的 last_segment_id
    sequence = session.query(PromotionNextSequence).filter_by(sequence_type=sequence_type).first()

    if not sequence:
        # 如果 sequence 不存在，创建一个新的记录
        sequence = PromotionNextSequence(sequence_type=sequence_type, next_sequence=10000)
        session.add(sequence)
        session.commit()
        session.refresh(sequence)

    # 获取当前的 last_segment_id 并递增
    current_id = sequence.next_sequence
    sequence.next_sequence += 1
    session.commit()

    return current_id


async def get_worker_next_task(session: Session, location_id: int, terminal_id: int):
    # 添加索引提示和限制返回数量
    query = session.query(WorkerTask).filter(
        WorkerTask.location_id == location_id,
        WorkerTask.terminal_id == terminal_id,
        WorkerTask.status == 'N'
    ).order_by(WorkerTask.data_seq).limit(1)  # 限制返回数量

    result = query.first()
    return result

async def update_worker_task(session: Session, location_id: int, terminal_id: int, session_id: int, status: str,
                             msg: str):
    # 使用 bulk_update_mappings 提高更新效率
    session.query(WorkerTask).filter(
        WorkerTask.location_id == location_id,
        WorkerTask.terminal_id == terminal_id,
        WorkerTask.session_id == session_id
    ).update({
        WorkerTask.status: status,
        WorkerTask.msg: msg,
        WorkerTask.update_time: datetime.now()
    }, synchronize_session=False)

    session.commit()


async def create_worker_task(session: Session, locs: [], data_type: str, data_key: str,
                             data_seq: int = 0, status: str = 'N'):
    sessionId = generate_next_id(session, "WorkerSession")
    for location_id in locs:

        terminals = session.query(WorkerTerminal).filter(
            WorkerTerminal.location_id == location_id,
            WorkerTerminal.active == 1  # 只查询激活的终端
        ).all()

        if not terminals:
            terminals = [type('Terminal', (), {'terminal_id': 1})()]

        for terminal in terminals:
            worker_task = WorkerTask(
                location_id=location_id,
                terminal_id=terminal.terminal_id,
                session_id=sessionId,
                data_type=data_type,
                data_key=data_key,
                data_seq=data_seq,
                status=status,
                create_time=datetime.now(),
            )
            session.add(worker_task)
    session.commit()
    session.refresh(worker_task)
    return sessionId
