from models.model import WorkerTask, PromotionNextSequence
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
    query = session.query(WorkerTask).filter(
        WorkerTask.location_id == location_id,
        WorkerTask.terminal_id == terminal_id,
        WorkerTask.status == 'N'
    )
    result = query.first()
    return result


async def update_worker_task(session: Session, location_id: int, terminal_id: int, session_id: int, status: str,
                             msg: str):
    query = session.query(WorkerTask).filter(
        WorkerTask.location_id == location_id,
        WorkerTask.terminal_id == terminal_id,
        WorkerTask.session_id == session_id
    )
    result = query.first()
    result.status = status
    result.msg = msg
    result.update_time = datetime.now()
    session.commit()
    session.refresh(result)
    return result


async def create_worker_task(session: Session, locs: [], data_type: str, data_key: str,
                             data_seq: int = 0, status: str = 'N'):
    sessionId = generate_next_id(session, "WorkerSession")
    for location_id in locs:
        worker_task = WorkerTask(
            location_id=location_id,
            terminal_id=1,
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
