from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import case, func, text, cast, String
from sqlalchemy.orm import Session
from utils.logger import app_logger
from models.inventory_model import (
    InventoryTask,
    InventoryOrder,
    InventoryItem,
    InventoryDetail,
    InventorySnapshotDetail,
    InventoryNextSequence
)
from models.xstore_model import (ItmItemCrossReference, ItmItem, ItmItemOptions, LocRtlLoc)
import time
import pandas as pd

import yaml
from utils.path_utils import get_config_path

_INVENTORY_COLUMNS_CONFIG = None


def _load_inventory_columns_config():
    global _INVENTORY_COLUMNS_CONFIG
    if _INVENTORY_COLUMNS_CONFIG is None:
        config_path = get_config_path('translations_inventory.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            _INVENTORY_COLUMNS_CONFIG = yaml.safe_load(f)
    return _INVENTORY_COLUMNS_CONFIG


def get_table_columns(table_name: str, lang: str = "en") -> list:
    config = _load_inventory_columns_config()
    labels = config.get("labels", {})
    fields = config.get("tables", {}).get(table_name, [])
    result = []
    for field in fields:
        label_dict = labels.get(field, {})
        result.append({
            "field": field,
            "label": label_dict.get(lang, label_dict.get("en", field)),
        })
    return result


def translate_value(field: str, raw_value: str, lang: str = "en") -> str:
    if raw_value is None:
        return None
    config = _load_inventory_columns_config()
    value_labels = config.get("value_labels", {})
    field_labels = value_labels.get(field, {})
    label_dict = field_labels.get(raw_value, {})
    return label_dict.get(lang, label_dict.get("en", raw_value))


def generate_inventory_id(session: Session, sequence_type: str = 'task'):
    # 获取当前的 last_segment_id
    sequence = session.query(InventoryNextSequence).filter_by(sequence_type=sequence_type).first()

    if not sequence:
        # 如果 sequence 不存在，创建一个新的记录
        sequence = InventoryNextSequence(sequence_type=sequence_type, next_sequence=10000)
        session.add(sequence)
        session.commit()
        session.refresh(sequence)

    # 获取当前的 last_segment_id 并递增
    current_id = sequence.next_sequence
    sequence.next_sequence += 1
    session.commit()

    return current_id


def generate_order_no(session: Session, task_id: int) -> str:
    task = session.query(InventoryTask).filter(InventoryTask.task_id == task_id).first()
    if not task:
        raise ValueError(f"task_id={task_id} 不存在")

    date_str = task.inventory_date.strftime("%Y%m%d")
    store_code = task.store_code
    prefix = f"INV{date_str}{store_code}"

    max_order = session.query(
        func.max(InventoryOrder.order_no)
    ).filter(
        InventoryOrder.order_no.like(f"{prefix}%")
    ).scalar()

    if max_order:
        seq_str = max_order[len(prefix):]
        next_seq = int(seq_str) + 1
    else:
        next_seq = 1

    return f"{prefix}{next_seq}"


def get_item_prices(session: Session, item_ids: list, org_id: str) -> Dict[str, float]:
    if not item_ids:
        return {}
    organization_id = 1
    now = datetime.now()
    placeholders = ", ".join([f":iid_{i}" for i in range(len(item_ids))])
    params = {f"iid_{i}": str(iid) for i, iid in enumerate(item_ids)}
    params.update({
        "org_id": organization_id,
        "level_value": org_id,
        "prop_code": "REGULAR_PRICE",
        "now": now,
    })
    rows = session.execute(
        text(f"""
             SELECT t.item_id, t.price
             FROM (
                 SELECT item_id, price,
                        ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY effective_date DESC) AS rn
                 FROM itm_item_prices
                 WHERE item_id IN ({placeholders})
                   AND organization_id = :org_id
                   AND level_value = :level_value
                   AND itm_price_property_code = :prop_code
                   AND effective_date <= :now
                   AND (expiration_date IS NULL OR expiration_date >= :now)
             ) t
             WHERE t.rn = 1
             """),
        params
    ).all()
    return {str(r.item_id).strip(): float(r.price) for r in rows}


async def scan_item(
        session: Session,
        org_id: str,
        scan_code: str
) -> Dict[str, Any]:
    try:
        scan_code = scan_code.strip()
        organization_id = 1

        t0 = time.perf_counter()

        cross_ref = session.query(ItmItemCrossReference.item_id, ItmItemCrossReference.manufacturer_upc).filter(
            ItmItemCrossReference.organization_id == organization_id,
            ItmItemCrossReference.manufacturer_upc == scan_code
        ).first()
        t1 = time.perf_counter()
        if cross_ref:
            item_id = cross_ref.item_id
            upc = cross_ref.manufacturer_upc
            app_logger.info(
                f"[scan_item] UPC命中: scan_code={scan_code}, item_id={item_id}, 耗时={((t1 - t0) * 1000):.1f}ms")
        else:
            item_id = scan_code
            upc_ref = session.query(ItmItemCrossReference.manufacturer_upc).filter(
                ItmItemCrossReference.organization_id == organization_id,
                ItmItemCrossReference.item_id == item_id
            ).first()
            upc = upc_ref.manufacturer_upc if upc_ref else None
            t1 = time.perf_counter()
            app_logger.info(
                f"[scan_item] item_id反查UPC: item_id={item_id}, upc={upc}, 耗时={((t1 - t0) * 1000):.1f}ms")

        t2 = time.perf_counter()
        itm_item = session.query(ItmItem.item_id, ItmItem.name, ItmItem.dimension1).filter(
            ItmItem.organization_id == organization_id,
            ItmItem.item_id == item_id
        ).first()
        t3 = time.perf_counter()
        app_logger.info(
            f"[scan_item] 查询商品: item_id={item_id}, found={itm_item is not None}, 耗时={((t3 - t2) * 1000):.1f}ms")

        if not itm_item:
            return {"found": False, "item_id": item_id, "message": "商品不存在"}

        t4 = time.perf_counter()
        price_map = get_item_prices(session, [item_id], org_id)
        price = price_map.get(item_id)
        t5 = time.perf_counter()
        app_logger.info(
            f"[scan_item] 查询价格: item_id={item_id}, found={price is not None}, 耗时={((t5 - t4) * 1000):.1f}ms")

        app_logger.info(f"[scan_item] 总耗时: {((t5 - t0) * 1000):.1f}ms")
        return {
            "found": True,
            "item_id": item_id,
            "upc": upc,
            "name": itm_item.name,
            "color": itm_item.dimension1,
            "price": price,
        }
    except Exception as e:
        app_logger.error(f"Error scanning item: {str(e)}")
        raise


async def get_task_list(
        session: Session,
        org_id: str,
        key_word: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 30,
        lang: str = "en",
) -> Dict[str, Any]:
    try:
        query = session.query(
            InventoryTask,
            LocRtlLoc.store_name,
        ).join(
            LocRtlLoc,
            InventoryTask.store_code == cast(LocRtlLoc.rtl_loc_id, String)
        )
        if org_id:
            query = query.filter(InventoryTask.org_id == org_id)
        if key_word:
            query = query.filter(
                (InventoryTask.batch_no.like(f"%{key_word}%")) |
                (InventoryTask.store_code.like(f"%{key_word}%")) |
                (InventoryTask.create_user.like(f"%{key_word}%"))
            )
        if start_date:
            query = query.filter(InventoryTask.inventory_date >= start_date)
        if end_date:
            query = query.filter(InventoryTask.inventory_date <= end_date)
        if status:
            query = query.filter(InventoryTask.status == status)

        app_logger.info(f"[get_task_list] 生成的SQL: {query.statement}")

        total = query.count()
        rows = query.order_by(InventoryTask.create_time.desc()) \
            .offset((page - 1) * page_size).limit(page_size).all()
        app_logger.info(f"[get_task_list] 查询任务列表: total={total}, page={page}, page_size={page_size}")

        tasks = [r[0] for r in rows]
        store_name_map = {r[0].task_id: r[1] for r in rows}
        task_ids = [t.task_id for t in tasks]

        order_agg_query = (session.query(
            InventoryOrder.task_id,
            InventoryOrder.order_no,
            InventoryOrder.store_code,
            InventoryOrder.zone_code,
            InventoryOrder.status,
            InventoryOrder.is_red,
            InventoryOrder.create_user,
            InventoryOrder.create_time,
            InventoryOrder.confirm_user,
            InventoryOrder.confirm_time,
            func.count(InventoryItem.item_id).label("item_count"),
            func.coalesce(func.sum(InventoryItem.inventory_qty), 0).label("inventory_qty"),
            func.coalesce(func.sum(InventoryItem.inventory_qty * InventoryItem.price), 0).label("total_amount"),
        ).outerjoin(
            InventoryItem, InventoryOrder.order_no == InventoryItem.order_no
        ).filter(
            InventoryOrder.task_id.in_(task_ids)
        ).group_by(
            InventoryOrder.task_id,
            InventoryOrder.order_no,
            InventoryOrder.store_code,
            InventoryOrder.zone_code,
            InventoryOrder.status,
            InventoryOrder.is_red,
            InventoryOrder.create_user,
            InventoryOrder.create_time,
            InventoryOrder.confirm_user,
            InventoryOrder.confirm_time,
        ).order_by(InventoryOrder.create_time.desc()).all())
        app_logger.info(f"[get_task_list] 查询订单汇总: total={len(order_agg_query)}")
        task_orders = {}
        task_summary = {}
        for row in order_agg_query:
            tid = row.task_id
            if tid not in task_orders:
                task_orders[tid] = []
            if tid not in task_summary:
                task_summary[tid] = {"total_item_count": 0, "total_inventory_qty": 0, "total_amount": 0}

            order_data = {
                "order_no": row.order_no,
                "task_id": row.task_id,
                "store_code": row.store_code,
                "zone_code": row.zone_code,
                # "inventory_date": str(row.inventory_date) if row.inventory_date else None,
                "status": row.status,
                "is_red": row.is_red,
                # "data_source": row.data_source,
                "create_user": row.create_user,
                "create_time": str(row.create_time) if row.create_time else None,
                "confirm_user": row.confirm_user,
                "confirm_time": str(row.confirm_time) if row.confirm_time else None,
                # "update_time": str(row.update_time) if row.update_time else None,
                # "update_user": row.update_user,
                # "remark": row.remark,
                "item_count": row.item_count or 0,
                "inventory_qty": row.inventory_qty or 0,
                "total_amount": row.total_amount or 0,
            }
            task_orders[tid].append(order_data)
            task_summary[tid]["total_item_count"] += (row.item_count or 0)
            task_summary[tid]["total_inventory_qty"] += (row.inventory_qty or 0)
            task_summary[tid]["total_amount"] += (row.total_amount or 0)

        data = []
        for task in tasks:
            task_dict = {
                "task_id": task.task_id,
                "batch_no": task.batch_no,
                "org_id": task.org_id,
                "store_code": task.store_code,
                "store_name": store_name_map.get(task.task_id),
                "inventory_date": str(task.inventory_date) if task.inventory_date else None,
                "inventory_method": translate_value("inventory_method", task.inventory_method, lang),
                "inventory_type": translate_value("inventory_type", task.inventory_type, lang),
                "status": task.status,
                "difference_date": str(task.difference_date) if task.difference_date else None,
                "frozen_qty": task.frozen_qty,
                "Inventory_qty": task.Inventory_qty,
                "difference_qty": task.difference_qty,
                "export_count": task.export_count,
                "confirm_time": str(task.confirm_time) if task.confirm_time else None,
                "confirm_user": task.confirm_user,
                "remark": task.remark,
                "create_time": str(task.create_time) if task.create_time else None,
                "create_user": task.create_user,
                "update_time": str(task.update_time) if task.update_time else None,
                "update_user": task.update_user,
                "order_count": len(task_orders.get(task.task_id, [])),
                "total_item_count": task_summary.get(task.task_id, {}).get("total_item_count", 0),
                "total_inventory_qty": task_summary.get(task.task_id, {}).get("total_inventory_qty", 0),
                "total_amount": task_summary.get(task.task_id, {}).get("total_amount", 0),
                "orders": task_orders.get(task.task_id, []),
            }
            data.append(task_dict)
        app_logger.info(f"[get_task_list] 查询任务列表: total={total}, page={page}, page_size={page_size}, data={data}")
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data
        }
    except Exception as e:
        app_logger.error(f"Error getting task list: {str(e)}")
        raise


async def get_inventory_task_list(
        session: Session,
        org_id: str,
        key_word: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 30,
        lang: str = "en"
) -> Dict[str, Any]:
    try:

        app_logger.info(
            f"[get_inventory_task_list] 入参: org_id={org_id}, key_word={key_word}, "
            f"start_date={start_date}, end_date={end_date}, status={status}, "
            f"page={page}, page_size={page_size}"
        )
        allowed_statuses = ["submitted", "confirmed", "completed"]
        query = session.query(
            InventoryTask,
            LocRtlLoc.store_name,
        ).join(
            LocRtlLoc,
            InventoryTask.store_code == cast(LocRtlLoc.rtl_loc_id, String)
        )
        if org_id:
            query = query.filter(InventoryTask.org_id == org_id)
        if key_word:
            query = query.filter(
                (InventoryTask.batch_no.like(f"%{key_word}%")) |
                (InventoryTask.store_code.like(f"%{key_word}%")) |
                (InventoryTask.create_user.like(f"%{key_word}%"))
            )
        if start_date:
            query = query.filter(InventoryTask.create_date >= start_date)
        if end_date:
            query = query.filter(InventoryTask.create_date <= end_date)
        if status:
            query = query.filter(InventoryTask.status == status)
        else:
            query = query.filter(InventoryTask.status.in_(allowed_statuses))

        app_logger.info(f"[get_inventory_task_list] 生成的SQL: {query.statement}")

        total = query.count()
        rows = query.order_by(InventoryTask.create_time.desc()) \
            .offset((page - 1) * page_size).limit(page_size).all()

        data = []
        for r in rows:
            task = r[0]

            def _fmt_dt(v):
                if v is None:
                    return None
                if hasattr(v, 'strftime'):
                    if hasattr(v, 'hour'):
                        return v.strftime('%Y-%m-%d %H:%M')
                    return v.strftime('%Y-%m-%d')
                return str(v)

            data.append({
                "task_id": task.task_id,
                "batch_no": task.batch_no,
                "org_id": task.org_id,
                "store_code": task.store_code,
                "store_name": r[1],
                "inventory_date": str(task.inventory_date) if task.inventory_date else None,
                "inventory_method": translate_value("inventory_method", task.inventory_method, lang),
                "inventory_type": translate_value("inventory_type", task.inventory_type, lang),
                "status": task.status,
                "difference_date": str(task.difference_date) if task.difference_date else None,
                "frozen_qty": task.frozen_qty,
                "Inventory_qty": task.Inventory_qty,
                "difference_qty": task.difference_qty,
                "export_count": task.export_count,
                "confirm_time": _fmt_dt(task.confirm_time),
                "confirm_user": task.confirm_user,
                "business_owner": task.business_owner,
                "remark": task.remark,
                "create_time": _fmt_dt(task.create_time),
                "create_user": task.create_user,
                "update_time": _fmt_dt(task.update_time),
                "update_user": task.update_user,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data,
            "columns": get_table_columns("inventory_task_list", lang)
        }
    except Exception as e:
        app_logger.error(f"Error getting inventory task list: {str(e)}")
        raise


async def get_inventory_task_by_id(
        session: Session,
        task_id: int
) -> Optional[InventoryTask]:
    try:
        return session.query(InventoryTask).filter(
            InventoryTask.task_id == task_id
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting inventory task by id: {str(e)}")
        raise


async def create_inventory_task(
        session: Session,
        task_data: Dict[str, Any]
) -> InventoryTask:
    try:
        store_code = task_data["store_code"]
        inventory_date = task_data.get("inventory_date")

        existing_task = session.query(InventoryTask).filter(
            InventoryTask.store_code == store_code,
            InventoryTask.inventory_date == inventory_date,
            InventoryTask.status != 'cancelled'
        ).first()

        if existing_task:
            raise ValueError(f"inventory_task_already_exists")

        task_id = generate_inventory_id(session, 'task')
        new_task = InventoryTask(
            task_id=task_id,
            batch_no=task_id,
            org_id=task_data["org_id"],
            store_code=store_code,
            inventory_date=inventory_date,
            inventory_method=task_data.get("inventory_method", "店舗棚卸"),
            inventory_type=task_data.get("inventory_type", "full"),
            status=task_data.get("status", "in_progress"),
            remark=task_data.get("remark"),
            create_user=task_data["create_user"],
            create_time=datetime.now(),
        )
        session.add(new_task)
        session.commit()
        session.refresh(new_task)
        app_logger.info(f"Created inventory task: task_id={new_task.task_id}, batch_no={new_task.batch_no}")
        return new_task
    except ValueError:
        raise
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating inventory task: {str(e)}")
        raise


async def confirm_inventory_detail(
        session: Session,
        task_id: int
) -> Dict[str, Any]:
    try:
        func_start = time.time()
        now = datetime.now()
        app_logger.info(f"[confirm_inventory_detail] 开始, task_id={task_id}")

        step_start = time.time()
        session.query(InventoryDetail).filter(
            InventoryDetail.task_id == task_id
        ).delete()
        app_logger.info(f"[confirm_inventory_detail] 清除旧明细, 耗时={time.time() - step_start:.3f}s")

        step_start = time.time()
        snapshot_rows = session.query(
            InventorySnapshotDetail.item_id,
            func.min(InventorySnapshotDetail.upc).label("upc"),
            func.min(InventorySnapshotDetail.batch).label("batch"),
            func.min(InventorySnapshotDetail.material).label("material"),
            func.min(InventorySnapshotDetail.grid).label("grid"),
            func.min(InventorySnapshotDetail.price).label("price"),
            func.sum(InventorySnapshotDetail.frozen_qty).label("frozen_qty"),
        ).filter(
            InventorySnapshotDetail.task_id == task_id
        ).group_by(
            InventorySnapshotDetail.item_id,
        ).all()

        snapshot_map = {}
        for row in snapshot_rows:
            snapshot_map[row.item_id] = {
                "upc": str(row.upc or "").strip(),
                "material": str(row.material or "").strip(),
                "grid": str(row.grid or "").strip(),
                "batch": str(row.batch or "").strip(),
                "price": float(row.price or 0),
                "frozen_qty": int(row.frozen_qty or 0),
            }
        app_logger.info(
            f"[confirm_inventory_detail] 快照汇总: {len(snapshot_map)}个SKU, "
            f"耗时={time.time() - step_start:.3f}s")

        step_start = time.time()

        options_subq = session.query(
            ItmItemOptions.item_id,
            func.min(ItmItemOptions.part_number).label("part_number")
        ).group_by(ItmItemOptions.item_id).subquery(name="options_subq")

        material_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number, 1,
                            func.charindex('-', options_subq.c.part_number) - 1)),
            else_=''
        ).label('material')
        grid_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number,
                            func.charindex('-', options_subq.c.part_number) + 1,
                            func.len(options_subq.c.part_number))),
            else_=''
        ).label('grid')

        item_rows = session.query(
            InventoryItem.item_id,
            func.min(InventoryItem.upc).label("upc"),
            func.min(InventoryItem.price).label("price"),
            func.sum(InventoryItem.inventory_qty).label("inventory_qty"),
            material_expr,
            grid_expr,
        ).join(
            InventoryOrder, InventoryItem.order_no == InventoryOrder.order_no
        ).outerjoin(
            options_subq,
            text("inventory_item.item_id = options_subq.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).filter(
            InventoryOrder.task_id == task_id
        ).group_by(
            InventoryItem.item_id,
            options_subq.c.part_number,
        ).all()

        item_map = {}
        for row in item_rows:
            item_map[row.item_id] = {
                "upc": str(row.upc or "").strip(),
                "price": float(row.price or 0),
                "inventory_qty": int(row.inventory_qty or 0),
                "material": str(row.material or "").strip(),
                "grid": str(row.grid or "").strip(),
            }
        app_logger.info(
            f"[confirm_inventory_detail] 盘点汇总: {len(item_map)}个SKU, "
            f"耗时={time.time() - step_start:.3f}s")

        step_start = time.time()
        all_item_ids = set(snapshot_map.keys()) | set(item_map.keys())
        new_details = []
        total_frozen_qty = 0
        total_inventory_qty = 0
        total_difference_qty = 0

        for item_id in all_item_ids:
            snap = snapshot_map.get(item_id)
            inv = item_map.get(item_id)

            if snap:
                upc = snap["upc"]
                material = snap["material"]
                grid = snap["grid"]
                batch = snap["batch"]
                price = snap["price"]
                frozen_qty = snap["frozen_qty"]
            else:
                upc = inv["upc"] if inv else ""
                material = inv["material"] if inv else ""
                grid = inv["grid"] if inv else ""
                batch=""
                price = inv["price"] if inv else 0
                frozen_qty = 0

            inventory_qty = inv["inventory_qty"] if inv else 0
            difference_qty = frozen_qty - inventory_qty
            total_frozen_qty += frozen_qty
            total_inventory_qty += inventory_qty
            total_difference_qty += difference_qty
            difference_amt = round(difference_qty * price, 2)

            new_details.append(InventoryDetail(
                task_id=task_id,
                item_id=item_id,
                upc=upc,
                material=material,
                grid=grid,
                batch=batch,
                price=price,
                frozen_qty=frozen_qty,
                Inventory_qty=inventory_qty,
                difference_qty=difference_qty,
                difference_amt=difference_amt,
                create_time=now,
                update_time=now,
            ))

        app_logger.info(
            f"[confirm_inventory_detail] 总汇总: "
            f"总冻 qty={total_frozen_qty}, 总库存 qty={total_inventory_qty}, 总差异 qty={total_difference_qty}, "
            f"耗时={time.time() - step_start:.3f}s")
        app_logger.info(
            f"[confirm_inventory_detail] 组装明细: {len(new_details)}条, "
            f"耗时={time.time() - step_start:.3f}s")

        step_start = time.time()
        if new_details:
            session.bulk_save_objects(new_details)
        app_logger.info(
            f"[confirm_inventory_detail] 批量写入: {len(new_details)}条, "
            f"耗时={time.time() - step_start:.3f}s")

        total_elapsed = time.time() - func_start
        app_logger.info(
            f"[confirm_inventory_detail] 完成: task_id={task_id}, "
            f"快照SKU={len(snapshot_map)}, 盘点SKU={len(item_map)}, "
            f"汇总明细={len(new_details)}, 总耗时={total_elapsed:.3f}s")

        return {
            "task_id": task_id,
            "snapshot_sku_count": len(snapshot_map),
            "inventory_sku_count": len(item_map),
            "detail_count": len(new_details),
            "frozen_qty": total_frozen_qty,
            "Inventory_qty": total_inventory_qty,
            "difference_qty": total_difference_qty,
        }

    except Exception as e:
        app_logger.error(f"[confirm_inventory_detail] 失败: task_id={task_id}, error={str(e)}")
        raise


async def update_inventory_task(
        session: Session,
        task_id: int,
        update_data: Dict[str, Any],
        user_id: str
) -> Optional[InventoryTask]:
    try:
        task = session.query(InventoryTask).filter(
            InventoryTask.task_id == task_id
        ).first()
        if not task:
            return None

        updatable_fields = [
            "inventory_date", "inventory_method", "inventory_type", "status",
            "difference_date", "export_count", "confirm_user", "confirm_time",
            "operator", "remark",
        ]
        for field in updatable_fields:
            if field in update_data:
                setattr(task, field, update_data[field])

        if "status" in update_data:
            new_status = update_data["status"]
            session.query(InventoryOrder).filter(
                InventoryOrder.task_id == task_id
            ).update({"status": new_status})
            app_logger.info(f"[update_inventory_task] 同步更新order状态: task_id={task_id}, status={new_status}")

        if update_data.get("status") in ["submitted", "confirmed"]:
            task.confirm_time = datetime.now()
            if user_id:
                task.confirm_user = user_id

            confirm_result = await confirm_inventory_detail(session, task_id)
            task.frozen_qty = confirm_result["frozen_qty"]
            task.Inventory_qty = confirm_result["Inventory_qty"]
            task.difference_qty = confirm_result["difference_qty"]
            app_logger.info(f"[update_inventory_task] 盘点确认汇总完成: {confirm_result}")

        task.update_time = datetime.now()
        session.commit()
        session.refresh(task)
        app_logger.info(f"Updated inventory task: task_id={task_id}")
        return task
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error updating inventory task: {str(e)}")
        raise


# ==================== InventoryOrder 服务 ====================

async def get_inventory_order_list(
        session: Session,
        org_id: str,
        key_word: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        status: Optional[str] = None,
        page: int = 1,
        page_size: int = 30,
        lang: str = "en"
) -> Dict[str, Any]:
    try:
        query = session.query(
            InventoryTask.task_id,
            InventoryTask.batch_no,
            InventoryTask.org_id,
            InventoryTask.store_code,
            InventoryTask.inventory_date,
            InventoryTask.status,
            InventoryTask.create_user,
            InventoryTask.create_time,
            LocRtlLoc.store_name,
            func.count(func.distinct(InventoryOrder.order_no)).label("order_count"),
            func.count(InventoryItem.item_id).label("item_count"),
            func.coalesce(func.sum(InventoryItem.inventory_qty), 0).label("inventory_qty"),
        ).outerjoin(
            InventoryOrder,
            InventoryTask.task_id == InventoryOrder.task_id
        ).outerjoin(
            InventoryItem,
            InventoryOrder.order_no == InventoryItem.order_no
        ).join(
            LocRtlLoc,
            InventoryTask.store_code == cast(LocRtlLoc.rtl_loc_id, String)
        ).group_by(
            InventoryTask.task_id,
            InventoryTask.batch_no,
            InventoryTask.org_id,
            InventoryTask.store_code,
            InventoryTask.inventory_date,
            InventoryTask.status,
            InventoryTask.create_user,
            InventoryTask.create_time,
            LocRtlLoc.store_name,
        )

        if org_id:
            query = query.filter(InventoryTask.org_id == org_id)
        if key_word:
            query = query.filter(
                (InventoryTask.batch_no.like(f"%{key_word}%")) |
                (InventoryTask.store_code.like(f"%{key_word}%")) |
                (InventoryTask.create_user.like(f"%{key_word}%"))
            )
        if start_date:
            query = query.filter(InventoryTask.inventory_date >= start_date)
        if end_date:
            query = query.filter(InventoryTask.inventory_date <= end_date)
        if status:
            query = query.filter(InventoryTask.status == status)

        total = query.count()
        rows = query.order_by(InventoryTask.create_time.desc()) \
            .offset((page - 1) * page_size).limit(page_size).all()

        data = []
        for row in rows:
            data.append({
                "task_id": row.task_id,
                "batch_no": row.batch_no,
                "org_id": row.org_id,
                "store_code": row.store_code,
                "store_name": row.store_name,
                "inventory_date": str(row.inventory_date) if row.inventory_date else None,
                "status": row.status,
                "create_user": row.create_user,
                "create_time": str(row.create_time) if row.create_time else None,
                "order_count": row.order_count or 0,
                "item_count": row.item_count or 0,
                "inventory_qty": row.inventory_qty or 0,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data,
            "columns": get_table_columns("inventory_order_list", lang)
        }
    except Exception as e:
        app_logger.error(f"Error getting inventory order list: {str(e)}")
        raise


async def get_inventory_order_by_no(
        session: Session,
        order_no: str
) -> Optional[InventoryOrder]:
    try:
        return session.query(InventoryOrder).filter(
            InventoryOrder.order_no == order_no
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting inventory order by no: {str(e)}")
        raise


async def create_inventory_order(
        session: Session,
        order_data: Dict[str, Any]
) -> InventoryOrder:
    try:
        app_logger.info(
            f"[create_order] 开始创建: order_no={order_data.get('order_no')}, task_id={order_data.get('task_id')}, store_code={order_data.get('store_code')}")

        new_order = InventoryOrder(
            order_no=order_data["order_no"],
            task_id=order_data["task_id"],
            org_id=order_data.get("org_id"),
            store_code=order_data["store_code"],
            zone_code=order_data.get("zone_code"),
            inventory_date=order_data["inventory_date"],
            status="in_progress",
            is_red=order_data.get("is_red", False),
            red_dimension=order_data.get("red_dimension"),
            data_source=order_data.get("data_source", "manual"),
            import_file_name=order_data.get("import_file_name"),
            create_user=order_data["create_user"],
            confirm_user=order_data.get("confirm_user"),
            remark=order_data.get("remark"),
            create_time=datetime.now(),
        )
        session.add(new_order)
        session.flush()
        app_logger.info(f"[create_order] Order已创建: order_no={new_order.order_no}")

        items_data = order_data.get("items")
        if items_data:
            app_logger.info(f"[create_order] 收到items数={len(items_data)}")
            merged = {}
            for item in items_data:
                item_id = item["item_id"] if isinstance(item, dict) else item.item_id
                qty = item.get("inventory_qty", 1) if isinstance(item, dict) else item.inventory_qty
                price = item.get("price", 0) if isinstance(item, dict) else getattr(item, "price", 0)
                upc = item.get("upc", "") if isinstance(item, dict) else getattr(item, "upc", "")
                if item_id in merged:
                    merged[item_id]["qty"] += qty
                else:
                    merged[item_id] = {"qty": qty, "price": price, "upc": upc}

            created_count = 0
            updated_count = 0
            for item_id, info in merged.items():
                existing = session.query(InventoryItem).filter(
                    InventoryItem.order_no == new_order.order_no,
                    InventoryItem.item_id == item_id
                ).first()

                if existing:
                    existing.inventory_qty += info["qty"]
                    existing.price = info["price"]
                    existing.upc = info["upc"]
                    existing.update_time = datetime.now()
                    updated_count += 1
                else:
                    new_item = InventoryItem(
                        order_no=new_order.order_no,
                        item_id=item_id,
                        inventory_qty=info["qty"],
                        price=info["price"],
                        upc=info["upc"],
                        create_time=datetime.now(),
                    )
                    session.add(new_item)
                    created_count += 1

            app_logger.info(
                f"[create_order] items处理完成: order_no={new_order.order_no}, "
                f"去重后={len(merged)}条, 新建={created_count}条, 累加更新={updated_count}条")

        session.commit()
        session.refresh(new_order)
        app_logger.info(f"[create_order] 完成: order_no={new_order.order_no}, task_id={new_order.task_id}")
        return new_order
    except Exception as e:
        session.rollback()
        app_logger.error(f"[create_order] 失败: order_no={order_data.get('order_no')}, error={str(e)}")
        raise


async def update_inventory_order(
        session: Session,
        order_no: str,
        update_data: Dict[str, Any]
) -> Optional[InventoryOrder]:
    try:
        order = session.query(InventoryOrder).filter(
            InventoryOrder.order_no == order_no
        ).first()
        if not order:
            return None

        updatable_fields = [
            "zone_code", "inventory_date", "status", "is_red", "red_dimension",
            "data_source", "import_file_name", "confirm_user", "confirm_time", "remark",
        ]
        for field in updatable_fields:
            if field in update_data:
                setattr(order, field, update_data[field])

        session.commit()
        session.refresh(order)
        app_logger.info(f"Updated inventory order: order_no={order_no}")
        return order
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error updating inventory order: {str(e)}")
        raise


async def delete_inventory_order(
        session: Session,
        order_no: str
) -> bool:
    try:
        order = session.query(InventoryOrder).filter(
            InventoryOrder.order_no == order_no
        ).first()
        if not order:
            return False

        session.query(InventoryItem).filter(
            InventoryItem.order_no == order_no
        ).delete()

        session.delete(order)
        session.commit()
        app_logger.info(f"Deleted inventory order: order_no={order_no}")
        return True
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting inventory order: {str(e)}")
        raise


# ==================== InventoryItem 服务 ====================

async def export_inventory_difference_detail(
        session: Session,
        task_ids: List[int],
        lang: str = "en"
) -> bytes:
    try:
        import pandas as pd
        import io

        tasks = session.query(InventoryTask).filter(InventoryTask.task_id.in_(task_ids)).all()
        if not tasks:
            raise ValueError(f"task_ids={task_ids} 均不存在")

        task_map = {t.task_id: t for t in tasks}

        store_codes = [t.store_code for t in tasks]
        store_name_rows = session.query(
            LocRtlLoc.rtl_loc_id, LocRtlLoc.store_name
        ).filter(
            cast(LocRtlLoc.rtl_loc_id, String).in_(store_codes)
        ).all()
        store_name_map = {str(r.rtl_loc_id).strip(): r.store_name for r in store_name_rows}

        options_subq = session.query(
            ItmItemOptions.item_id,
            func.min(ItmItemOptions.part_number).label("part_number")
        ).group_by(ItmItemOptions.item_id).subquery(name="options_subq")

        material_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number, 1,
                            func.charindex('-', options_subq.c.part_number) - 1)),
            else_=''
        ).label('material')
        grid_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number,
                            func.charindex('-', options_subq.c.part_number) + 1,
                            func.len(options_subq.c.part_number))),
            else_=''
        ).label('grid')

        query = session.query(
            InventoryDetail.task_id,
            InventoryDetail.item_id,
            material_expr,
            grid_expr,
            InventoryDetail.frozen_qty,
            InventoryDetail.Inventory_qty,
            InventoryDetail.difference_qty,
            ItmItem.name,
        ).outerjoin(
            ItmItem,
            text("inventory_detail.item_id = ITM_ITEM.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).outerjoin(
            options_subq,
            text("inventory_detail.item_id = options_subq.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).filter(
            InventoryDetail.task_id.in_(task_ids)
        )

        rows = query.order_by(InventoryDetail.task_id, InventoryDetail.item_id).all()

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet("Difference")

            col_fmt = workbook.add_format({
                'bold': True,
                'bg_color': '#D9E1F2',
                'border': 1,
                'text_wrap': True,
                'valign': 'vcenter',
            })

            headers = [
                "営業担当者",
                "店舗コード",
                "店舗名",
                "店舗カウント、または第三者ベンダー",
                "SKU",
                "Material",
                "Grid",
                "シリーズ名",
                "Material詳細",
                "フローズン在庫数",
                "実棚カウント数",
                "差異数",
            ]
            for col_num, header in enumerate(headers):
                worksheet.write(0, col_num, header, col_fmt)

            worksheet.set_row(0, 30)

            for row_num, row in enumerate(rows):
                task = task_map.get(row.task_id)
                if not task:
                    continue
                r = row_num + 1
                worksheet.write(r, 0, task.business_owner or "")
                worksheet.write(r, 1, task.store_code)
                worksheet.write(r, 2, store_name_map.get(task.store_code, ""))
                worksheet.write(r, 3, translate_value("inventory_method", task.inventory_method, lang))
                worksheet.write(r, 4, row.item_id)
                worksheet.write(r, 5, row.material or "")
                worksheet.write(r, 6, row.grid or "")
                worksheet.write(r, 7, row.name or "")
                worksheet.write(r, 8, row.description or "")
                worksheet.write(r, 9, row.frozen_qty or 0)
                worksheet.write(r, 10, row.Inventory_qty or 0)
                worksheet.write(r, 11, row.difference_qty or 0)

            # worksheet.set_column('A:A', 10)
            # worksheet.set_column('B:E', 15)
            # worksheet.set_column('F:H', 15)
            # worksheet.set_column('I:I', 25)
            # worksheet.set_column('J:L', 12)

        output.seek(0)
        return output.getvalue()

    except ValueError:
        raise
    except Exception as e:
        app_logger.error(f"Error exporting inventory difference detail: {str(e)}")
        raise


async def get_inventory_item_list(
        session: Session,
        order_no: Optional[str] = None,
        key_word: Optional[str] = None,
        page: int = 1,
        page_size: int = 30,
        lang: str = "en"
) -> Dict[str, Any]:
    try:

        options_subq = session.query(
            ItmItemOptions.item_id,
            func.min(ItmItemOptions.part_number).label("part_number")
        ).group_by(ItmItemOptions.item_id).subquery(name="options_subq")

        material_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number, 1,
                            func.charindex('-', options_subq.c.part_number) - 1)),
            else_=''
        ).label('material')
        grid_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number,
                            func.charindex('-', options_subq.c.part_number) + 1,
                            func.len(options_subq.c.part_number))),
            else_=''
        ).label('grid')

        query = session.query(
            InventoryItem.order_no,
            InventoryItem.item_id,
            InventoryItem.upc,
            InventoryItem.price,
            InventoryItem.inventory_qty,
            InventoryItem.create_time,
            InventoryItem.update_time,
            ItmItem.name,
            ItmItem.description,
            ItmItem.dimension1,
            material_expr,
            grid_expr
        ).select_from(InventoryItem).outerjoin(
            ItmItem,
            text("inventory_item.item_id = ITM_ITEM.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).outerjoin(
            options_subq,
            text("inventory_item.item_id = options_subq.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        )

        # if org_id:
        #     query = query.filter(InventoryItem.org_id == org_id)

        if order_no:
            query = query.filter(InventoryItem.order_no == order_no)
        if key_word:
            query = query.filter(
                (InventoryItem.item_id.like(f"%{key_word}%")) |
                (InventoryItem.upc.like(f"%{key_word}%"))
            )
        total = query.count()
        rows = query.order_by(InventoryItem.item_id) \
            .offset((page - 1) * page_size).limit(page_size).all()

        data = []
        for row in rows:
            qty = row.inventory_qty or 0
            price = float(row.price or 0)
            data.append({
                "order_no": row.order_no,
                "item_id": row.item_id,
                "upc": row.upc,
                "price": price,
                "inventory_qty": qty,
                "amount": round(qty * price, 2),
                "name": row.name,
                "description": row.description,
                "color": row.dimension1,
                "material": row.material,
                "grid": row.grid,
                "create_time": str(row.create_time) if row.create_time else None,
                "update_time": str(row.update_time) if row.update_time else None,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data,
            "columns": get_table_columns("inventory_item_list", lang)
        }
    except Exception as e:
        app_logger.error(f"Error getting inventory item list: {str(e)}")
        raise


async def export_inventory_difference_detail(
        session: Session,
        task_ids: List[int],
        lang: str = "en"
) -> bytes:
    try:
        import pandas as pd
        import io

        t_start = time.perf_counter()
        app_logger.info(f"[export_difference_detail] 开始导出, task_ids={task_ids}, lang={lang}")

        t0 = time.perf_counter()
        tasks = session.query(InventoryTask).filter(InventoryTask.task_id.in_(task_ids)).all()
        t1 = time.perf_counter()
        app_logger.info(
            f"[export_difference_detail] 查询InventoryTask: 找到{len(tasks)}条, 耗时={((t1 - t0) * 1000):.1f}ms")
        if not tasks:
            raise ValueError(f"task_ids={task_ids} 均不存在")

        task_map = {t.task_id: t for t in tasks}

        store_codes = [t.store_code for t in tasks]
        t0 = time.perf_counter()
        store_name_rows = session.query(
            LocRtlLoc.rtl_loc_id, LocRtlLoc.store_name
        ).filter(
            cast(LocRtlLoc.rtl_loc_id, String).in_(store_codes)
        ).all()
        t1 = time.perf_counter()
        store_name_map = {str(r.rtl_loc_id).strip(): r.store_name for r in store_name_rows}
        app_logger.info(
            f"[export_difference_detail] 查询store_name: 匹配{len(store_name_map)}/{len(store_codes)}个门店, 耗时={((t1 - t0) * 1000):.1f}ms")

        t0 = time.perf_counter()
        query = session.query(
            InventoryDetail.task_id,
            InventoryDetail.item_id,
            InventoryDetail.material,
            InventoryDetail.grid,
            InventoryDetail.frozen_qty,
            InventoryDetail.Inventory_qty,
            InventoryDetail.difference_qty,
            ItmItem.name,
            ItmItem.description,
        ).outerjoin(
            ItmItem,
            text("inventory_detail.item_id = ITM_ITEM.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).filter(
            InventoryDetail.task_id.in_(task_ids)
        )

        rows = query.order_by(InventoryDetail.task_id, InventoryDetail.item_id).all()
        t1 = time.perf_counter()
        app_logger.info(
            f"[export_difference_detail] 查询InventoryDetail+ItmItem: 共{len(rows)}行, 耗时={((t1 - t0) * 1000):.1f}ms")

        t0 = time.perf_counter()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet("Difference")

            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})
            col_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})

            columns = ["営業担当者", "店舗コード", "店舗名", "店舗カウント、または第三者ベンダー",
                       "SKU", "Material", "Grid", "シリーズ名", "Material詳細",
                       "フローズン在庫数", "実棚カウント数", "差異数"]
            for col_num, col_name in enumerate(columns):
                worksheet.write(0, col_num, col_name, col_fmt)

            for row_num, row in enumerate(rows):
                task = task_map.get(row.task_id)
                if not task:
                    continue
                worksheet.write(row_num + 1, 0, task.business_owner or "")
                worksheet.write(row_num + 1, 1, task.store_code)
                worksheet.write(row_num + 1, 2, store_name_map.get(task.store_code, ""))
                worksheet.write(row_num + 1, 3, task.inventory_method)
                worksheet.write(row_num + 1, 4, row.item_id)
                worksheet.write(row_num + 1, 5, row.material)
                worksheet.write(row_num + 1, 6, row.grid)
                worksheet.write(row_num + 1, 7, row.name or "")
                worksheet.write(row_num + 1, 8, row.description or "")
                worksheet.write(row_num + 1, 9, row.frozen_qty or 0)
                worksheet.write(row_num + 1, 10, row.Inventory_qty or 0)
                worksheet.write(row_num + 1, 11, row.difference_qty or 0)

            worksheet.set_column('A:A', 15)
            worksheet.set_column('B:D', 15)
            worksheet.set_column('E:G', 15)
            worksheet.set_column('H:I', 25)
            worksheet.set_column('J:L', 12)

        output.seek(0)
        t1 = time.perf_counter()
        app_logger.info(
            f"[export_difference_detail] Excel生成完成, 大小={len(output.getvalue())}字节, 耗时={((t1 - t0) * 1000):.1f}ms")

        t_end = time.perf_counter()
        app_logger.info(f"[export_difference_detail] 导出完成, 总耗时={((t_end - t_start) * 1000):.1f}ms")
        return output.getvalue()

    except ValueError:
        raise
    except Exception as e:
        app_logger.error(f"Error exporting inventory difference detail: {str(e)}")
        raise


async def export_inventory_sap_csv(
        session: Session,
        task_ids: List[int],
) -> bytes:
    try:
        import io

        t_start = time.perf_counter()
        app_logger.info(f"[export_sap_csv] 开始导出, task_ids={task_ids}")

        t0 = time.perf_counter()
        tasks = session.query(InventoryTask).filter(InventoryTask.task_id.in_(task_ids)).all()
        t1 = time.perf_counter()
        app_logger.info(f"[export_sap_csv] 查询InventoryTask: 找到{len(tasks)}条, 耗时={((t1 - t0) * 1000):.1f}ms")
        if not tasks:
            raise ValueError(f"task_ids={task_ids} 均不存在")

        task_map = {t.task_id: t for t in tasks}
        org_id = tasks[0].org_id
        app_logger.info(f"[export_sap_csv] 使用org_id={org_id} (来自task_id={tasks[0].task_id})")

        t0 = time.perf_counter()
        query = session.query(
            InventoryDetail.task_id,
            InventoryDetail.batch,
            InventoryDetail.material,
            InventoryDetail.grid,
            InventoryDetail.Inventory_qty,
        ).filter(
            InventoryDetail.task_id.in_(task_ids)
        )

        rows = query.order_by(InventoryDetail.task_id, InventoryDetail.item_id).all()
        t1 = time.perf_counter()
        app_logger.info(
            f"[export_sap_csv] 查询InventoryDetail+Snapshot: 共{len(rows)}行, 耗时={((t1 - t0) * 1000):.1f}ms")

        t0 = time.perf_counter()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            worksheet = workbook.add_worksheet("SAP Adjustment")

            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1})

            headers = ["Plant", "Storage Location", "Material", "Batch", "Grid",
                       "Stock Category", "Actual count", "Unit", "Type",
                       "Consignment Customer"]
            for col_num, header in enumerate(headers):
                worksheet.write(0, col_num, header, header_fmt)

            row_count = 0
            for row in rows:
                task = task_map.get(row.task_id)
                if not task:
                    continue
                worksheet.write(row_count + 1, 0, org_id)
                worksheet.write(row_count + 1, 1, "")
                worksheet.write(row_count + 1, 2, row.material)
                worksheet.write(row_count + 1, 3, row.batch or "")
                worksheet.write(row_count + 1, 4, row.grid)
                worksheet.write(row_count + 1, 5, "1CNU")
                worksheet.write(row_count + 1, 6, row.Inventory_qty or 0)
                worksheet.write(row_count + 1, 7, "PC")
                worksheet.write(row_count + 1, 8, "1")
                worksheet.write(row_count + 1, 9, task.store_code)
                row_count += 1

            worksheet.set_column('A:A', 12)
            worksheet.set_column('B:B', 18)
            worksheet.set_column('C:E', 15)
            worksheet.set_column('F:F', 14)
            worksheet.set_column('G:G', 12)
            worksheet.set_column('H:H', 8)
            worksheet.set_column('I:I', 8)
            worksheet.set_column('J:J', 18)

        output.seek(0)
        t1 = time.perf_counter()
        app_logger.info(f"[export_sap_csv] XLSX生成完成, 大小={len(output.getvalue())}字节, {row_count}行, 耗时={((t1 - t0) * 1000):.1f}ms")

        t_end = time.perf_counter()
        app_logger.info(f"[export_sap_csv] 导出完成, 总耗时={((t_end - t_start) * 1000):.1f}ms")

        return output.getvalue()

    except ValueError:
        raise
    except Exception as e:
        app_logger.error(f"Error exporting SAP CSV: {str(e)}")
        raise


async def get_inventory_item(
        session: Session,
        order_no: str,
        item_id: str
) -> Optional[InventoryItem]:
    try:
        return session.query(InventoryItem).filter(
            InventoryItem.order_no == order_no,
            InventoryItem.item_id == item_id
        ).first()
    except Exception as e:
        app_logger.error(f"Error getting inventory item: {str(e)}")
        raise


async def create_inventory_item(
        session: Session,
        item_data: Dict[str, Any]
) -> InventoryItem:
    try:
        order_no = item_data["order_no"]
        item_id = item_data["item_id"]
        incoming_qty = item_data.get("inventory_qty", 1)

        existing = session.query(InventoryItem).filter(
            InventoryItem.order_no == order_no,
            InventoryItem.item_id == item_id
        ).first()

        if existing:
            existing.inventory_qty += incoming_qty
            existing.update_time = datetime.now()
            session.commit()
            session.refresh(existing)
            app_logger.info(
                f"Accumulated inventory item: order_no={order_no}, item_id={item_id}, "
                f"added_qty={incoming_qty}, total_qty={existing.inventory_qty}"
            )
            return existing

        new_item = InventoryItem(
            order_no=order_no,
            item_id=item_id,
            inventory_qty=incoming_qty,
            create_time=datetime.now(),
        )
        session.add(new_item)
        session.commit()
        session.refresh(new_item)
        app_logger.info(f"Created inventory item: order_no={new_item.order_no}, item_id={new_item.item_id}")
        return new_item
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating inventory item: {str(e)}")
        raise


async def batch_update_inventory_items(
        session: Session,
        order_no: str,
        items_data: List[Dict[str, Any]]
) -> List[InventoryItem]:
    try:
        merged = {}
        for item in items_data:
            item_id = item["item_id"] if isinstance(item, dict) else item.item_id
            qty = item.get("inventory_qty", 1) if isinstance(item, dict) else item.inventory_qty
            price = item.get("price", 0) if isinstance(item, dict) else getattr(item, "price", 0)
            upc = item.get("upc", "") if isinstance(item, dict) else getattr(item, "upc", "")
            if item_id in merged:
                merged[item_id]["qty"] += qty
            else:
                merged[item_id] = {"qty": qty, "price": price, "upc": upc}

        existing_items = session.query(InventoryItem).filter(
            InventoryItem.order_no == order_no
        ).all()
        existing_map = {item.item_id: item for item in existing_items}

        result = []
        now = datetime.now()
        created_count = 0
        updated_count = 0

        for item_id, info in merged.items():
            if item_id in existing_map:
                existing_map[item_id].inventory_qty = info["qty"]
                existing_map[item_id].price = info["price"]
                existing_map[item_id].upc = info["upc"]
                existing_map[item_id].update_time = now
                result.append(existing_map[item_id])
                updated_count += 1
            else:
                new_item = InventoryItem(
                    order_no=order_no,
                    item_id=item_id,
                    inventory_qty=info["qty"],
                    price=info["price"],
                    upc=info["upc"],
                    create_time=now,
                )
                session.add(new_item)
                result.append(new_item)
                created_count += 1

        deleted_count = 0
        for item_id, item in existing_map.items():
            if item_id not in merged:
                session.delete(item)
                deleted_count += 1

        session.commit()
        for item in result:
            session.refresh(item)
        app_logger.info(
            f"[batch_update] 完成: order_no={order_no}, "
            f"去重后={len(merged)}条, 新建={created_count}条, 更新={updated_count}条, 删除={deleted_count}条"
        )
        return result
    except Exception as e:
        session.rollback()
        app_logger.error(f"[batch_update] 失败: order_no={order_no}, error={str(e)}")
        raise


async def delete_inventory_item(
        session: Session,
        order_no: str,
        item_id: str
) -> bool:
    try:
        item = session.query(InventoryItem).filter(
            InventoryItem.order_no == order_no,
            InventoryItem.item_id == item_id
        ).first()
        if not item:
            return False

        session.delete(item)
        session.commit()
        app_logger.info(f"Deleted inventory item: order_no={order_no}, item_id={item_id}")
        return True
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting inventory item: {str(e)}")
        raise


def validate_upload_data(session: Session, df, org_id: str, upc_col: str = "upc", store_col: str = "store_code",
                         part_number_col: str = None) -> \
        Dict[str, Any]:
    total_start = time.time()
    app_logger.info(f"[validate_upload_data] 开始校验, 数据总行数={len(df)}, upc_col={upc_col}, store_col={store_col}")

    upc_list = df[upc_col].dropna().unique().tolist() if upc_col and upc_col in df.columns else []
    upc_list = [str(u).strip() for u in upc_list if str(u).strip()]
    app_logger.info(f"[validate_upload_data] 去重后UPC数量={len(upc_list)}")

    store_list = df[store_col].dropna().unique().tolist()
    store_list = [str(s).strip() for s in store_list if str(s).strip()]
    app_logger.info(f"[validate_upload_data] 去重后门店数量={len(store_list)}")

    upc_to_item = {}
    item_id_set = set()
    if upc_list:
        upc_start = time.time()
        cross_refs = session.query(
            ItmItemCrossReference.manufacturer_upc,
            ItmItemCrossReference.item_id
        ).filter(
            ItmItemCrossReference.manufacturer_upc.in_(upc_list)
        ).all()
        upc_elapsed = time.time() - upc_start
        for ref in cross_refs:
            uid = str(ref.item_id).strip()
            upc_to_item[str(ref.manufacturer_upc).strip()] = uid
            item_id_set.add(uid)
        app_logger.info(
            f"[validate_upload_data] UPC映射: 查询{len(upc_list)}个, 命中{len(upc_to_item)}个, "
            f"未命中{len(upc_list) - len(upc_to_item)}个, 耗时{upc_elapsed:.3f}s")

    part_number_to_item = {}
    if part_number_col and part_number_col in df.columns:
        pn_list = df[part_number_col].dropna().unique().tolist()
        pn_list = [str(p).strip() for p in pn_list if str(p).strip()]
        app_logger.info(f"[validate_upload_data] 去重后PART_NUMBER数量={len(pn_list)}")
        if pn_list:
            pn_start = time.time()
            organization_id = 1
            pn_refs = session.query(
                ItmItemOptions.part_number,
                ItmItemOptions.item_id
            ).filter(
                ItmItemOptions.organization_id == organization_id,
                ItmItemOptions.part_number.in_(pn_list)
            ).all()
            pn_elapsed = time.time() - pn_start
            for ref in pn_refs:
                pn = str(ref.part_number).strip()
                uid = str(ref.item_id).strip()
                part_number_to_item[pn] = uid
                item_id_set.add(uid)
            app_logger.info(
                f"[validate_upload_data] PART_NUMBER映射: 查询{len(pn_list)}个, 命中{len(part_number_to_item)}个, "
                f"未命中{len(pn_list) - len(part_number_to_item)}个, 耗时{pn_elapsed:.3f}s")

    item_id_to_price = {}
    if item_id_set:
        price_start = time.time()
        item_id_to_price = get_item_prices(session, list(item_id_set), org_id)
        price_elapsed = time.time() - price_start
        app_logger.info(
            f"[validate_upload_data] 价格映射: 查询{len(item_id_set)}个item_id, "
            f"命中{len(item_id_to_price)}个, 耗时{price_elapsed:.3f}s")

    valid_stores = set()
    if store_list:
        store_start = time.time()
        digit_stores = [int(s) for s in store_list if s.isdigit()]
        non_digit_stores = [s for s in store_list if not s.isdigit()]
        if non_digit_stores:
            app_logger.warning(f"[validate_upload_data] 门店值非数字将被跳过: {non_digit_stores}")
        store_refs = session.query(
            LocRtlLoc.rtl_loc_id
        ).filter(
            LocRtlLoc.rtl_loc_id.in_(digit_stores)
        ).all()
        store_elapsed = time.time() - store_start
        valid_stores = {str(r.rtl_loc_id).strip() for r in store_refs}
        app_logger.info(
            f"[validate_upload_data] 门店校验: 查询{len(store_list)}个(数字{len(digit_stores)}个), "
            f"命中{len(valid_stores)}个, 未命中{len(store_list) - len(valid_stores)}个, 耗时{store_elapsed:.3f}s")

    data = []
    has_error = False
    upc_col_idx = df.columns.get_loc(upc_col) if upc_col and upc_col in df.columns else None
    store_col_idx = df.columns.get_loc(store_col)
    pn_col_idx = df.columns.get_loc(part_number_col) if part_number_col and part_number_col in df.columns else None
    columns = df.columns.tolist()

    for row_tuple in df.itertuples(index=False, name=None):
        row_list = list(row_tuple)
        upc = str(row_list[upc_col_idx]).strip() if upc_col_idx is not None else ""
        store = str(row_list[store_col_idx]).strip()
        part_number = str(row_list[pn_col_idx]).strip() if pn_col_idx is not None else ""
        errors = []
        item_id = ""
        price = 0.0

        if part_number_col and part_number:
            item_id = part_number_to_item.get(part_number, "")
            if not item_id:
                errors.append(f"Material-Grid不存在: {part_number}")
            else:
                price = item_id_to_price.get(item_id, 0.0)
        elif upc:
            item_id = upc_to_item.get(upc, "")
            if not item_id:
                errors.append(f"UPC不存在: {upc}")
            else:
                price = item_id_to_price.get(item_id, 0.0)
        if store and store not in valid_stores:
            errors.append(f"门店不存在: {store}")

        error_flag = len(errors) > 0
        if error_flag:
            has_error = True

        row_dict = {"item_id": item_id, "price": price, "error_flag": error_flag}
        if error_flag:
            row_dict["error_msg"] = "; ".join(errors)
        for i, col in enumerate(columns):
            val = row_list[i]
            row_dict[col] = "" if (isinstance(val, float) and str(val) == "nan") else val
        data.append(row_dict)

    valid = not has_error
    error_count = sum(1 for d in data if d["error_flag"])
    total_elapsed = time.time() - total_start
    app_logger.info(
        f"[validate_upload_data] 校验完成, 结果={'通过' if valid else '失败'}, "
        f"总行数={len(data)}, 错误行数={error_count}, 总耗时{total_elapsed:.3f}s")

    return {
        "valid": valid,
        "data": data,
    }


async def upload_inventory_items(
        session: Session,
        org_id: str,
        df,
        create_user: str
) -> Dict[str, Any]:
    try:

        func_start = time.time()

        column_mapping = {
            "棚番": "order_id",
            "SKU": "upc",
            "数量": "inv_qty",
            "棚卸日付": "inventory_date",
            "棚卸担当者": "operator",
            "棚卸場所": "store_code",
            "処理区分": "zone_code",
        }
        df = df.rename(columns=column_mapping)

        app_logger.info(
            f"[upload_inventory_items] 开始处理: org_id={org_id}, 总行数={len(df)}")

        step_start = time.time()
        validation = validate_upload_data(session, df, org_id, upc_col="upc", store_col="store_code")
        data = validation["data"]
        app_logger.info(f"[upload_inventory_items] 数据校验完成, 耗时{time.time() - step_start:.3f}s")
        app_logger.info(f"[upload_inventory_items] 数据预览: {data[:10]}")
        if not validation["valid"]:
            error_rows = [d for d in data if d["error_flag"]]
            app_logger.info(f"[upload_inventory_items] 校验失败, 错误行数={len(error_rows)}")
            return {
                "tasks": [],
                "has_unmatched": True,
                "error_data": error_rows,
            }

        step_start = time.time()
        now = datetime.now()

        for row_dict in data:
            row_dict["_inv_date"] = (
                datetime.strptime(str(row_dict["inventory_date"]).strip(), "%Y-%m-%d").date()
                if isinstance(row_dict["inventory_date"], str)
                else row_dict["inventory_date"]
            )

        grouped_map = {}
        for row_dict in data:
            if row_dict["error_flag"]:
                continue
            store_code = str(row_dict["store_code"]).strip()
            inv_date = row_dict["_inv_date"]
            order_id = str(row_dict["order_id"]).strip()
            key = (store_code, inv_date, order_id)
            if key not in grouped_map:
                grouped_map[key] = []
            grouped_map[key].append(row_dict)

        store_date_keys = {(k[0], k[1]) for k in grouped_map}
        app_logger.info(
            f"[upload_inventory_items] 数据分组完成, 门店日期组合={len(store_date_keys)}, "
            f"订单组合={len(grouped_map)}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        existing_tasks = session.query(InventoryTask).filter(
            InventoryTask.org_id == org_id,
            InventoryTask.store_code.in_([k[0] for k in store_date_keys]),
        ).all()
        task_map = {(t.store_code, t.inventory_date): t for t in existing_tasks}
        app_logger.info(
            f"[upload_inventory_items] 查询已有Task: 查询门店={len(store_date_keys)}, "
            f"命中={len(task_map)}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        all_order_nos = {
            f"INV{inv_date.strftime('%Y%m%d')}{store_code}{order_id}"
            for (store_code, inv_date, order_id) in grouped_map
        }

        existing_orders = {}
        if all_order_nos:
            order_results = session.query(
                InventoryOrder.order_no, InventoryOrder.task_id
            ).filter(
                InventoryOrder.order_no.in_(all_order_nos)
            ).all()
            existing_orders = {r.order_no: r.task_id for r in order_results}
        app_logger.info(
            f"[upload_inventory_items] 查询已有Order: 待查={len(all_order_nos)}, "
            f"命中={len(existing_orders)}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        new_items = []
        new_task_count = 0
        new_order_count = 0
        skipped_order_count = 0

        for (store_code, inv_date, order_id), rows in grouped_map.items():
            task_key = (store_code, inv_date)
            task = task_map.get(task_key)

            if task:
                task_id = task.task_id
            else:
                task_id = generate_inventory_id(session, 'task')
                new_task = InventoryTask(
                    task_id=task_id,
                    batch_no=str(task_id),
                    org_id=org_id,
                    store_code=store_code,
                    inventory_date=inv_date,
                    inventory_method="外部棚卸会社",
                    inventory_type="full",
                    status="in_progress",
                    create_user=create_user,
                    create_time=now,
                )
                session.add(new_task)
                session.flush()
                task_map[task_key] = new_task
                new_task_count += 1
                app_logger.info(f"[upload_inventory_items] 门店={store_code}, 日期={inv_date}, 新建task_id={task_id}")

            date_str = inv_date.strftime("%Y%m%d")
            order_no = f"INV{date_str}{store_code}{order_id}"

            if order_no in existing_orders:
                skipped_order_count += 1
            else:
                zone_code = rows[0]["zone_code"]
                new_order = InventoryOrder(
                    order_no=order_no,
                    task_id=task_id,
                    org_id=org_id,
                    store_code=store_code,
                    zone_code=zone_code,
                    inventory_date=inv_date,
                    status="pending",
                    is_red=False,
                    data_source="imported",
                    create_user=create_user,
                    create_time=now,
                )
                session.add(new_order)
                existing_orders[order_no] = task_id
                new_order_count += 1

            item_count = 0

            for row_dict in rows:
                item_id = row_dict["item_id"]
                if not item_id:
                    continue

                upc = str(row_dict["upc"]).strip()
                price = row_dict["price"]
                qty_raw = row_dict["inv_qty"]
                qty = int(float(str(qty_raw))) if str(qty_raw).strip() else 0

                new_item = InventoryItem(
                    order_no=order_no,
                    item_id=item_id,
                    upc=upc,
                    price=price,
                    inventory_qty=qty,
                    create_time=now,
                )
                new_items.append(new_item)
                item_count += 1

        app_logger.info(
            f"[upload_inventory_items] 数据组装完成: 新建Task={new_task_count}, "
            f"新建Order={new_order_count}, 跳过已有Order={skipped_order_count}, "
            f"待写入Item={len(new_items)}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        session.flush()
        if new_items:
            session.bulk_save_objects(new_items)
        session.commit()
        app_logger.info(
            f"[upload_inventory_items] 批量写入+提交完成: Item={len(new_items)}, "
            f"耗时{time.time() - step_start:.3f}s")

        store_summary = {}
        for row_dict in data:
            if row_dict["error_flag"]:
                continue
            store_code = str(row_dict["store_code"]).strip()
            inv_date = str(row_dict["_inv_date"])
            key = f"{store_code}_{inv_date}"
            if key not in store_summary:
                store_summary[key] = {"store_code": store_code, "inventory_date": inv_date, "item_count": 0}
            store_summary[key]["item_count"] += 1

        results = list(store_summary.values())

        task_id_map = {}
        for (store_code, inv_date), t in task_map.items():
            task_id_map[(store_code, str(inv_date))] = t.task_id if hasattr(t, 'task_id') else t.task_id

        for r in results:
            key = (r["store_code"], r["inventory_date"])
            r["task_id"] = task_id_map.get(key, "")

        total_elapsed = time.time() - func_start
        app_logger.info(
            f"[upload_inventory_items] 全部完成: 共处理{len(results)}个门店, "
            f"总耗时{total_elapsed:.3f}s")

        return {
            "tasks": results,
            "has_unmatched": False,
        }
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error uploading inventory items: {str(e)}")
        raise


async def upload_snapshot_details(
        session: Session,
        org_id: str,
        inventory_date,
        df,
        create_user: str
) -> Dict[str, Any]:
    try:

        func_start = time.time()

        column_mapping = {
            "Batch": "batch",
            "Special stock number": "store_code",
            "Material": "material",
            "Grid Value": "grid",
            "Stock Category": "stock_category",
            "Unrestricted": "frozen_qty",
            "Plant": "plant",
            # "Type": "type",
        }
        df = df.rename(columns=column_mapping)

        df["PART_NUMBER"] = df.apply(
            lambda row: f"{str(row.get('material', '')).strip()}-{str(row.get('grid', '')).strip()}"
            if str(row.get('material', '')).strip() and str(row.get('grid', '')).strip()
            else "",
            axis=1
        )

        app_logger.info(
            f"[upload_snapshot] 开始处理: org_id={org_id}, inventory_date={inventory_date}, 总行数={len(df)}")

        step_start = time.time()
        validation = validate_upload_data(session, df, org_id, upc_col="", store_col="store_code",
                                          part_number_col='PART_NUMBER')
        data = validation["data"]
        app_logger.info(f"[upload_snapshot] 数据校验完成, 耗时{time.time() - step_start:.3f}s")

        if not validation["valid"]:
            error_rows = [d for d in data if d["error_flag"]]
            app_logger.info(f"[upload_snapshot] 校验失败, 错误行数={len(error_rows)}")
            return {
                "tasks": [],
                "has_unmatched": True,
                "error_data": error_rows,
            }

        step_start = time.time()
        now = datetime.now()

        grouped = {}
        for row_dict in data:
            if row_dict["error_flag"]:
                continue
            store_code = str(row_dict["store_code"]).strip()
            if not store_code:
                continue
            if store_code not in grouped:
                grouped[store_code] = []
            grouped[store_code].append(row_dict)

        store_codes = list(grouped.keys())
        app_logger.info(
            f"[upload_snapshot] 数据分组完成, 门店数={len(store_codes)}, "
            f"有效行数={sum(len(v) for v in grouped.values())}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        existing_tasks = session.query(InventoryTask).filter(
            InventoryTask.org_id == org_id,
            InventoryTask.store_code.in_(store_codes),
            InventoryTask.inventory_date == inventory_date
        ).all()
        task_map = {t.store_code: t for t in existing_tasks}
        app_logger.info(
            f"[upload_snapshot] 查询已有Task: 查询门店={len(store_codes)}, "
            f"命中={len(task_map)}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        results = []
        new_task_count = 0
        reused_task_count = 0
        total_detail_count = 0
        all_new_details = []

        for store_code, rows in grouped.items():
            task = task_map.get(store_code)

            if task:
                task_id = task.task_id
                session.query(InventorySnapshotDetail).filter(
                    InventorySnapshotDetail.task_id == task_id
                ).delete()
                reused_task_count += 1
                app_logger.info(f"[upload_snapshot] 门店={store_code}, 复用已有task_id={task_id}, 清除旧明细")
            else:
                task_id = generate_inventory_id(session, 'task')
                new_task = InventoryTask(
                    task_id=task_id,
                    batch_no=str(task_id),
                    org_id=org_id,
                    store_code=store_code,
                    inventory_date=inventory_date,
                    inventory_method="外部棚卸会社",
                    inventory_type="full",
                    status="in_progress",
                    create_user=create_user,
                    create_time=now,
                )
                session.add(new_task)
                new_task_count += 1
                app_logger.info(f"[upload_snapshot] 门店={store_code}, 新建task_id={task_id}")

            detail_count = 0
            for row_dict in rows:
                item_id = row_dict["item_id"]
                if not item_id:
                    app_logger.warning(f"[upload_snapshot] 门店={store_code}, 跳过无item_id行")
                    continue

                # upc = str(row_dict["upc"]).strip()
                price = row_dict["price"]
                frozen_qty_raw = row_dict["frozen_qty"]
                frozen_qty = int(float(str(frozen_qty_raw))) if str(frozen_qty_raw).strip() else 0
                type_raw = row_dict.get("type", 1)
                # item_type = int(float(str(type_raw))) if str(type_raw).strip() else 1

                detail = InventorySnapshotDetail(
                    task_id=task_id,
                    inventory_date=inventory_date,
                    item_id=item_id,
                    # upc=upc,
                    price=price,
                    material=str(row_dict.get("material", "")).strip(),
                    grid=str(row_dict.get("grid", "")).strip(),
                    batch=str(row_dict.get("batch", "")).strip(),
                    stock_category=str(row_dict.get("stock_category", "")).strip(),
                    unit=str(row_dict.get("unit", "PC")).strip(),
                    # plant=item_type,
                    frozen_qty=frozen_qty,
                    create_time=now,
                    create_user=create_user,
                    update_time=now,
                    update_user=create_user,
                )
                all_new_details.append(detail)
                detail_count += 1

            total_detail_count += detail_count
            results.append({
                "store_code": store_code,
                "task_id": task_id,
                "detail_count": detail_count
            })
            app_logger.info(
                f"[upload_snapshot] 门店={store_code}, task_id={task_id}, 写入明细={detail_count}条")

        app_logger.info(
            f"[upload_snapshot] 数据组装完成: 新建Task={new_task_count}, 复用Task={reused_task_count}, "
            f"待写入Detail={total_detail_count}, 耗时{time.time() - step_start:.3f}s")

        step_start = time.time()
        session.flush()
        if all_new_details:
            session.bulk_save_objects(all_new_details)
        session.commit()
        app_logger.info(
            f"[upload_snapshot] 批量写入+提交完成: Detail={total_detail_count}, "
            f"耗时{time.time() - step_start:.3f}s")

        total_elapsed = time.time() - func_start
        app_logger.info(
            f"[upload_snapshot] 全部完成: 共处理{len(results)}个门店, "
            f"总明细={total_detail_count}, 总耗时{total_elapsed:.3f}s")

        return {
            "tasks": results,
            "has_unmatched": False,
        }
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error uploading inventory snapshot: {str(e)}")
        raise


# ==================== InventorySnapshotDetail 服务 ====================

async def get_snapshot_detail_list(
        session: Session,
        snapshot_id: Optional[int] = None,
        task_id: Optional[int] = None,
        item_id: Optional[str] = None,
        page: int = 1,
        page_size: int = 30
) -> Dict[str, Any]:
    try:
        query = session.query(InventorySnapshotDetail)
        if snapshot_id:
            query = query.filter(InventorySnapshotDetail.snapshot_id == snapshot_id)
        if task_id:
            query = query.filter(InventorySnapshotDetail.task_id == task_id)
        if item_id:
            query = query.filter(InventorySnapshotDetail.item_id == item_id)

        total = query.count()
        items = query.order_by(InventorySnapshotDetail.item_id) \
            .offset((page - 1) * page_size).limit(page_size).all()

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": items
        }
    except Exception as e:
        app_logger.error(f"Error getting snapshot detail list: {str(e)}")
        raise


async def create_snapshot_detail(
        session: Session,
        detail_data: Dict[str, Any]
) -> InventorySnapshotDetail:
    try:
        new_detail = InventorySnapshotDetail(
            snapshot_id=detail_data["snapshot_id"],
            item_id=detail_data["item_id"],
            task_id=detail_data["task_id"],
            material=detail_data["material"],
            grid=detail_data["grid"],
            stock_category=detail_data["stock_category"],
            frozen_qty=detail_data.get("frozen_qty", 0),
            frozen_amt=detail_data.get("frozen_amt", 0),
            create_time=datetime.now(),
        )
        session.add(new_detail)
        session.commit()
        session.refresh(new_detail)
        app_logger.info(f"Created snapshot detail: snapshot_id={new_detail.snapshot_id}, item_id={new_detail.item_id}")
        return new_detail
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating snapshot detail: {str(e)}")
        raise


async def delete_snapshot_detail(
        session: Session,
        snapshot_id: int,
        item_id: str
) -> bool:
    try:
        detail = session.query(InventorySnapshotDetail).filter(
            InventorySnapshotDetail.snapshot_id == snapshot_id,
            InventorySnapshotDetail.item_id == item_id
        ).first()
        if not detail:
            return False

        session.delete(detail)
        session.commit()
        app_logger.info(f"Deleted snapshot detail: snapshot_id={snapshot_id}, item_id={item_id}")
        return True
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error deleting snapshot detail: {str(e)}")
        raise


# ==================== InventoryDifferenceDetail 服务 ====================

async def get_difference_detail_list(
        session: Session,
        task_id: Optional[int] = None,
        key_word: Optional[str] = None,
        only_difference: bool = False,
        page: int = 1,
        page_size: int = 30,
        lang: str = "en"
) -> Dict[str, Any]:
    try:
        query = session.query(
            InventoryDetail.task_id,
            InventoryDetail.item_id,
            InventoryDetail.upc,
            InventoryDetail.material,
            InventoryDetail.grid,
            InventoryDetail.price,
            InventoryDetail.frozen_qty,
            InventoryDetail.Inventory_qty,
            InventoryDetail.difference_qty,
            InventoryDetail.difference_amt,
            InventoryDetail.create_time,
            InventoryDetail.update_time,
            ItmItem.name,
            ItmItem.description,
            ItmItem.dimension1,
        ).outerjoin(
            ItmItem,
            text("inventory_detail.item_id = ITM_ITEM.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        )

        if task_id:
            query = query.filter(InventoryDetail.task_id == task_id)
        if key_word:
            query = query.filter(
                (InventoryDetail.item_id.like(f"%{key_word}%")) |
                (InventoryDetail.upc.like(f"%{key_word}%"))
            )
        if only_difference:
            query = query.filter(InventoryDetail.difference_qty != 0)

        total = query.count()
        rows = query.order_by(InventoryDetail.item_id) \
            .offset((page - 1) * page_size).limit(page_size).all()

        data = []
        for row in rows:
            qty = row.frozen_qty or 0
            inv_qty = row.Inventory_qty or 0
            price = float(row.price or 0)
            diff_qty = qty - inv_qty
            data.append({
                "task_id": row.task_id,
                "item_id": row.item_id,
                "upc": row.upc,
                "name": row.name,
                "description": row.description,
                "color": row.dimension1,
                "material": row.material,
                "grid": row.grid,
                "price": price,
                "frozen_qty": qty,
                "Inventory_qty": inv_qty,
                "difference_qty": diff_qty,
                "difference_amt": round(diff_qty * price, 2),
                "create_time": str(row.create_time) if row.create_time else None,
                "update_time": str(row.update_time) if row.update_time else None,
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": data,
            "columns": get_table_columns("difference_detail_list", lang)
        }
    except Exception as e:
        app_logger.error(f"Error getting difference detail list: {str(e)}")
        raise


async def create_difference_detail(
        session: Session,
        detail_data: Dict[str, Any]
) -> InventoryDetail:
    try:
        new_detail = InventoryDetail(
            task_id=detail_data["task_id"],
            item_id=detail_data["item_id"],
            material=detail_data["material"],
            grid=detail_data["grid"],
            item_price=detail_data.get("item_price", 0),
            frozen_qty=detail_data.get("frozen_qty", 0),
            Inventory_qty=detail_data.get("Inventory_qty", 0),
            difference_qty=detail_data.get("difference_qty", 0),
            difference_amt=detail_data.get("difference_amt", 0),
            create_time=datetime.now(),
        )
        session.add(new_detail)
        session.commit()
        session.refresh(new_detail)
        app_logger.info(f"Created difference detail: task_id={new_detail.task_id}, item_id={new_detail.item_id}")
        return new_detail
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating difference detail: {str(e)}")
        raise


async def get_order_zone_list(
        session: Session,
        org_id: str,
        task_id: int
) -> List[str]:
    try:
        rows = session.query(
            InventoryOrder.zone_code
        ).filter(
            InventoryOrder.org_id == org_id,
            InventoryOrder.task_id == task_id
        ).distinct().all()
        zones = [r.zone_code for r in rows if r.zone_code]
        app_logger.info(f"[get_order_zone_list] org_id={org_id}, task_id={task_id}, zones={zones}")
        return zones
    except Exception as e:
        app_logger.error(f"Error getting order zone list: {str(e)}")
        raise


async def get_inventory_items_by_task_id(
        session: Session,
        task_id: int,
        key_word: Optional[str] = None,
        lang: str = "en"
) -> Dict[str, Any]:
    try:
        task = session.query(InventoryTask).filter(InventoryTask.task_id == task_id).first()
        if not task:
            raise ValueError(f"task_id={task_id} 不存在")

        store_name = session.query(LocRtlLoc.store_name).filter(
            cast(LocRtlLoc.rtl_loc_id, String) == task.store_code
        ).scalar()

        options_subq = session.query(
            ItmItemOptions.item_id,
            func.min(ItmItemOptions.part_number).label("part_number")
        ).group_by(ItmItemOptions.item_id).subquery(name="options_subq")

        material_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number, 1,
                            func.charindex('-', options_subq.c.part_number) - 1)),
            else_=''
        ).label('material')
        grid_expr = case(
            (options_subq.c.part_number.like('%-%'),
             func.substring(options_subq.c.part_number,
                            func.charindex('-', options_subq.c.part_number) + 1,
                            func.len(options_subq.c.part_number))),
            else_=''
        ).label('grid')

        query = session.query(
            InventoryItem.order_no,
            InventoryItem.item_id,
            InventoryItem.upc,
            InventoryItem.price,
            InventoryItem.inventory_qty,
            InventoryItem.create_time,
            InventoryItem.update_time,
            ItmItem.name,
            ItmItem.description,
            ItmItem.dimension1,
            material_expr,
            grid_expr,
        ).join(
            InventoryOrder,
            InventoryItem.order_no == InventoryOrder.order_no,
        ).outerjoin(
            ItmItem,
            text("inventory_item.item_id = ITM_ITEM.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).outerjoin(
            options_subq,
            text("inventory_item.item_id = options_subq.item_id COLLATE SQL_Latin1_General_CP1_CI_AS")
        ).filter(
            InventoryOrder.task_id == task_id
        )

        if key_word:
            query = query.filter(
                (InventoryItem.item_id.like(f"%{key_word}%")) |
                (InventoryItem.upc.like(f"%{key_word}%"))
            )

        rows = query.order_by(InventoryItem.order_no, InventoryItem.item_id).all()

        data = []
        for row in rows:
            qty = row.inventory_qty or 0
            price = float(row.price or 0)
            data.append({
                "order_no": row.order_no,
                "item_id": row.item_id,
                "upc": row.upc,
                "price": price,
                "inventory_qty": qty,
                "amount": round(qty * price, 2),
                "name": row.name,
                "description": row.description,
                "color": row.dimension1,
                "material": row.material,
                "grid": row.grid,
                "create_time": str(row.create_time) if row.create_time else None,
                "update_time": str(row.update_time) if row.update_time else None,
            })

        header = {
            "task_id": task.task_id,
            "batch_no": task.batch_no,
            "org_id": task.org_id,
            "store_code": task.store_code,
            "store_name": store_name,
            "inventory_date": str(task.inventory_date) if task.inventory_date else None,
            "inventory_method": task.inventory_method,
            "inventory_type": task.inventory_type,
            "status": task.status,
            "create_user": task.create_user,
            "create_time": str(task.create_time) if task.create_time else None,
        }

        return {
            "header": header,
            "total": len(data),
            "data": data,
            "columns": get_table_columns("inventory_item_list", lang)
        }
    except ValueError:
        raise
    except Exception as e:
        app_logger.error(f"Error getting inventory items by task_id: {str(e)}")
        raise


async def update_inventory_task_business_owner(
        session: Session,
        task_id: int,
        business_owner: str,
        user_id: Optional[str] = None
) -> Optional[InventoryTask]:
    try:
        task = session.query(InventoryTask).filter(InventoryTask.task_id == task_id).first()
        if not task:
            return None

        task.business_owner = business_owner
        task.update_user = user_id
        task.status='in_progress'
        task.update_time = datetime.now()
        session.commit()
        session.refresh(task)
        app_logger.info(f"[update_business_owner] task_id={task_id}, business_owner={business_owner}")
        return task
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error updating business_owner: {str(e)}")
        raise
