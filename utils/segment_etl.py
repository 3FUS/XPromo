from sqlalchemy import text
from sqlalchemy.orm import Session
import asyncio
import service
from models.model import SegmentsItemCondition, SegmentsItemDetail, SegmentsLocationCondition, SegmentsLocationDetail, \
    SegmentsCustomerCondition, SegmentsCustomerDetail

import pandas as pd
from datetime import datetime

import yaml
from typing import Optional

from service.promotion import get_promotionId_by_segmentId, get_location_detail_by_promotionId
from service.worker import create_worker_task
from utils.logger import app_logger

from service.segments_service import update_segment_some

with open('./config/segments_condition.yaml', 'r', encoding='utf-8') as mapping_file:
    mapping_config = yaml.safe_load(mapping_file)
condition_name_mapping = mapping_config.get("condition_name_mapping", {})
item_mapping = condition_name_mapping.get("item", {})
field_type_config = mapping_config.get("field_types", {})

SEGMENT_FIELD_MAPS = {
    "item": {"item_id": "item_id", "item_name": "name", "item_description": "description",
             "item_price": "list_price"},
    "customer": {"party_id": "party_id", "first_name": "first_name", "cust_phone": "telephone_number"},
    "location": {"rtl_loc_id": "rtl_loc_id", "store_name": "store_name", "city": "city"}
}

SEGMENT_DETAIL_MODELS = {
    "item": SegmentsItemDetail,
    "customer": SegmentsCustomerDetail,
    "location": SegmentsLocationDetail
}

SEGMENT_ID_FIELDS = {
    "item": "item_id",
    "customer": "party_id",
    "location": "rtl_loc_id"
}


def fetch_segment_conditions(segment_type, db: Session, segment_id: int):
    if segment_type == 'item':
        conditions = db.query(SegmentsItemCondition).filter(
            SegmentsItemCondition.segment_id == segment_id
        ).all()
    elif segment_type == 'customer':
        conditions = db.query(SegmentsCustomerCondition).filter(
            SegmentsCustomerCondition.segment_id == segment_id
        ).all()
    elif segment_type == 'location':
        conditions = db.query(SegmentsLocationCondition).filter(
            SegmentsLocationCondition.segment_id == segment_id
        ).all()
    else:
        return []
    return [
        {
            "condition_name": cond.condition_name,
            "condition_type": cond.condition_type,
            "condition_value": cond.condition_value
        }
        for cond in conditions
    ]


def apply_conditions_to_items(segment_type, df: pd.DataFrame, conditions: list, condition_logic: str = "and"):
    app_logger.debug(f"[apply_conditions_to_items] Processing segment type: {segment_type}")
    app_logger.debug(f"[apply_conditions_to_items] DataFrame shape: {df.shape}")
    app_logger.debug(f"[apply_conditions_to_items] DataFrame columns: {list(df.columns)}")
    app_logger.debug(f"[apply_conditions_to_items] Number of conditions: {len(conditions)}")
    app_logger.debug(f"[apply_conditions_to_items] Condition logic: {condition_logic}")

    def convert_value(val, col_type):
        if col_type == "int":
            return int(val)
        elif col_type == "float":
            return float(val)
        return val

    def convert_values(vals, col_type):
        if col_type == "int":
            return [int(x.strip()) for x in vals]
        elif col_type == "float":
            return [float(x.strip()) for x in vals]
        return [x.strip() for x in vals]

    operators = {
        "=": lambda df_col, val: df_col == val,
        "<>": lambda df_col, val: df_col != val,
        ">": lambda df_col, val: df_col > val,
        "<": lambda df_col, val: df_col < val,
    }

    combined_mask = pd.Series(True, index=df.index) if condition_logic == "and" else pd.Series(False, index=df.index)
    app_logger.debug(f"[apply_conditions_to_items] Initial combined mask shape: {combined_mask.shape}")

    for i, condition in enumerate(conditions):
        condition_name = condition["condition_name"]
        condition_type = condition["condition_type"]
        condition_value = condition["condition_value"]

        app_logger.debug(
            f"[apply_conditions_to_items] Processing condition {i + 1}: {condition_name} {condition_type} {condition_value}")

        col_name_map = condition_name_mapping.get(segment_type, {})
        app_logger.debug(f"[apply_conditions_to_items] Column name mapping for {segment_type}: {col_name_map}")

        if condition_name not in col_name_map:
            app_logger.error(
                f"[apply_conditions_to_items] Condition name '{condition_name}' not found for segment type '{segment_type}'")
            app_logger.error(f"[apply_conditions_to_items] Available condition names: {list(col_name_map.keys())}")
            raise KeyError(f"Condition name '{condition_name}' not found for segment type '{segment_type}'")

        col_name = col_name_map[condition_name]
        app_logger.debug(f"[apply_conditions_to_items] Mapped column name: {col_name}")

        # 将列名转换为小写以匹配DataFrame中的列名
        col_name_lower = col_name.lower()
        app_logger.debug(f"[apply_conditions_to_items] Converted column name to lowercase: {col_name_lower}")

        if col_name_lower not in df.columns:
            app_logger.error(
                f"[apply_conditions_to_items] Column '{col_name_lower}' (mapped from '{condition_name}' and converted to lowercase) not found in DataFrame")
            app_logger.error(f"[apply_conditions_to_items] Available DataFrame columns: {list(df.columns)}")
            raise KeyError(
                f"Column '{col_name_lower}' (mapped from '{condition_name}' and converted to lowercase) not found in DataFrame. Available columns: {list(df.columns)}")

        col_type = field_type_config.get(segment_type, {}).get(col_name_lower, 'str')
        app_logger.debug(f"[apply_conditions_to_items] Column type for '{col_name_lower}': {col_type}")

        current_mask = pd.Series(False, index=df.index)

        if condition_type in operators:
            converted_val = convert_value(condition_value, col_type)
            app_logger.debug(
                f"[apply_conditions_to_items] Applying operator '{condition_type}' with value '{converted_val}' on column '{col_name_lower}'")
            current_mask = operators[condition_type](df[col_name_lower], converted_val)
        elif condition_type == "between" and "," in condition_value:
            parts = condition_value.split(",")
            if len(parts) != 2:
                raise ValueError("Between condition requires exactly two values separated by comma")
            low, high = map(lambda x: x.strip(), parts)
            low = convert_value(low, col_type)
            high = convert_value(high, col_type)
            app_logger.debug(
                f"[apply_conditions_to_items] Applying 'between' condition with range [{low}, {high}] on column '{col_name_lower}'")
            current_mask = (df[col_name_lower] >= low) & (df[col_name_lower] <= high)
        elif condition_type == "include":
            values = convert_values(condition_value.split(","), col_type)
            app_logger.debug(
                f"[apply_conditions_to_items] Applying 'include' condition with values {values} on column '{col_name_lower}'")
            current_mask = df[col_name_lower].isin(values)
        elif condition_type == "exclude":
            values = convert_values(condition_value.split(","), col_type)
            app_logger.debug(
                f"[apply_conditions_to_items] Applying 'exclude' condition with values {values} on column '{col_name_lower}'")
            current_mask = ~df[col_name_lower].isin(values)
        else:
            app_logger.error(f"[apply_conditions_to_items] Unsupported condition type: {condition_type}")
            raise ValueError(f"Unsupported condition type: {condition_type}")

        app_logger.debug(f"[apply_conditions_to_items] Condition {i + 1} mask sum: {current_mask.sum()}")

        if condition_logic == "and":
            app_logger.debug(
                f"[apply_conditions_to_items] Before AND operation, combined mask sum: {combined_mask.sum()}")
            combined_mask &= current_mask
            app_logger.debug(
                f"[apply_conditions_to_items] After AND operation, combined mask sum: {combined_mask.sum()}")
        else:
            app_logger.debug(
                f"[apply_conditions_to_items] Before OR operation, combined mask sum: {combined_mask.sum()}")
            combined_mask |= current_mask
            app_logger.debug(
                f"[apply_conditions_to_items] After OR operation, combined mask sum: {combined_mask.sum()}")

    result_df = df[combined_mask]
    app_logger.debug(f"[apply_conditions_to_items] Final result shape: {result_df.shape}")
    app_logger.debug(f"[apply_conditions_to_items] Combined mask sum: {combined_mask.sum()}")

    return result_df


def _insert_details(session, model_class, data_rows, segment_id, field_map, create_time):
    details = [
        model_class(
            segment_id=segment_id,
            create_time=create_time,
            **{field: row[val] for field, val in field_map.items()}
        )
        for _, row in data_rows.iterrows()
    ]
    session.add_all(details)
    return len(details)


def load_item_data_from_db(segment_type, org_id=None, engine=None):
    if engine is None:
        engine = service.get_engine()
    if segment_type == 'item':
        sql = ("SELECT itm_item.item_id,parent_item_id, name,description, list_price, "
               "merch_level_1,merch_level_2,merch_level_3,merch_level_4,vendor,"
               "case  when part_number like '%-%'  then "
               "SUBSTRING(part_number, 1, CHARINDEX('-', part_number) - 1)  else '' end AS material,"
               "case  when part_number like '%-%'  then "
               "SUBSTRING(part_number, CHARINDEX('-', part_number) + 1, LEN(part_number)) else '' end AS grid "
               "FROM itm_item "
               "LEFT JOIN itm_item_options ON itm_item.ORGANIZATION_ID=itm_item_options.organization_id "
               "AND itm_item.ITEM_ID=itm_item_options.ITEM_ID "
               "INNER JOIN itm_item_prices ON itm_item.ORGANIZATION_ID=itm_item_prices.organization_id "
               "AND itm_item.ITEM_ID=itm_item_prices.ITEM_ID "
               "WHERE item_lvlcode='ITEM' "
               "AND itm_item_prices.level_value=:org_id")
    elif segment_type == 'customer':
        sql = ("SELECT a.party_id, party_typcode, first_name, sign_up_rtl_loc_id,telephone_number,gender,birth_date "
               "FROM crm_party a INNER JOIN crm_party_telephone b on a.party_id=b.party_id "
               "where telephone_number is not null")
    elif segment_type == 'location':
        sql = ("SELECT * FROM loc_rtl_loc WHERE EXISTS "
               "(SELECT 1 from loc_org_hierarchy "
               "where loc_org_hierarchy.organization_id=loc_rtl_loc.organization_id "
               "and loc_org_hierarchy.ORG_VALUE=loc_rtl_loc.rtl_loc_id "
               "and loc_org_hierarchy.org_code='STORE' and loc_org_hierarchy.parent_value=:org_id)")
    else:
        return pd.DataFrame()
    chunks = pd.read_sql(text(sql), engine, params={"org_id": org_id}, chunksize=5000)
    df = pd.concat(chunks, ignore_index=True)
    df.columns = df.columns.str.lower()
    return df


async def get_segments_for_current_time(segment_type: str = None):
    """
    查询当前时间需要执行的 segments
    根据 schedule_type, schedule_value, schedule_time 判断
    :param segment_type: 指定查询的段类型 ('item', 'location', 'customer')，如果为None则查询所有类型
    """
    current_time = datetime.now()
    current_hour = current_time.hour
    current_minute = current_time.minute
    current_weekday = current_time.weekday()  # 0=Monday, 6=Sunday (Monday=0)
    current_day = current_time.day

    # 格式化当前时间为 HH:MM 格式
    time_str = f"{current_hour:02d}:{current_minute:02d}"

    # 构建查询条件
    base_condition = """
        AND ss.schedule_time = :time_str
        AND (
            (ss.schedule_type = 'D')  -- 每天执行
            OR (ss.schedule_type = 'W' AND ss.schedule_value = :current_weekday)  -- 每周指定星期
            OR (ss.schedule_type = 'M' AND ss.schedule_value = :current_day)   -- 每月指定日期
        )
    """

    # 根据 segment_type 参数构建不同的查询
    queries = []
    table_configs = []

    if segment_type is None or segment_type == 'item':
        table_configs.append({
            'table': 'segments_items',
            'type': 'item',
            'condition_field': 'condition_type'
        })

    if segment_type is None or segment_type == 'location':
        table_configs.append({
            'table': 'segments_locations',
            'type': 'location',
            'condition_field': 'condition_type'
        })

    if segment_type is None or segment_type == 'customer':
        table_configs.append({
            'table': 'segments_customers',
            'type': 'customer',
            'condition_field': 'condition_type'
        })

    # 构建 UNION 查询
    union_parts = []
    for config in table_configs:
        query_part = f"""
        SELECT 
            s.segment_id,
            ss.schedule_type,
            ss.schedule_value,
            ss.schedule_time,
            s.{config['condition_field']} as condition_type,
            '{config['type']}' as segment_type
        FROM {config['table']} s
        INNER JOIN segments_schedule ss ON ss.segment_id = s.segment_id 
            AND ss.segment_type = '{config['type']}'
        WHERE s.segment_status = 'active' 
            AND s.create_type = 'condition'
        """
        query_part += base_condition
        union_parts.append(query_part)

    final_query = " UNION ALL ".join(union_parts)
    query = text(final_query)

    try:
        engine = service.get_engine()
        with engine.connect() as conn:
            result = conn.execute(query, {
                'time_str': time_str,
                'current_weekday': current_weekday,
                'current_day': current_day
            })

            segments = []
            for row in result:
                segment = {
                    'segment_id': row[0],
                    'schedule_type': row[1],
                    'schedule_value': row[2],
                    'schedule_time': row[3],
                    'condition_type': row[4],
                    'segment_type': row[5]
                }
                segments.append(segment)

            app_logger.info(f"ETL Segments for current time:{time_str}, found {len(segments)} segments")
            return segments

    except Exception as e:
        app_logger.error(f"Error getting segments for current time: {str(e)}")
        return []


async def run_segment_cleaning(segment_type=None, segment_id=None, condition_logic='and', org_id=None,
                               session: Optional[Session] = None):
    if segment_id is None:
        # 执行当前时间需要的所有 segments
        segments_to_run = await get_segments_for_current_time(segment_type)  # 传入 segment_type 参数
        for segment in segments_to_run:
            app_logger.info(f"ETL segment {segment['segment_id']} of type {segment['segment_type']}")
            await _execute_single_segment(
                segment['segment_id'],
                segment['segment_type'],
                segment.get('condition_type', 'and'),
                session, True, segment.get('org_id', '*'),
            )
    else:
        # 执行特定 segment
        await _execute_single_segment(segment_id, segment_type, condition_logic, session, False, org_id)


async def _execute_single_segment(segment_id: int, segment_type: str, condition_logic: str,
                                  session: Optional[Session] = None, is_schedule: bool = True, org_id=None):
    """执行单个 segment 的清理任务"""
    engine = service.get_engine()
    external_session = session is not None
    if not external_session:
        session = service.create_session()

    now_time = datetime.now()
    try:
        conditions = fetch_segment_conditions(segment_type, session, segment_id)
        if not conditions:
            raise ValueError(f"No conditions found for segment_id {segment_id}")

        raw_df = load_item_data_from_db(segment_type, org_id, engine)
        cleaned_df = apply_conditions_to_items(segment_type, raw_df, conditions, condition_logic)

        if cleaned_df.empty:
            app_logger.warning(f"[run segment cleaning], No items matched the conditions for segment_id {segment_id}")
            # raise ValueError(f"No items matched the conditions for segment_id {segment_id}")
            return

        model_class = SEGMENT_DETAIL_MODELS.get(segment_type)
        field_map = SEGMENT_FIELD_MAPS.get(segment_type)
        id_field = SEGMENT_ID_FIELDS.get(segment_type)

        if not all([model_class, field_map, id_field]):
            app_logger.error(f"[run segment cleaning], Invalid segment type: {segment_type}")
            raise ValueError(f"Invalid segment type: {segment_type}")

        existing_ids = {
            detail[0] for detail in session.query(model_class.__dict__[id_field]).filter(
                model_class.segment_id == segment_id
            ).all()
        }

        new_ids = set(cleaned_df[id_field].unique())
        added_ids = new_ids - existing_ids
        removed_ids = existing_ids - new_ids
        has_changes = bool(added_ids or removed_ids)

        if has_changes:
            app_logger.info(
                f"[run segment cleaning], segment_id {segment_id} has changes: added {len(added_ids)}, removed {len(removed_ids)}")
            session.query(model_class).filter(model_class.segment_id == segment_id).delete(synchronize_session=False)
            session.flush()
            cleaned_df = cleaned_df.drop_duplicates(subset=[id_field], keep='first')

            sub_count = _insert_details(session, model_class, cleaned_df, segment_id, field_map, now_time)
            segment_some = {"run_time": now_time, "sub_count": sub_count, 'update_time': now_time}
            if is_schedule:
                promotion_ids = await get_promotionId_by_segmentId(segment_id, session)
                all_rtl_loc_ids = []
                for promotion_id in promotion_ids:
                    app_logger.info(f"[run segment cleaning], promotion_id: {promotion_id}")
                    locs_data = await get_location_detail_by_promotionId(promotion_id.get('promotion_id'), session)
                    df_locs = locs_data['data']

                    if not df_locs.empty:
                        all_rtl_loc_ids.extend(df_locs['rtl_loc_id'].tolist())

                unique_rtl_loc_ids = list(set(all_rtl_loc_ids)) if all_rtl_loc_ids else []
                app_logger.info(f"[run segment cleaning], unique_rtl_loc_ids: {unique_rtl_loc_ids}")
                if unique_rtl_loc_ids:
                    sessionId = await create_worker_task(session, unique_rtl_loc_ids, 'segment_item',
                                                         segment_id)
                    segment_some['last_session_id'] = sessionId
                    segment_some["export_time"] = now_time

        else:
            app_logger.info(f"[run segment cleaning], segment_id {segment_id} has no changes")
            segment_some = {"run_time": now_time, "sub_count": len(new_ids)}

        session.commit()
        await update_segment_some(segment_type, session, segment_id, segment_some)
        app_logger.info(f"[run segment cleaning], segment_id {segment_id} processed successfully.")

    except Exception as e:
        session.rollback()
        app_logger.error(f"Error processing segment {segment_id}: {str(e)}")
        raise
    finally:
        if not external_session:
            session.close()
            engine.dispose()

#
# if __name__ == '__main__':
#     print(f"[{datetime.now()}] start.")
#     asyncio.run(run_segment_cleaning('item', 20006, 'and'))
