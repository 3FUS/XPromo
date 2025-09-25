from sqlalchemy.orm import Session
import asyncio
import service
from models.model import SegmentsItemCondition, SegmentsItemDetail, SegmentsLocationCondition, SegmentsLocationDetail, \
    SegmentsCustomerCondition, SegmentsCustomerDetail

import pandas as pd
from datetime import datetime

import yaml
from typing import Optional

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

    for condition in conditions:
        condition_name = condition["condition_name"]
        condition_type = condition["condition_type"]
        condition_value = condition["condition_value"]

        col_name_map = condition_name_mapping.get(segment_type, {})
        if condition_name not in col_name_map:
            raise KeyError(f"Condition name '{condition_name}' not found for segment type '{segment_type}'")

        col_name = col_name_map[condition_name]
        col_type = field_type_config.get(segment_type, {}).get(col_name, 'str')

        current_mask = pd.Series(False, index=df.index)

        if condition_type in operators:
            converted_val = convert_value(condition_value, col_type)
            current_mask = operators[condition_type](df[col_name], converted_val)
        elif condition_type == "between" and "," in condition_value:
            parts = condition_value.split(",")
            if len(parts) != 2:
                raise ValueError("Between condition requires exactly two values separated by comma")
            low, high = map(lambda x: x.strip(), parts)
            low = convert_value(low, col_type)
            high = convert_value(high, col_type)
            current_mask = (df[col_name] >= low) & (df[col_name] <= high)
        elif condition_type == "include":
            values = convert_values(condition_value.split(","), col_type)
            current_mask = df[col_name].isin(values)
        elif condition_type == "exclude":
            values = convert_values(condition_value.split(","), col_type)
            current_mask = ~df[col_name].isin(values)
        else:
            raise ValueError(f"Unsupported condition type: {condition_type}")

        if condition_logic == "and":
            combined_mask &= current_mask
        else:
            combined_mask |= current_mask

    return df[combined_mask]


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


def load_item_data_from_db(segment_type, engine=None):
    if engine is None:
        engine = service.get_engine()
    if segment_type == 'item':
        sql = "SELECT item_id,parent_item_id, name,description, list_price, merch_level_1,merch_level_2,merch_level_3,merch_level_4 FROM itm_item where item_lvlcode='ITEM'"
    elif segment_type == 'customer':
        sql = "SELECT a.party_id, party_typcode, first_name, sign_up_rtl_loc_id,telephone_number,gender,birth_date FROM crm_party a INNER JOIN crm_party_telephone b on a.party_id=b.party_id where telephone_number is not null"
    elif segment_type == 'location':
        sql = "SELECT rtl_loc_id, store_name, city, location_type, country FROM loc_rtl_loc"
    else:
        return pd.DataFrame()
    chunks = pd.read_sql(sql, engine, chunksize=5000)
    return pd.concat(chunks, ignore_index=True)


async def run_segment_cleaning(segment_type, segment_id, condition_logic, session: Optional[Session] = None):
    engine = service.get_engine()
    external_session = session is not None
    if not external_session:
        session = service.create_session()

    now_time = datetime.now()
    try:
        conditions = fetch_segment_conditions(segment_type, session, segment_id)
        if not conditions:
            raise ValueError(f"No conditions found for segment_id {segment_id}")

        raw_df = load_item_data_from_db(segment_type, engine)
        cleaned_df = apply_conditions_to_items(segment_type, raw_df, conditions, condition_logic)

        if cleaned_df.empty:
            raise ValueError(f"No items matched the conditions for segment_id {segment_id}")

        model_class = SEGMENT_DETAIL_MODELS.get(segment_type)
        field_map = SEGMENT_FIELD_MAPS.get(segment_type)
        id_field = SEGMENT_ID_FIELDS.get(segment_type)

        if not all([model_class, field_map, id_field]):
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
            session.query(model_class).filter(model_class.segment_id == segment_id).delete(synchronize_session=False)
            sub_count = _insert_details(session, model_class, cleaned_df, segment_id, field_map, now_time)
            segment_some = {"run_time": now_time, "sub_count": sub_count, 'update_time': now_time}
        else:
            segment_some = {"run_time": now_time, "sub_count": len(new_ids)}

        session.commit()
        await update_segment_some(segment_type, session, segment_id, segment_some)
        print(f"Segment {segment_id} processed successfully.")

    except Exception as e:
        session.rollback()
        print(f"Error processing segment {segment_id}: {str(e)}")
        raise
    finally:
        session.close()
        engine.dispose()


if __name__ == '__main__':
    print(f"[{datetime.now()}] start.")
    asyncio.run(run_segment_cleaning('item', 20006, 'and'))
