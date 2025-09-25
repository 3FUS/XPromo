from typing import Dict, Any


#

def generate_deal_insert(promotion_id: int, line: dict) -> str:
    return (
        f"INSERT|"  # 01\n"
        f"DEAL|"  # 02\n"
        f"{promotion_id}|"  # 03\n"
        f"{line.get('description')}|"  # 04\n"
        f"|"  # 05\n"
        f"{line.get('effective_date')}|"  # 06\n"
        f"{line.get('end_date')}|"  # 07\n"
        f"|"  # 08\n"
        f"|"  # 09\n"
        f"999999999|"  # 10\n"
        f"{line.get('iteration_cap') or ''}|"  # 11\n"
        f"1|"  # 12\n"
        f"|"  # 13\n"
        f"999999999|"  # 14\n"
        f"{line.get('trwide_action') or ''}|"  # 15\n"
        f"{line.get('trwide_amount') or ''}|"  # 16\n"
        f"|"  # 17\n"
        f"|"  # 18\n"
        f"|"  # 19\n"
        f"|"  # 20\n"
        f"|"  # 21\n"
        f"|"  # 22\n"
        f"1|"  # 23\n"
        f"0|"  # 24\n"
        f"|"  # 25\n"
        f"{line.get('act_deferred')}|"  # 26\n"
        f"0|"  # 27\n"
        f"{line.get('consumable')}|"  # 28\n"
        f"{line.get('sort_order') or ''}|"  # 29\n"
        f"|"   # 30
        f"{line.get('group_id') or ''}\n"  # 31
    )



def generate_deal_item_insert(promotion_id: int, line: dict) -> str:
    return (
        f"INSERT|"  # 01\n"
        f"DEAL_ITEM|"  # 02\n"
        f"{promotion_id}|"  # 03\n"
        f"{line.get('item_ordinal')}|"  # 04\n"
        f"{line.get('consumable') or ''}|"  # 05\n"
        f"{line.get('qty_min') or ''}|"  # 06\n"
        f"{line.get('qty_max') or ''}|"  # 07\n"
        f"{line.get('min_item_total') or ''}|"  # 08\n"
        f"{line.get('deal_action') or ''}|"  # 09\n"
        f"{line.get('action_arg') or ''}|"  # 10\n"
        f"{line.get('action_arg_qty') or ''}|"  # 11\n"
        f"|"  # 12\n"
        f"|"  # 13\n"
        f"|"  # 14\n"
        f"|"  # 15\n"
        f"1|"  # 16\n"
        f"0|"  # 17\n"
        f"|"  # 18\n"
        f"0|"  # 19\n"
        f"0|"  # 20\n"
        f"0|"  # 21\n"
        f"1|"  # 22\n"
        f"1\n"  # 23
    )


def generate_deal_item_test_insert(promotion_id: int, line: dict) -> str:
    return (
        f"INSERT|"  # 01\n"
        f"DEAL_ITEM_TEST|"  # 02\n"
        f"{promotion_id}|"  # 03\n"
        f"{line.get('item_ordinal') or ''}|"  # 04\n"
        f"{line.get('item_field') or ''}|"  # 05\n"
        f"{line.get('match_rule') or ''}|"  # 06\n"
        f"{line.get('value1') or ''}|"  # 07\n"
        f"|"  # 08\n"
        f"1|"  # 09\n"
        f"{line.get('item_condition_group') or ''}"  # 10\n"
        f" \n"
    )


def generate_deal_trigger_insert(promotion_id: int, line: dict) -> str:
    return (
        f"INSERT|"  # 01\n"
        f"DEAL_TRIGGER|"  # 02\n"
        f"{promotion_id}|"  # 03
        f"|"  # 04\
        f"{line.get('deal_trigger') or ''}|"  # 05
        f"|"  # 06\n"
        f"|"  # 07\n"
        f" \n"
    )


def generate_deal_coupon_xref_insert(promotion_id: int, line: dict) -> str:
    return (
        f"INSERT|"  # 01\n"
        f"COUPON_XREF|"  # 02
        f"{line.get('coupon_serial_nbr') or ''}|"  # 03\n"
        f"|"  # 04\n"
        f"|"  # 05\n"
        f"{line.get('effective_date')}|"  # 06
        f"{line.get('expiration_date')}|"  # 07
        f"{line.get('coupon_type') or ''}|"  # 08
        f"{line.get('serialized_flag') or ''}|"  # 09
        f"*|"  # 10
        f"*"  # 11
        f" \n"
    )
