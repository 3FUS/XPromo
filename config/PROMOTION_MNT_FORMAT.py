
class Mnt_Format:
    def __init__(self, promotion_id):
        self.promotion_id = promotion_id

    # DEAL = f'INSERT|DEAL|1001|KBS-全場商品服飾配件類88折||2025-01-01T00:00:00|2025-02-02T23:59:00|||999999999|-1|1||999999999||||||||1|0||0|0|0|1|1'
    # DEAL_ITEM = f'INSERT|DEAL_ITEM|1001|1|1|1|999||PERCENT_OFF|12|||||'
    #
    def get_mnt_format(self):
        DEAL_ITEM_TEST = f'INSERT|DEAL_ITEM_TEST|{self.promotion_id}|1|SKU|EQUAL|I1JP01002||1|1'
        return DEAL_ITEM_TEST
