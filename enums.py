
from enum import Enum

class Segment_Type(Enum):
    item = "item"
    location = "location"
    customer = "customer"


class Segment_Status(Enum):
    active = "active"
    inactive = "inactive"


class Data_Status(Enum):
    ALL = "ALL"
    active = "active"
    inactive = "inactive"


class Promotion_Type(Enum):
    Product = "Product"
    Coupon = "Coupon"
