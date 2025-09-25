from enum import Enum

from pydantic import BaseModel, Field, model_validator
from datetime import datetime, time

from typing import List, Optional, Union, Dict


class Create_Type(str, Enum):
    _condition = "condition"
    _import = "import"


class condition_type(str, Enum):
    _and = "and"
    _or = "or"


class Segment_Type(str, Enum):
    item = "item"
    customer = "customer"
    location = "location"


class Promotion_Type(str, Enum):
    Product = "Product"
    Coupon = "Coupon"


class Item_Type(str, Enum):
    Condition = "Condition"
    Result = "Result"


class SegmentsBase(BaseModel):
    segment_id: Optional[int] = None
    name: str = "segment name"
    description: str = "segment description"
    segment_status: str = "active"
    public: int = 0
    export: int = 0
    sub_count: int = 0
    create_type: Create_Type
    condition_type: Optional[condition_type]


class SegmentsItemCreate(SegmentsBase):
    create_user: str


class SegmentsItemUpdate(SegmentsBase):
    update_user: str


class SegmentsItemConditionCreate(BaseModel):
    segment_id: int
    condition_name: str
    condition_type: str = '='
    condition_value: str
    create_user: str


class SegmentsCondition(BaseModel):
    condition_name: str
    condition_type: str = '='
    condition_value: Union[str, int, float]
    create_user: str


class Segment_Schedule(BaseModel):
    schedule_type: str = 'W'
    schedule_value: int = 1
    schedule_time: time
    create_user: str


class SegmentSubmit(BaseModel):
    segment: SegmentsBase
    segment_condition: Optional[List[SegmentsCondition]]
    segment_schedule: Optional[Segment_Schedule]
    segment_type: Segment_Type


class SegmentUpload(BaseModel):
    segment: SegmentsBase
    segment_type: Segment_Type


class Promotions(BaseModel):
    promotion_id: int
    name: str
    description: str
    promotion_type: Promotion_Type
    promotion_status: str = "active"
    class_id: int
    subclass_id: Optional[int] = Field(None)
    iteration_cap: Optional[int] = -1
    promotion_group: Optional[int] = Field(None)
    promotion_level: Optional[int] = Field(None)
    coupon_code: Optional[str] = Field(None)
    start_date: datetime
    end_date: datetime
    create_user: str
    update_user: str


class PromotionItemSegments(BaseModel):
    set_id: Optional[int] = 1
    segment_id: int
    item_type: Item_Type
    include: int = 1


class PromotionLocationSegments(BaseModel):
    segment_id: int
    include: int = 1


class PromotionCustomersSegments(BaseModel):
    segment_id: int
    include: int = 1


class PromotionCondition(BaseModel):
    set_id: Optional[int] = 1
    condition_type: str
    threshold_style: str
    MinQty: Optional[int] = 1
    MaxQty: Optional[int] = 9999
    MinItemTotal: Optional[float] = 0


class PromotionResult(BaseModel):
    set_id: Optional[int] = 2
    overlap: int = 0
    apply_type: str
    discount_type: str
    action_qty: Optional[int] = 0
    discount_value: Union[float, int]


class PromotionSubmit(BaseModel):
    promotion: Promotions
    promotion_condition: List[PromotionCondition]
    promotion_result: List[PromotionResult]
    promotion_item_segments: Optional[List[PromotionItemSegments]]
    promotion_location_segments: Optional[List[PromotionLocationSegments]] = None
    promotion_org_data: Optional[List[str]] = None
    promotion_customer_segments: Optional[List[PromotionCustomersSegments]]

    @model_validator(mode='after')
    def check_location_or_org_data(self):
        location_segments = self.promotion_location_segments
        org_data = self.promotion_org_data

        # 检查两个字段是否都为空或 None
        if not location_segments and not org_data:
            raise ValueError("Either 'promotion_location_segments' or 'promotion_org_data' must be provided.")

        return self


class SysUserRole(BaseModel):
    role_code: str


class SysUserSubmit(BaseModel):
    user_code: str
    user_name: str
    user_password: str
    user_status: str = "active"
    role_code: Optional[List[SysUserRole]]
    user_email: Optional[str] = Field(None)
    create_time: Optional[datetime] = Field(None)
    update_time: Optional[datetime] = Field(None)
    create_user: Optional[str] = Field(None)
    update_user: Optional[str] = Field(None)


class SysRoleSubmit(BaseModel):
    role_code: str
    role_description: str
    role_status: str = "active"
    data: Optional[List[Dict]] = Field(None)
    org_data: Optional[List[str]] = Field(None)
    create_time: Optional[datetime] = Field(None)
    update_time: Optional[datetime] = Field(None)
    create_user: Optional[str] = Field(None)
    update_user: Optional[str] = Field(None)


class RoleMenuPermissionsUpdate(BaseModel):
    menu_permissions: Dict[str, List[str]]
