"""
盘点模块 Pydantic Schema
"""
from pydantic import BaseModel, Field
from datetime import datetime, date
from typing import Optional, List
from decimal import Decimal


# ==================== InventoryTask Schema ====================

class InventoryTaskBase(BaseModel):
    # batch_no: str = Field(..., description="盘点批次号")
    org_id: str = Field(..., description="组织ID")
    store_code: str = Field(..., description="店铺代码")
    inventory_date: date = Field(None, description="盘点日期")
    inventory_method: str = Field("店舗棚卸", description="盘点方式: 店舗棚卸, 外部棚卸会社")
    inventory_type: str = Field("full", description="盘点类型: sample=抽盘, full=全盘")
    status: str = Field("in_progress", description="任务状态: in_progress/confirming/cancelled/completed")
    create_user: str = Field(..., description="制单人")
    create_date: Optional[date] = Field(None, description="制单日期")
    operator: Optional[str] = Field(None, description="操作人")
    remark: Optional[str] = Field(None, description="备注")


class InventoryTaskCreate(InventoryTaskBase):
    pass


class InventoryTaskUpdate(BaseModel):
    inventory_method: Optional[str] = Field(None, description="盘点方式: self/external")
    inventory_type: Optional[str] = Field(None, description="盘点类型: sample/full")
    status: Optional[str] = Field(None, description="任务状态: in_progress/confirming/cancelled/completed")
    difference_date: Optional[date] = None
    confirm_user: Optional[str] = None
    operator: Optional[str] = None
    remark: Optional[str] = None


class InventoryTaskResponse(InventoryTaskBase):
    task_id: int
    difference_date: Optional[date] = None
    export_count: int = 0
    confirm_user: Optional[str] = None
    confirm_time: Optional[datetime] = None
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==================== InventoryOrder Schema ====================

class InventoryOrderBase(BaseModel):
    order_no: str = Field(..., description="盘点单号")
    task_id: int = Field(..., description="所属盘点任务ID")
    org_id: Optional[str] = Field(None, description="组织ID")
    store_code: str = Field(..., description="店铺代码")
    zone_code: Optional[str] = Field(None, description="货位")
    inventory_date: date = Field(..., description="盘点日期")
    status: str = Field("pending", description="状态: pending/in_progress/completed/confirmed")
    is_red: bool = Field(False, description="是否红盘")
    red_dimension: Optional[str] = Field(None, description="红盘维度: sku=SKU级别, location=货位级别")
    data_source: str = Field("manual", description="数据来源: manual=手工录入, imported=导入")
    import_file_name: Optional[str] = Field(None, description="导入文件名")
    create_user: str = Field(..., description="制单人")
    confirm_user: Optional[str] = None
    remark: Optional[str] = None



class InventoryOrderUpdate(BaseModel):
    zone_code: Optional[str] = None
    status: Optional[str] = None
    is_red: Optional[bool] = None
    red_dimension: Optional[str] = None
    data_source: Optional[str] = None
    import_file_name: Optional[str] = None
    confirm_user: Optional[str] = None
    confirm_time: Optional[datetime] = None
    remark: Optional[str] = None


class InventoryOrderResponse(InventoryOrderBase):
    create_time: Optional[datetime] = None
    confirm_time: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==================== InventoryItem Schema ====================

class InventoryItemBase(BaseModel):
    order_no: str = Field(..., description="所属盘点单号")
    item_id: str = Field(..., description="商品SKU代码")
    upc: Optional[str] = Field(None, description="商品UPC")
    price: Decimal = Field(0, description="商品单价")
    inventory_qty: int = Field(1, description="盘点数量")


class InventoryOrderCreate(InventoryOrderBase):
    items: Optional[List[InventoryItemBase]] = Field(None, description="盘点明细列表，可选")


class InventoryItemCreate(BaseModel):
    item_id: str = Field(..., description="商品SKU代码")
    upc: Optional[str] = Field(None, description="商品UPC")
    price: Decimal = Field(0, description="商品单价")
    inventory_qty: int = Field(1, description="盘点数量")



class InventoryItemUpdate(BaseModel):
    inventory_qty: int = Field(..., description="盘点数量")


class InventoryItemBatchUpdate(BaseModel):
    order_no: str = Field(..., description="所属盘点单号")
    items: List[InventoryItemCreate] = Field(..., description="盘点明细列表")

class InventoryItemResponse(InventoryItemBase):
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==================== InventorySnapshotHeader Schema ====================

class SnapshotHeaderBase(BaseModel):
    task_id: int = Field(..., description="所属盘点任务ID")
    org_id: str = Field(..., description="组织ID")
    store_code: str = Field(..., description="店铺代码")
    inventory_date: date = Field(..., description="盘点日期")
    import_method: str = Field("auto_frozen", description="数据导入方式: imported=导入, auto_frozen=系统自动冻结")
    operator: Optional[str] = Field(None, description="操作人")
    operate_time: Optional[datetime] = Field(None, description="操作日期")
    remark: Optional[str] = None


class SnapshotHeaderCreate(SnapshotHeaderBase):
    pass


class SnapshotHeaderUpdate(BaseModel):
    inventory_date: Optional[date] = None
    import_method: Optional[str] = None
    operator: Optional[str] = None
    operate_time: Optional[datetime] = None
    remark: Optional[str] = None


class SnapshotHeaderResponse(SnapshotHeaderBase):
    snapshot_id: int
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==================== InventorySnapshotDetail Schema ====================

class SnapshotDetailBase(BaseModel):
    snapshot_id: int = Field(..., description="所属快照ID")
    item_id: str = Field(..., description="商品SKU代码")
    task_id: int = Field(..., description="所属盘点任务ID")
    material: str = Field(..., description="物料代码")
    grid: str = Field(..., description="格子代码")
    stock_category: str = Field(..., description="库存类别")
    frozen_qty: int = Field(0, description="冻结库存数量")
    frozen_amt: Optional[Decimal] = Field(0, description="冻结库存金额")


class SnapshotDetailCreate(SnapshotDetailBase):
    pass


class SnapshotDetailResponse(SnapshotDetailBase):
    create_time: Optional[datetime] = None

    class Config:
        from_attributes = True


# ==================== InventoryDifferenceDetail Schema ====================

class DifferenceDetailBase(BaseModel):
    task_id: int = Field(..., description="所属盘点任务ID")
    item_id: str = Field(..., description="商品SKU代码")
    material: str = Field(..., description="物料代码")
    grid: str = Field(..., description="格子代码")
    item_price: Optional[Decimal] = Field(0, description="商品单价")
    frozen_qty: int = Field(0, description="冻结数量(系统库存)")
    Inventory_qty: int = Field(0, description="盘点数量(实际盘点)")
    difference_qty: int = Field(0, description="差异数量")
    difference_amt: Optional[Decimal] = Field(0, description="差异金额")


class DifferenceDetailCreate(DifferenceDetailBase):
    pass


class DifferenceDetailResponse(DifferenceDetailBase):
    create_time: Optional[datetime] = None

    class Config:
        from_attributes = True
