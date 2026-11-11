from sqlalchemy import Column, Integer, String, ForeignKey, Text, BigInteger, Date, DateTime, Boolean, Numeric
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index
from datetime import datetime

Base = declarative_base()


class InventoryTask(Base):
    """
    盘点任务表 - 对应「盘点任务列表」菜单
    一个盘点任务包含多个盘点单(InventoryOrder)
    """
    __tablename__ = "inventory_task"

    task_id = Column(BigInteger, primary_key=True, comment="盘点任务ID")
    batch_no = Column(String(50), nullable=False, unique=True, comment="盘点批次号")
    org_id = Column(String(30), nullable=False, comment="组织ID")
    store_code = Column(String(20), nullable=False, comment="店铺代码")
    inventory_date = Column(Date, comment="盘点日期")
    inventory_method = Column(NVARCHAR(30), nullable=False, default="self",
                              comment="盘点方式: self=店舗棚卸, external=外部棚卸会社")
    inventory_type = Column(String(20), nullable=False, default="full")
    status = Column(String(20), nullable=False, default="pending",
                    comment="任务状态: pending/in_progress/submitted/confirmed/completed/cancelled")

    difference_date = Column(Date, comment="差异日期")
    frozen_qty = Column(Integer, default=0, comment="冻结数量(系统库存)")
    Inventory_qty = Column(Integer, default=0, comment="盘点数量(实际盘点)")
    difference_qty = Column(Integer, default=0, comment="差异数量")
    export_count = Column(Integer, default=0, comment="导出次数")
    confirm_time = Column(DateTime, comment="差异确认时间")
    confirm_user = Column(NVARCHAR(100), comment="差异确认人")
    business_owner = Column(NVARCHAR(100), comment="業務担当者")
    remark = Column(Text, comment="备注")
    create_time = Column(DateTime, default=datetime.now)
    create_user = Column(String(50), nullable=False, comment="制单人")
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    update_user = Column(String(50), comment="更新人")

    __table_args__ = (
        Index("idx_task_org_date", "org_id", "inventory_date"),  # 修复大小写
        Index("idx_task_status", "status"),
        Index("idx_task_store", "store_code"),
    )


class InventoryOrder(Base):
    """
    盘点单表 - 对应「盘点列表」菜单
    一个盘点任务下有多个盘点单，每个盘点单记录一个库位的盘点数据
    页面字段: order_no, order_type, Inventory_date, store_code, store_name,
             warehouse_code, create_user, total_quantity, total_amount
    """
    __tablename__ = "inventory_order"

    order_no = Column(String(50), primary_key=True, comment="盘点单号，如 INV-20260610-001")
    task_id = Column(BigInteger, ForeignKey("inventory_task.task_id"), nullable=False, comment="所属盘点任务ID")
    org_id = Column(String(30), comment="组织ID")
    store_code = Column(String(20), nullable=False, comment="店铺代码")
    zone_code = Column(NVARCHAR(40), comment="货位")
    inventory_date = Column(Date, nullable=False, comment="盘点日期")
    status = Column(String(20), nullable=False, default="in_progress",
                    comment="状态: pending/in_progress/completed/confirmed")
    is_red = Column(Boolean, default=False, comment="是否红盘")
    red_dimension = Column(String(20), comment="红盘维度: sku=SKU级别, location=货位级别")
    data_source = Column(String(20), nullable=False, default="manual",
                         comment="数据来源: manual=手工录入, imported=导入")
    import_file_name = Column(String(200), comment="导入文件名")
    create_user = Column(String(50), nullable=False, comment="制单人")
    create_time = Column(DateTime, default=datetime.utcnow, comment="制单时间")
    confirm_user = Column(String(50), comment="确认人")
    confirm_time = Column(DateTime, comment="确认时间")
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    update_user = Column(String(50), comment="更新人")
    remark = Column(Text, comment="备注")

    __table_args__ = (
        Index("idx_order_task", "task_id"),
        Index("idx_order_org_date", "org_id", "inventory_date"),
        Index("idx_order_store", "store_code"),
        Index("idx_order_status", "status"),
    )


class InventoryItem(Base):
    """
    盘点明细表 - 盘点单中的商品明细行
    页面字段(任务列表明细): product_code, product_name, color, size, quantity, price
    """
    __tablename__ = "inventory_item"
    order_no = Column(String(50), ForeignKey("inventory_order.order_no"),
                      primary_key=True, comment="所属盘点单号")
    item_id = Column(String(60), primary_key=True, comment="商品SKU代码")
    upc = Column(String(60), nullable=False, comment="商品UPC")
    price = Column(Numeric(12, 2), default=0, comment="商品单价")
    inventory_qty = Column(Integer, default=1, comment="盘点数量")
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index("idx_item_product", "item_id"),
    )


# ============================================================
#  菜单3: 差异列表 (Difference List)
#  页面字段: batch_no, difference_date, store_code, store_name,
#           create_user, frozen_total, Inventory_total,
#           difference_total, export_count, status
# ============================================================


class InventoryDetail(Base):
    """
    盘点差异明细表 - 差异列表中每个商品的差异详情
    对应差异明细页面字段: item_id, item_name, item_department, item_description, item_price
    """
    __tablename__ = "inventory_detail"

    task_id = Column(BigInteger, ForeignKey("inventory_task.task_id"),
                     primary_key=True, comment="所属盘点任务ID")
    item_id = Column(String(60), primary_key=True, comment="商品SKU代码")
    upc = Column(String(60), nullable=False, comment="商品UPC")
    batch = Column(String(60), nullable=False, comment="批次")
    material = Column(String(60), nullable=False, comment="物料代码")
    grid = Column(String(60), nullable=False, comment="格子代码")
    price = Column(Numeric(12, 2), default=0, comment="商品单价")
    frozen_qty = Column(Integer, default=0, comment="冻结数量(系统库存)")
    Inventory_qty = Column(Integer, default=0, comment="盘点数量(实际盘点)")
    difference_qty = Column(Integer, default=0, comment="差异数量")
    difference_amt = Column(Numeric(14, 2), default=0, comment="差异金额 = 差异数量 × 单价")
    create_time = Column(DateTime, default=datetime.now)
    update_time = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index("idx_diff_detail_product", "item_id"),
    )




class InventorySnapshotDetail(Base):
    """
    库存快照明细表 - 快照中每个SKU的冻结库存
    """
    __tablename__ = "inventory_snapshot"

    # snapshot_id = Column(BigInteger, ForeignKey("inventory_snapshot_header.snapshot_id"),
    #                      primary_key=True, comment="所属快照ID")
    task_id = Column(BigInteger, ForeignKey("inventory_task.task_id"), primary_key=True, comment="所属盘点任务ID")
    inventory_date = Column(Date, comment="盘点日期")
    item_id = Column(String(60), primary_key=True, comment="商品SKU代码")
    upc = Column(String(60), comment="商品UPC")
    price = Column(Numeric(12, 2), default=0, comment="商品单价")
    material = Column(String(60), nullable=False, comment="物料代码")
    grid = Column(String(60), nullable=False, comment="格子代码")
    batch = Column(String(60), nullable=False, comment="批次")
    stock_category = Column(String(60), nullable=False, comment="库存类别")
    unit = Column(String(20), nullable=False, default="PC", comment="单位(PC=件)")
    type = Column(Integer, nullable=False, default=1, comment="库存类型(1=unrestricted,2=quality,4=blocked)")
    frozen_qty = Column(Integer, default=0, comment="冻结库存数量")
    # frozen_amt = Column(Numeric(14, 2), default=0, comment="冻结库存金额 = 数量 × 单价")
    create_time = Column(DateTime, default=datetime.now)
    create_user = Column(String(30), comment="创建人")
    update_time = Column(DateTime, default=datetime.now)
    update_user = Column(String(30), comment="更新人")

    __table_args__ = (
        Index("idx_snapshot_detail_task", "task_id"),
        Index("idx_snapshot_detail_product", "item_id"),
    )


class InventoryNextSequence(Base):
    __tablename__ = 'inventory_next_seq'
    sequence_type = Column(String(30), primary_key=True)
    next_sequence = Column(Integer)
    create_time = Column(DateTime)
    create_user = Column(String(30))
    update_time = Column(DateTime)
    update_user = Column(String(30))
    __table_args__ = (
        Index('idx_next_sequence_type', 'sequence_type'),
    )
