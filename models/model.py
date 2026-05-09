from sqlalchemy import Column, Integer, String, ForeignKey, DATETIME, TIME, DECIMAL, Text

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy import Index

Base = declarative_base()


class Segment_Condition(Base):
    __tablename__ = 'segments_condition'
    condition_id = Column(Integer, primary_key=True)
    condition_type = Column(String(30))
    condition_name = Column(String(30))
    condition_value = Column(String(30))
    create_time = Column(DATETIME)


class Promotion(Base):
    __tablename__ = 'promotions'
    org_id = Column(String(30))
    promotion_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(80), index=True)
    description = Column(NVARCHAR(120))
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    iteration_cap = Column(Integer)
    class_id = Column(String(10))
    subclass_id = Column(String(30))
    promotion_group = Column(Integer)
    promotion_level = Column(Integer)
    promotion_type = Column(String(30))
    coupon_code = Column(String(30))
    promotion_status = Column(String(30))
    price_tag = Column(Integer)
    stackable = Column(Integer)
    last_export_time = Column(DATETIME)
    last_session_id = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))
    __table_args__ = (
        Index('idx_promotion_name', 'name'),
        Index('idx_promotion_promotion_status', 'promotion_status'),
        Index('idx_promotion_create_time', 'create_time', postgresql_ops={'create_time': 'DESC'}),
        Index('idx_promotion_last_session_id', 'last_session_id'),
        Index('idx_promotion_dates', 'start_date', 'end_date'),
    )


class PromotionCondition(Base):
    __tablename__ = 'promotions_condition'
    promotion_id = Column(Integer, primary_key=True)
    set_id = Column(Integer, primary_key=True)
    condition_type = Column(String(30))
    threshold_style = Column(String(30))
    MinQty = Column(Integer)
    MaxQty = Column(Integer)
    MinItemTotal = Column(DECIMAL(12, 2))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_condition_promotion_id', 'promotion_id'),
        Index('idx_promotion_condition_set_id', 'set_id'),
    )


class PromotionResult(Base):
    __tablename__ = 'promotions_result'
    promotion_id = Column(Integer, primary_key=True)
    set_id = Column(Integer, primary_key=True)
    apply_type = Column(String(30))
    overlap = Column(Integer, comment="是否叠加")
    discount_type = Column(String(30))
    action_qty = Column(Integer)
    discount_value = Column(DECIMAL(12, 2))
    is_active = Column(Integer, default=1, comment="状态，1表示开启，0表示关闭")
    create_time = Column(DATETIME)
    create_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_result_promotion_id', 'promotion_id'),
        Index('idx_promotion_result_set_id', 'set_id'),
    )


class PromotionCustomerSegments(Base):
    __tablename__ = 'promotions_customer_segments'
    promotion_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, primary_key=True)
    include = Column(Integer, comment="是否包含")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class PromotionItemSegments(Base):
    __tablename__ = 'promotions_item_segments'
    promotion_id = Column(Integer, primary_key=True)
    set_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, primary_key=True)
    item_type = Column(String(30), primary_key=True, comment="条件/结果")
    include = Column(Integer, comment="是否包含")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_item_segments_promotion_id', 'promotion_id'),
        Index('idx_promotion_item_segments_segment_id', 'segment_id'),
        Index('idx_promotion_item_segments_set_id', 'set_id'),
    )


class PromotionLocationSegments(Base):
    __tablename__ = 'promotions_location_segments'
    promotion_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, primary_key=True)
    include = Column(Integer, comment="是否包含")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_location_segments_promotion_id', 'promotion_id'),
        Index('idx_promotion_location_segments_segment_id', 'segment_id'),
    )


class PromotionOrgJoin(Base):
    __tablename__ = 'promotions_org_join'
    promotion_id = Column(Integer, primary_key=True)
    org_code = Column(String(30), primary_key=True)
    org_value = Column(String(60), primary_key=True)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_org_join_promotion_id', 'promotion_id'),
        Index('idx_promotion_org_join_org_code_value', 'org_code', 'org_value'),
    )

class PromotionAttributes(Base):
    __tablename__ = 'promotions_attributes'
    promotion_id = Column(Integer, primary_key=True)
    attribute_code = Column(String(60), primary_key=True)
    attribute_value = Column(String(255))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_promotion_attributes_promotion_id', 'promotion_id'),
        Index('idx_promotion_attributes_attribute_code', 'attribute_code'),
    )

class PromotionImport(Base):
    __tablename__ = 'promotions_import'
    promotion_id = Column(Integer, primary_key=True)
    file_name = Column(String(180))
    count_success = Column(Integer)
    count_fail = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsCustomer(Base):
    __tablename__ = 'segments_customers'
    org_id = Column(String(30))
    segment_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(30))
    description = Column(NVARCHAR(60))
    segment_status = Column(String(30), comment="客户标签状态")
    condition_type = Column(String(30))
    create_type = Column(String(30), comment="创建类型")
    public = Column(Integer, comment="是否公开", default=0)
    export = Column(Integer, comment="是否导出", default=0)
    sub_count = Column(Integer, comment="标签数量", default=0)
    last_export_time = Column(DATETIME)
    last_run_time = Column(DATETIME)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsCustomerCondition(Base):
    __tablename__ = 'segments_customers_condition'
    segment_id = Column(Integer, primary_key=True)
    condition_name = Column(String(60), primary_key=True)
    condition_type = Column(String(30))
    condition_value = Column(String(255))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsItem(Base):
    __tablename__ = 'segments_items'
    org_id = Column(String(30))
    segment_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(30))
    description = Column(NVARCHAR(60))
    segment_status = Column(String(30), comment="商品标签状态")
    condition_type = Column(String(30))
    create_type = Column(String(30), comment="创建类型")
    public = Column(Integer, comment="是否公开", default=1)
    export = Column(Integer, comment="是否导出", default=0)
    sub_count = Column(Integer, comment="标签数量", default=0)
    last_export_time = Column(DATETIME)
    last_session_id = Column(Integer)
    last_run_time = Column(DATETIME)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_segments_item_name', 'name'),
        Index('idx_segments_item_segment_status', 'segment_status'),
        Index('idx_segments_item_last_session_id', 'last_session_id'),
        Index('idx_segments_item_public', 'public'),
    )


class SegmentsItemCondition(Base):
    __tablename__ = 'segments_Item_condition'
    segment_id = Column(Integer, primary_key=True)
    condition_name = Column(String(60), primary_key=True)
    condition_type = Column(String(30))
    condition_value = Column(String(255))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_segments_item_condition_segment_id', 'segment_id'),
        Index('idx_segments_item_condition_name_type', 'condition_name', 'condition_type'),
    )


class SegmentsLocation(Base):
    __tablename__ = 'segments_locations'
    org_id = Column(String(30))
    segment_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(30))
    description = Column(NVARCHAR(60))
    segment_status = Column(String(30), comment="门店标签状态")
    condition_type = Column(String(30))
    create_type = Column(String(30), comment="创建类型")
    public = Column(Integer, comment="是否公开", default=0)
    export = Column(Integer, comment="是否导出", default=0)
    sub_count = Column(Integer, comment="标签数量", default=0)
    last_export_time = Column(DATETIME)
    last_run_time = Column(DATETIME)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsLocationCondition(Base):
    __tablename__ = 'segments_locations_condition'
    segment_id = Column(Integer, primary_key=True)
    condition_name = Column(String(60), primary_key=True)
    condition_type = Column(String(30))
    condition_value = Column(String(255))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsItemDetail(Base):
    __tablename__ = 'segments_item_detail'
    segment_id = Column(Integer, primary_key=True)
    item_id = Column(String(60), primary_key=True)
    item_name = Column(String(255))
    sku = Column(String())
    item_description = Column(String(255))
    item_department = Column(String(30))
    item_class = Column(String(30))
    item_sub_class = Column(String(30))
    item_price = Column(DECIMAL(10, 2))
    error_flag = Column(Integer,
                        default=0)
    create_type = Column(String(30))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_segments_item_detail_segment_id', 'segment_id'),
        Index('idx_segments_item_detail_item_id', 'item_id'),
    )


class SegmentsLocationDetail(Base):
    __tablename__ = 'segments_location_detail'
    segment_id = Column(Integer, primary_key=True)
    rtl_loc_id = Column(Integer, primary_key=True)
    store_name = Column(String(60))
    city = Column(String(30))
    location_type = Column(String(30))
    error_flag = Column(Integer, default=0)
    create_type = Column(String(30))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class PromotionNextSequence(Base):
    __tablename__ = 'promotion_next_seq'
    sequence_type = Column(String(30), primary_key=True)
    next_sequence = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))
    __table_args__ = (
        Index('idx_promotion_next_sequence_type', 'sequence_type'),
    )


class SegmentsCustomerDetail(Base):
    __tablename__ = 'segments_customer_detail'
    segment_id = Column(Integer, primary_key=True)
    party_id = Column(String(60))
    first_name = Column(String(60))
    last_name = Column(String(60))
    cust_phone = Column(String(60), primary_key=True)
    cust_email = Column(String(60))
    cust_sex = Column(String(30))
    cust_birthday = Column(String(30))
    sign_up_rtl_loc_id = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsImport(Base):
    __tablename__ = 'segments_import'
    segment_id = Column(Integer, primary_key=True)
    segment_type = Column(String(30), primary_key=True)
    file_name = Column(String(180))
    count_success = Column(Integer)
    count_fail = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsSchedule(Base):
    __tablename__ = 'segments_schedule'
    segment_id = Column(Integer, primary_key=True)
    segment_type = Column(String(30), primary_key=True)
    schedule_type = Column(String(30))
    schedule_value = Column(Integer)
    schedule_time = Column(TIME)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class WorkerTask(Base):
    __tablename__ = 'worker_task'
    location_id = Column(Integer, primary_key=True)
    terminal_id = Column(Integer, primary_key=True)
    session_id = Column(Integer, primary_key=True)
    priority = Column(Integer, default=0)
    data_type = Column(String(30))
    data_key = Column(String(30))
    data_seq = Column(Integer, default=1)
    status = Column(String(30))
    retry_count = Column(Integer, default=0)
    msg = Column(Text)
    termination = Column(Integer, default=0)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_worker_task_session_id', 'session_id'),
        Index('idx_worker_task_status', 'status'),
        Index('idx_worker_task_data_type_key', 'data_type', 'data_key'),
        Index('idx_worker_task_location_id', 'location_id'),
    )


class WorkerTerminal(Base):
    __tablename__ = 'worker_terminal'
    location_id = Column(Integer, primary_key=True)
    terminal_id = Column(Integer, primary_key=True)
    active = Column(Integer, default=1, comment="激活状态")  # 1表示启用，0表示禁用
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysUser(Base):
    __tablename__ = 'sys_user'
    user_code = Column(String(60), primary_key=True)
    user_name = Column(String(60))
    user_password = Column(String(60))
    user_status = Column(String(30))
    user_email = Column(String(60))
    language = Column(String(30))
    configuration = Column(Integer, default=0)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysRole(Base):
    __tablename__ = 'sys_role'
    org_id = Column(String(30))
    role_code = Column(String(60), primary_key=True)
    role_description = Column(String(120))
    role_status = Column(String(30))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysUserRole(Base):
    __tablename__ = 'sys_user_role'
    user_code = Column(String(60), primary_key=True)
    role_code = Column(String(60), primary_key=True)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysMenu(Base):
    __tablename__ = 'sys_menu'
    parent_code = Column(String(30))
    menu_code = Column(String(30), primary_key=True)
    menu_name = Column(String(30))
    menu_url = Column(String(255))
    menu_icon = Column(String(255))


class SysMenuPermission(Base):
    __tablename__ = 'sys_menu_permission'
    menu_code = Column(String(30), primary_key=True)
    permission_type = Column(String(50), primary_key=True)  # 查询/编辑/删除/导出等权限类型
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysRoleMenuPermission(Base):
    __tablename__ = 'sys_role_menu_permission'
    role_code = Column(String(60), primary_key=True)
    menu_code = Column(String(30), primary_key=True)
    permission_type = Column(String(50), primary_key=True)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysRoleOrgPermission(Base):
    __tablename__ = 'sys_role_org_permission'
    role_code = Column(String(60), primary_key=True)
    org_code = Column(String(30), primary_key=True)
    org_value = Column(String(60), primary_key=True)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class LOC_ORG_HIERARCHY(Base):
    __tablename__ = 'LOC_ORG_HIERARCHY'
    ORG_CODE = Column(String(30), primary_key=True)
    ORG_VALUE = Column(String(60), primary_key=True)
    DESCRIPTION = Column(String(255))
    PARENT_CODE = Column(String(30))
    PARENT_VALUE = Column(String(60))


class WorkerTaskDataDispatch(Base):
    __tablename__ = 'worker_task_data_dispatch'
    dispatch_id = Column(Integer, primary_key=True, autoincrement=True, comment="下发记录ID")
    table_name = Column(String(100), nullable=False, comment="目标表名")
    table_key = Column(String(500), nullable=False, comment="表的主键字段，逗号分隔")
    action = Column(String(30), nullable=False, comment="下发动作：update, delete, insert")
    data_content = Column(Text, comment="数据内容，JSON格式的列表")
    record_count = Column(Integer, default=0, comment="数据记录数量")
    status = Column(String(30), default='active', comment="状态：active-开启, inactive-关闭")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))
