from sqlalchemy import Column, Integer, String, ForeignKey, DATETIME, TIME, DECIMAL, Text

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mssql import NVARCHAR

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
    promotion_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(30), index=True)
    description = Column(NVARCHAR(60))
    start_date = Column(DATETIME)
    end_date = Column(DATETIME)
    iteration_cap = Column(Integer)
    class_id = Column(Integer)
    subclass_id = Column(Integer, default=0)
    promotion_group = Column(Integer)
    promotion_level = Column(Integer)
    promotion_type = Column(String(30))
    coupon_code = Column(String(30))
    promotion_status = Column(String(30))
    last_export_time = Column(DATETIME)
    last_session_id = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


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


class PromotionResult(Base):
    __tablename__ = 'promotions_result'
    promotion_id = Column(Integer, primary_key=True)
    set_id = Column(Integer, primary_key=True)
    apply_type = Column(String(30))
    overlap = Column(Integer, comment="是否叠加")
    discount_type = Column(String(30))
    action_qty = Column(Integer)
    discount_value = Column(DECIMAL(12, 2))
    create_time = Column(DATETIME)
    create_user = Column(String(30))


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


class PromotionLocationSegments(Base):
    __tablename__ = 'promotions_location_segments'
    promotion_id = Column(Integer, primary_key=True)
    segment_id = Column(Integer, primary_key=True)
    include = Column(Integer, comment="是否包含")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class PromotionOrgJoin(Base):
    __tablename__ = 'promotions_org_join'
    promotion_id = Column(Integer, primary_key=True)
    org_code = Column(String(30), primary_key=True)
    org_value = Column(String(60), primary_key=True)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsCustomer(Base):
    __tablename__ = 'segments_customers'
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
    segment_id = Column(Integer, primary_key=True)
    name = Column(NVARCHAR(30))
    description = Column(NVARCHAR(60))
    segment_status = Column(String(30), comment="商品标签状态")
    condition_type = Column(String(30))
    create_type = Column(String(30), comment="创建类型")
    public = Column(Integer, comment="是否公开", default=0)
    export = Column(Integer, comment="是否导出", default=0)
    sub_count = Column(Integer, comment="标签数量", default=0)
    last_export_time = Column(DATETIME)
    last_session_id = Column(Integer)
    last_run_time = Column(DATETIME)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


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


class SegmentsLocation(Base):
    __tablename__ = 'segments_locations'
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
    item_name = Column(String(60))
    item_description = Column(String(255))
    item_department = Column(String(30))
    item_class = Column(String(30))
    item_sub_class = Column(String(30))
    item_price = Column(DECIMAL(10, 2))
    create_type = Column(String(30))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SegmentsLocationDetail(Base):
    __tablename__ = 'segments_location_detail'
    segment_id = Column(Integer, primary_key=True)
    rtl_loc_id = Column(Integer, primary_key=True)
    store_name = Column(String(60))
    city = Column(String(30))
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
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysUser(Base):
    __tablename__ = 'sys_user'
    user_code = Column(String(60), primary_key=True)
    user_name = Column(String(60), unique=True)
    user_password = Column(String(60))
    user_status = Column(String(30))
    user_email = Column(String(60))
    language = Column(String(30))
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))


class SysRole(Base):
    __tablename__ = 'sys_role'
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
    PARENT_CODE = Column(String(30))
    PARENT_VALUE = Column(String(60))
