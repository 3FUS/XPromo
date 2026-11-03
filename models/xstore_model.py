from sqlalchemy import Column, Integer, String, ForeignKey, Text, BigInteger, Date, DateTime, Boolean, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Index
from datetime import datetime

Base = declarative_base()

class ItmItemCrossReference(Base):
    """
    商品交叉引用表 - 记录商品UPC与组织属性之间的映射关系
    """
    __tablename__ = "ITM_ITEM_CROSS_REFERENCE"

    organization_id = Column(Numeric(10, 0), primary_key=True, nullable=False, comment="组织ID")
    manufacturer_upc = Column(String(60), primary_key=True, nullable=False, comment="制造商UPC")
    org_code = Column(String(30), primary_key=True, nullable=False, comment="组织代码")
    org_value = Column(String(60), nullable=False, comment="组织值")
    item_id = Column(String(60), comment="商品SKU代码")
    manufacturer = Column(String(254), comment="制造商")
    primary_flag = Column(Numeric(1, 0), nullable=False, comment="主要标志")
    create_user_id = Column(String(256), comment="创建人")
    create_date = Column(DateTime, comment="创建时间")
    update_user_id = Column(String(256), comment="更新人")
    update_date = Column(DateTime, comment="更新时间")
    record_state = Column(String(30), comment="记录状态")

    __table_args__ = (
        Index("idx_item_cross_ref_org", "organization_id", "org_code"),
        Index("idx_item_cross_ref_item", "item_id"),
        Index("idx_item_cross_ref_upc", "manufacturer_upc"),
    )

class ItmItem(Base):
    """
    商品主数据表 - 记录商品基本信息及属性
    """
    __tablename__ = "ITM_ITEM"

    organization_id = Column(Numeric(10, 0), primary_key=True, nullable=False, comment="组织ID")
    item_id = Column(String(60), nullable=True, comment="商品SKU代码")
    org_code = Column(String(30), nullable=False, comment="组织代码")
    org_value = Column(String(60), nullable=False, comment="组织值")
    name = Column(String(254), nullable=True, comment="商品名称")
    description = Column(String(254), nullable=True, comment="商品描述")
    merch_level_1 = Column(String(60), nullable=True, comment="商品分类层级1")
    merch_level_2 = Column(String(60), nullable=True, comment="商品分类层级2")
    merch_level_3 = Column(String(60), nullable=True, comment="商品分类层级3")
    merch_level_4 = Column(String(60), nullable=True, comment="商品分类层级4")
    list_price = Column(Numeric(17, 6), nullable=True, comment="标价")
    measure_req_flag = Column(Numeric(1, 0), nullable=True, comment="需计量标志")
    item_lvcode = Column(String(30), nullable=True, comment="商品层级代码")
    parent_item_id = Column(String(60), nullable=True, comment="父商品ID")
    not_inventoried_flag = Column(Numeric(1, 0), nullable=True, comment="非库存标志")
    serialized_item_flag = Column(Numeric(1, 0), nullable=True, comment="序列号商品标志")
    item_typcode = Column(String(30), nullable=True, comment="商品类型代码")
    dtv_class_name = Column(String(254), nullable=True, comment="DTV分类名称")
    dimension_system = Column(String(60), nullable=True, comment="尺寸系统")
    disallow_matrix_display_flag = Column(Numeric(1, 0), nullable=True, comment="禁止矩阵显示标志")
    item_matrix_color = Column(String(20), nullable=True, comment="商品矩阵颜色")
    dimension1 = Column(String(60), nullable=True, comment="维度1")
    dimension2 = Column(String(60), nullable=True, comment="维度2")
    dimension3 = Column(String(60), nullable=True, comment="维度3")
    external_system = Column(String(60), nullable=True, comment="外部系统")
    create_user_id = Column(String(256), nullable=True, comment="创建人")
    create_date = Column(DateTime, nullable=True, comment="创建时间")
    update_user_id = Column(String(256), nullable=True, comment="更新人")
    update_date = Column(DateTime, nullable=True, comment="更新时间")
    record_state = Column(String(30), nullable=True, comment="记录状态")


class ItmItemPrice(Base):
    """
    商品价格表 - 记录商品在不同层级和生效时间下的价格信息
    """
    __tablename__ = "itm_item_prices"

    organization_id = Column(Integer, primary_key=True, nullable=False, comment="组织ID")
    item_id = Column(String(60), primary_key=True, nullable=False, comment="商品SKU代码")
    level_code = Column(String(30), primary_key=True, nullable=False, default="*", comment="层级代码")
    level_value = Column(String(60), primary_key=True, nullable=False, default="*", comment="层级值")
    itm_price_property_code = Column(String(60), primary_key=True, nullable=False, comment="价格属性代码")
    effective_date = Column(DateTime, primary_key=True, nullable=False, comment="生效日期")
    expiration_date = Column(DateTime, nullable=True, comment="失效日期")
    price = Column(Numeric(17, 6), nullable=False, comment="价格")
    price_qty = Column(Numeric(11, 4), primary_key=True, nullable=False, default=1, comment="价格数量")
    external_id = Column(String(60), nullable=True, comment="外部ID")
    external_system = Column(String(60), nullable=True, comment="外部系统")
    create_user_id = Column(String(256), nullable=True, comment="创建人")
    create_date = Column(DateTime, nullable=True, comment="创建时间")
    update_user_id = Column(String(256), nullable=True, comment="更新人")
    update_date = Column(DateTime, nullable=True, comment="更新时间")
    record_state = Column(String(30), nullable=True, comment="记录状态")

    __table_args__ = (
        Index("XST_ITM_ITEMPRICES_EXPR_copy1", "expiration_date"),
    )


class LocRtlLoc(Base):
    """
    门店位置表 - 记录零售门店的位置及基本信息
    """
    __tablename__ = "LOC_RTL_LOC"

    organization_id = Column(Numeric(10, 0), primary_key=True, nullable=False, comment="组织ID")
    rtl_loc_id = Column(Numeric(10, 0), primary_key=True, nullable=False, comment="零售位置ID")
    store_name = Column(String(254), nullable=True, comment="门店名称")
    address1 = Column(String(254), nullable=True, comment="地址1")
    address2 = Column(String(254), nullable=True, comment="地址2")
    address3 = Column(String(254), nullable=True, comment="地址3")
    address4 = Column(String(254), nullable=True, comment="地址4")
    city = Column(String(254), nullable=True, comment="城市")
    state = Column(String(30), nullable=True, comment="州/省")
    district = Column(String(30), nullable=True, comment="区")
    area = Column(String(30), nullable=True, comment="区域")
    postal_code = Column(String(30), nullable=True, comment="邮政编码")
    country = Column(String(2), nullable=True, comment="国家")
    neighborhood = Column(String(254), nullable=True, comment="社区")
    county = Column(String(254), nullable=True, comment="县")
    locale = Column(String(30), nullable=False, comment="语言环境")
    currency_id = Column(String(3), nullable=True, comment="货币ID")
    latitude = Column(Numeric(17, 6), nullable=True, comment="纬度")
    longitude = Column(Numeric(17, 6), nullable=True, comment="经度")
    telephone1 = Column(String(32), nullable=True, comment="电话1")
    telephone2 = Column(String(32), nullable=True, comment="电话2")
    telephone3 = Column(String(32), nullable=True, comment="电话3")
    telephone4 = Column(String(32), nullable=True, comment="电话4")
    description = Column(String(254), nullable=True, comment="描述")
    store_nbr = Column(String(254), nullable=True, comment="门店编号")
    apartment = Column(String(30), nullable=True, comment="公寓")
    store_manager = Column(String(254), nullable=True, comment="门店经理")
    email_addr = Column(String(254), nullable=True, comment="邮箱地址")
    default_tax_percentage = Column(Numeric(8, 6), nullable=True, comment="默认税率")
    location_type = Column(String(60), nullable=True, comment="位置类型")
    delivery_available_flag = Column(Numeric(1, 0), nullable=False, comment="可配送标志")
    pickup_available_flag = Column(Numeric(1, 0), nullable=False, comment="可自提标志")
    transfer_available_flag = Column(Numeric(1, 0), nullable=False, comment="可调拨标志")
    geo_code = Column(String(20), nullable=True, comment="地理编码")
    uez_flag = Column(Numeric(1, 0), nullable=False, comment="UEZ标志")
    alternate_store_nbr = Column(String(254), nullable=True, comment="备选门店编号")
    use_till_accountability_flag = Column(Numeric(1, 0), nullable=False, comment="使用出纳责任制标志")
    deposit_bank_name = Column(String(254), nullable=True, comment="存款银行名称")
    deposit_bank_account_number = Column(String(30), nullable=True, comment="存款银行账号")
    airport_code = Column(String(3), nullable=True, comment="机场代码")
    legal_entity_id = Column(String(30), nullable=True, comment="法人实体ID")
    create_user_id = Column(String(256), nullable=True, comment="创建人")
    create_date = Column(DateTime, nullable=True, comment="创建时间")
    update_user_id = Column(String(256), nullable=True, comment="更新人")
    update_date = Column(DateTime, nullable=True, comment="更新时间")
    record_state = Column(String(30), nullable=True, comment="记录状态")


class ItmItemOptions(Base):
    """
    商品选项表 - 记录商品的part_number等选项信息
    """
    __tablename__ = "ITM_ITEM_OPTIONS"

    organization_id = Column(Numeric(10, 0), primary_key=True, nullable=False)
    item_id = Column(String(60), primary_key=True, nullable=False)
    part_number = Column(String(60), nullable=True, comment="商品编号(材质-规格)")