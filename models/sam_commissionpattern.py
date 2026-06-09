
from sqlalchemy import Column, Integer, String, ForeignKey, DATETIME, TIME, DECIMAL, Text

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy import Index

Base = declarative_base()

class CommissionPattern(Base):
    __tablename__ = 'commission_pattern'
    commission_pattern_id = Column(Integer, primary_key=True, comment="佣金模式ID")
    location_id = Column(Integer, comment="店铺代码")
    brand_code = Column(String(30), comment="品牌CODE")
    category_code = Column(String(30), comment="销售区域CODE")
    start_date = Column(DATETIME, comment="开始时间")
    end_date = Column(DATETIME, comment="结束时间")
    p_value = Column(DECIMAL(12, 2), comment="p")
    s_value = Column(DECIMAL(12, 2), comment="s")
    status = Column(String(30), default='active', comment="状态：active-启用, inactive-禁用")
    last_export_time = Column(DATETIME)
    last_session_id = Column(Integer)
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_commission_pattern_location_brand', 'location_id', 'brand_code'),
        Index('idx_commission_pattern_category', 'category_code'),
        Index('idx_commission_pattern_dates', 'start_date', 'end_date'),
        Index('idx_commission_pattern_status', 'status'),
    )

class CommissionPatternCategory(Base):
    __tablename__ = 'commission_pattern_category'
    category_code = Column(String(30), primary_key=True, comment="销售区域CODE")
    category_name = Column(String(120), nullable=False, comment="销售区域名称")
    status = Column(String(30), default='active', comment="状态：active-启用, inactive-禁用")
    sort_order = Column(Integer, default=0, comment="排序顺序")
    create_time = Column(DATETIME)
    create_user = Column(String(30))

class CommissionPatternBrand(Base):
    __tablename__ = 'commission_pattern_brand'
    brand_code = Column(String(30), primary_key=True, comment="品牌CODE")
    brand_name= Column(String(120), nullable=False, comment="品牌名称")
    status = Column(String(30), default='active', comment="状态：active-启用, inactive-禁用")
    sort_order = Column(Integer, default=0, comment="排序顺序")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
