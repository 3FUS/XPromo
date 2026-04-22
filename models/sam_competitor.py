
from sqlalchemy import Column, Integer, String, ForeignKey, DATETIME, TIME, DECIMAL, Text

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mssql import NVARCHAR
from sqlalchemy import Index

Base = declarative_base()

class SamCompetitorSales(Base):
    __tablename__ = 'sam_competitorsales'
    location_id = Column(Integer, primary_key=True, comment="店铺代码")
    competitor_brand = Column(NVARCHAR(120), primary_key=True, comment="竞争品牌")
    sale_date = Column(DATETIME, primary_key=True, comment="日期")
    sales_amount = Column(DECIMAL(12, 2), comment="交易额")
    reporter = Column(String(60), comment="提报人")
    report_time = Column(DATETIME, comment="提报时间")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

class CompetitorBrand(Base):
    __tablename__ = 'competitor_brands'
    brand_id = Column(Integer, primary_key=True, autoincrement=True, comment="品牌ID")
    brand_name = Column(NVARCHAR(120), nullable=False, unique=True, comment="竞争品牌名称")
    brand_status = Column(String(30), default='active', comment="状态：active-启用, inactive-禁用")
    sort_order = Column(Integer, default=0, comment="排序顺序")
    description = Column(NVARCHAR(255), comment="品牌描述")
    create_time = Column(DATETIME)
    create_user = Column(String(30))
    update_time = Column(DATETIME)
    update_user = Column(String(30))

    __table_args__ = (
        Index('idx_competitor_brand_status', 'brand_status'),
        Index('idx_competitor_brand_sort_order', 'sort_order'),
    )