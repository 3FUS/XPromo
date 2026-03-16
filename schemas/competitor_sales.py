from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class CompetitorSalesBase(BaseModel):
    competitor_brand: str = Field(..., description="竞争品牌", max_length=120)
    sales_amount: float = Field(..., description="销售金额", gt=0)


class CompetitorSalesCreate(BaseModel):
    store_code: int = Field(..., description="店铺代码")
    sale_date: datetime = Field(..., description="销售日期")
    competitor_brands: List[CompetitorSalesBase]
    reporter: str = Field(..., description="提报人", max_length=60)
