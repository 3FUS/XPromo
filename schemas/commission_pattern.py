from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List
from decimal import Decimal


class CommissionPatternBase(BaseModel):
    """佣金模式基础模型"""
    location_id: int = Field(..., description="店铺代码")
    brand_code: str = Field(..., description="品牌CODE")
    category_code: str = Field(..., description="销售区域CODE")
    start_date: datetime = Field(..., description="开始时间")
    end_date: datetime = Field(..., description="结束时间")
    p_value: Optional[Decimal] = Field(None, description="p值", ge=0)
    s_value: Optional[Decimal] = Field(None, description="s值", ge=0)
    status: str = Field("active", description="状态：active-启用, inactive-禁用")


class CommissionPatternCreate(CommissionPatternBase):
    """创建佣金模式请求模型"""
    create_user: Optional[str] = Field(..., description="创建用户")


class CommissionPatternUpdate(BaseModel):
    """更新佣金模式请求模型"""
    location_id: Optional[int] = Field(None, description="店铺代码")
    brand_code: Optional[str] = Field(None, description="品牌CODE")
    category_code: Optional[str] = Field(None, description="销售区域CODE")
    start_date: Optional[datetime] = Field(None, description="开始时间")
    end_date: Optional[datetime] = Field(None, description="结束时间")
    p_value: Optional[Decimal] = Field(None, description="p值", ge=0)
    s_value: Optional[Decimal] = Field(None, description="s值", ge=0)
    status: Optional[str] = Field(None, description="状态：active-启用, inactive-禁用")
    update_user: Optional[str] = Field(..., description="更新用户")


class CommissionPatternResponse(CommissionPatternBase):
    """佣金模式响应模型"""
    commission_pattern_id: int = Field(..., description="佣金模式ID")
    create_time: Optional[datetime] = Field(None, description="创建时间")
    create_user: Optional[str] = Field(None, description="创建用户")
    update_time: Optional[datetime] = Field(None, description="更新时间")
    update_user: Optional[str] = Field(None, description="更新用户")

    class Config:
        from_attributes = True


class CommissionPatternCategoryBase(BaseModel):
    """佣金模式分类基础模型"""
    category_code: str = Field(..., description="销售区域CODE")
    category_name: str = Field(..., description="销售区域名称", max_length=120)
    status: str = Field("active", description="状态：active-启用, inactive-禁用")
    sort_order: int = Field(0, description="排序顺序")


class CommissionPatternCategoryCreate(CommissionPatternCategoryBase):
    """创建佣金模式分类请求模型"""
    create_user: str = Field(..., description="创建用户")



class CommissionPatternCategoryResponse(CommissionPatternCategoryBase):
    """佣金模式分类响应模型"""
    create_time: Optional[datetime] = Field(None, description="创建时间")
    create_user: Optional[str] = Field(None, description="创建用户")

    class Config:
        from_attributes = True


class CommissionPatternBrandBase(BaseModel):
    """佣金模式品牌基础模型"""
    brand_code: str = Field(..., description="品牌CODE")
    brand_name: str = Field(..., description="品牌名称", max_length=120)
    status: str = Field("active", description="状态：active-启用, inactive-禁用")
    sort_order: int = Field(0, description="排序顺序")


class CommissionPatternBrandCreate(CommissionPatternBrandBase):
    """创建佣金模式品牌请求模型"""
    create_user: str = Field(..., description="创建用户")


class CommissionPatternBrandResponse(CommissionPatternBrandBase):
    """佣金模式品牌响应模型"""
    create_time: Optional[datetime] = Field(None, description="创建时间")
    create_user: Optional[str] = Field(None, description="创建用户")

    class Config:
        from_attributes = True


class CommissionPatternQueryParams(BaseModel):
    """佣金模式查询参数"""
    key_word: Optional[str] = Field(None, description="关键字，用于模糊查询location_id、brand_code、category_code")
    status: Optional[str] = Field(None, description="状态筛选")
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(30, ge=1, le=1000, description="每页数量")


class CommissionPatternListResponse(BaseModel):
    """佣金模式列表响应"""
    total: int = Field(..., description="总数")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页数量")
    data: List[CommissionPatternResponse] = Field(..., description="数据列表")
