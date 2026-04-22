import datetime
from models.sam_competitor import CompetitorBrand,SamCompetitorSales
from utils.logger import app_logger
from sqlalchemy.orm import Session
from schemas.competitor_sales import CompetitorSalesCreate
from sqlalchemy import delete

async def create_competitor_sale(session: Session, sale_data: CompetitorSalesCreate):
    """
    创建单条竞争品牌销售记录
    """
    try:
        location_id = sale_data.store_code
        sale_date = sale_data.sale_date

        # 先删除已存在的location_id和sale_date对应的所有数据
        stmt = delete(SamCompetitorSales).where(
            SamCompetitorSales.location_id == location_id,
            SamCompetitorSales.sale_date == sale_date
        )
        session.execute(stmt)

        # 批量创建新的销售记录
        created_records = []
        for brand_data in sale_data.competitor_brands:
            new_sale = SamCompetitorSales(
                location_id=location_id,
                sale_date=sale_date,
                competitor_brand=brand_data.competitor_brand,
                sales_amount=brand_data.sales_amount,
                reporter=sale_data.reporter,
                report_time=datetime.datetime.now()
            )

            session.add(new_sale)
            created_records.append(new_sale)

        session.commit()

        app_logger.info(f"Created {len(created_records)} competitor sale records for location_id={location_id}, "
                        f"sale_date={sale_date}")

        return created_records
    except Exception as e:
        session.rollback()
        app_logger.error(f"Error creating competitor sales: {str(e)}")
        raise e


async def get_competitor_brands(session: Session):
    """
    从数据库中获取启用的竞争品牌清单

    Args:
        session: 数据库会话

    Returns:
        list: 竞争品牌名称列表
    """
    try:
        brands = session.query(CompetitorBrand.brand_name).filter(
            CompetitorBrand.brand_status == 'active'
        ).order_by(
            CompetitorBrand.sort_order.asc(),
            CompetitorBrand.brand_id.asc()
        ).all()

        brand_list = [brand[0] for brand in brands]
        app_logger.info(f"Retrieved {len(brand_list)} competitor brands from database")

        return brand_list
    except Exception as e:
        app_logger.error(f"Error retrieving competitor brands from database: {str(e)}")
        raise e
