from datetime import date
from fastapi.responses import StreamingResponse
from fastapi import APIRouter, Query, Depends, UploadFile, File
from typing import Optional
import pandas as pd
import io

from service import get_db
from service.inventory_service import (
    get_inventory_task_list,
    get_inventory_task_by_id,
    create_inventory_task,
    update_inventory_task,
    get_inventory_order_list,
    get_inventory_order_by_no,
    create_inventory_order,
    update_inventory_order,
    delete_inventory_order,
    get_inventory_item_list,
    scan_item,
    update_inventory_task_business_owner,
    batch_update_inventory_items,
    delete_inventory_item,
    upload_inventory_items,
    get_inventory_items_by_task_id,
    get_order_zone_list,
    get_difference_detail_list,
    generate_order_no, upload_snapshot_details, get_task_list,
    export_inventory_difference_detail, export_inventory_sap_csv,
)
from schemas.inventory import (
    InventoryTaskCreate,
    InventoryTaskUpdate,
    InventoryOrderCreate,
    InventoryOrderUpdate,
    InventoryItemCreate,
    InventoryItemBatchUpdate,
    SnapshotHeaderCreate,
    SnapshotHeaderUpdate,
    SnapshotDetailCreate,
)
from core.security import get_current_user
from utils.logger import app_logger
from utils.translator import get_message

router = APIRouter(prefix="/inventory", tags=["inventory"])


@router.get("/scan", summary="扫码", description="盘点单页面，扫描")
async def scan_item_api(
        org_id: str = Query(..., description="组织ID"),
        scan_code: str = Query(..., description="扫码值，可以是UPC或item_id"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await scan_item(session, org_id, scan_code)
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error scanning item: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/order/next_order_no", summary="获取下一个盘点单号")
async def get_next_order_no(
        task_id: int = Query(..., description="盘点任务ID"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        order_no = generate_order_no(session, task_id)
        return {"code": 200, "data": {"order_no": order_no}}
    except Exception as e:
        app_logger.error(f"Error generating order_no: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.post("/item/upload", summary="上传盘表", description="上传盘点明细数据文件(棚番/SKU/数量/棚卸日付/棚卸場所)")
async def upload_inventory_items_api(
        uFile: UploadFile = File(None),
        preview: bool = Query(False, description="是否为预览模式"),
        org_id: str = Query(None, description="组织ID"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        if not uFile:
            return {"code": 400, "msg": get_message("please_upload_file", lang)}

        contents = await uFile.read()
        if not contents:
            return {"code": 400, "msg": get_message("uploaded_file_empty", lang)}

        try:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
        except pd.errors.ParserError as pe:
            app_logger.error(f"Error parsing Excel file: {str(pe)}")
            return {"code": 400, "msg": get_message("excel_parse_failed", lang)}
        except Exception as e:
            app_logger.error(f"Error reading Excel file: {str(e)}")
            return {"code": 400, "msg": get_message("excel_read_failed", lang)}

        if df.empty:
            return {"code": 400, "msg": get_message("excel_data_empty", lang)}

        df = df.fillna("")

        required_columns = {"棚番", "SKU", "数量", "棚卸日付", "棚卸場所"}
        missing = required_columns - set(df.columns)
        if missing:
            return {"code": 400, "msg": get_message("excel_missing_columns", lang, columns=', '.join(missing))}

        if not org_id:
            return {"code": 400, "msg": get_message("please_provide_org_id", lang)}

        if preview:
            return {"code": 200, "msg": get_message("preview_mode", lang), "data": df}

        result = await upload_inventory_items(
            session, org_id, df, user_id
        )
        if result.get("has_unmatched"):
            return {"code": 400, "msg": get_message("data_validation_failed", lang), "data": result}

        task_ids = [t.get("task_id") for t in result.get("tasks", []) if t.get("task_id")]
        for task_id in task_ids:
            await update_inventory_task(session, task_id, {"status": "confirmed"}, user_id)

        return {"code": 200, "msg": get_message("snapshot_upload_success", lang), "data": result}
    except Exception as e:
        app_logger.error(f"Error uploading inventory items: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.post("/snapshot/upload", summary="上传库存", description="上传库存数据文件")
async def upload_snapshot(
        uFile: UploadFile = File(None),
        preview: bool = Query(False, description="是否为预览模式"),
        inventory_date: Optional[date] = Query(None),
        org_id: str = Query(None),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        if not uFile:
            return {"code": 400, "msg": get_message("please_upload_file", lang)}

        contents = await uFile.read()
        if not contents:
            return {"code": 400, "msg": get_message("uploaded_file_empty", lang)}

        try:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
        except pd.errors.ParserError as pe:
            app_logger.error(f"Error parsing Excel file{str(pe)}")
            return {"code": 400, "msg": get_message("excel_parse_failed", lang)}
        except Exception as e:
            app_logger.error(f"Error reading Excel file: {str(e)}")
            return {"code": 400, "msg": get_message("excel_read_failed", lang)}

        if df.empty:
            return {"code": 400, "msg": get_message("excel_data_empty", lang)}

        df = df.fillna("")

        required_columns = {"Material", "Grid Value", "Batch", "Special stock number"}
        missing = required_columns - set(df.columns)
        if missing:
            return {"code": 400, "msg": get_message("excel_missing_columns", lang, columns=', '.join(missing))}

        if not inventory_date:
            return {"code": 400, "msg": get_message("please_provide_inventory_date", lang)}

        if preview:
            column_mapping = {
                # "Batch": "upc",
                "Special stock number": "store_code",
            }
            df_preview = df.rename(columns=column_mapping)
            grouped = df_preview.groupby("store_code")
            return {"code": 200, "msg": get_message("preview_mode", lang), "data": df_preview}

        result = await upload_snapshot_details(
            session, org_id, inventory_date, df, user_id
        )
        if result.get("has_unmatched"):
            return {"code": 400, "msg": get_message("data_validation_failed", lang), "data": result}
        return {"code": 200, "msg": get_message("snapshot_upload_success", lang), "data": result}
    except Exception as e:
        app_logger.error(f"Error creating snapshot header: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/task/list", summary="任务列表")
async def list_tasks(
        org_id: str = Query(None, description="组织ID"),
        key_word: Optional[str] = Query(None, description="关键字，模糊查询：单号/门店/创建人"),
        start_date: Optional[str] = Query(None, description="开始日期，如 2026-01-01"),
        end_date: Optional[str] = Query(None, description="结束日期，如 2026-12-31"),
        status: Optional[str] = Query(None, description="状态: in_progress/confirming/cancelled/completed"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_task_list(
            session, org_id, key_word, start_date, end_date, status, page, page_size, lang
        )
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error listing inventory tasks: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/order/list", summary='盘点列表')
async def list_inventory_orders(
        org_id: str = Query(None, description="组织ID"),
        key_word: Optional[str] = Query(None, description="关键字，模糊查询：单号/门店/创建人"),
        start_date: Optional[str] = Query(None, description="开始日期，如 2026-01-01"),
        end_date: Optional[str] = Query(None, description="结束日期，如 2026-12-31"),
        status: Optional[str] = Query(None, description="状态: in_progress/confirming/cancelled/completed"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_inventory_order_list(
            session, org_id, key_word, start_date, end_date, status, page, page_size, lang
        )
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error listing inventory orders: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/inventory/list", summary="差异列表")
async def list_inventory_tasks(
        org_id: str = Query(None, description="组织ID"),
        key_word: Optional[str] = Query(None, description="关键字，模糊查询：单号/门店/创建人"),
        start_date: Optional[str] = Query(None, description="开始日期，如 2026-01-01"),
        end_date: Optional[str] = Query(None, description="结束日期，如 2026-12-31"),
        status: Optional[str] = Query(None, description="状态: in_progress/confirming/cancelled/completed"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_inventory_task_list(
            session, org_id, key_word, start_date, end_date, status, page, page_size, lang
        )
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error listing inventory tasks: {str(e)}")
        return {"code": 500, "msg": str(e)}


#

@router.get("/inventory/detail", summary="差异列表明细")
async def list_difference_details(
        task_id: int = Query(None, description="盘点任务ID"),
        key_word: Optional[str] = Query(None, description="关键字，模糊查询：SKU/UPC"),
        only_difference: bool = Query(False, description="是否只看有差异的记录"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_difference_detail_list(session, task_id, key_word, only_difference, page, page_size, lang)
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error listing difference details: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/task/{task_id}")
async def get_inventory_task(
        task_id: int,
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        task = await get_inventory_task_by_id(session, task_id)
        if not task:
            return {"code": 404, "msg": get_message("inventory_task_not_found", lang)}
        return {"code": 200, "data": task}
    except Exception as e:
        app_logger.error(f"Error getting inventory task: {str(e)}")
        return {"code": 500, "msg": str(e)}

@router.post("/task", summary='创建任务')
async def create_new_inventory_task(
        task_data: InventoryTaskCreate,
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:

        data = task_data.model_dump()
        data["create_user"] = user_id
        new_task = await create_inventory_task(session, data)
        return {"code": 200, "msg": get_message("inventory_task_created", lang), "data": new_task}
    except ValueError as e:
        return {"code": 400, "msg": get_message(str(e), lang)}
    except Exception as e:
        app_logger.error(f"Error creating inventory task: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.put("/task/{task_id}", summary='更新任务状态')
async def update_existing_inventory_task(
        task_id: int,
        task_data: InventoryTaskUpdate,
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        updated_task = await update_inventory_task(session, task_id, task_data.model_dump(exclude_unset=True), user_id)
        if not updated_task:
            return {"code": 404, "msg": get_message("inventory_task_not_found", lang)}
        return {"code": 200, "msg": get_message("inventory_task_updated", lang), "data": updated_task}
    except Exception as e:
        app_logger.error(f"Error updating inventory task: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.get("/order/{order_no}")
async def get_inventory_order(
        order_no: str,
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        order = await get_inventory_order_by_no(session, order_no)
        if not order:
            return {"code": 404, "msg": get_message("inventory_order_not_found", lang)}
        return {"code": 200, "data": order}
    except Exception as e:
        app_logger.error(f"Error getting inventory order: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.post("/order", summary="新增盘点单-输入")
async def create_new_inventory_order(
        order_data: InventoryOrderCreate,
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        data = order_data.model_dump()
        data["create_user"] = user_id
        new_order = await create_inventory_order(session, data)
        return {"code": 200, "msg": get_message("inventory_order_created", lang), "data": new_order}
    except Exception as e:
        app_logger.error(f"Error creating inventory order: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.put("/item/save", summary='保存盘点单明细')
async def batch_update_inventory_item(
        batch_data: InventoryItemBatchUpdate,
        session=Depends(get_db),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        user_id=Depends(get_current_user)
):
    try:
        updated_items = await batch_update_inventory_items(
            session, batch_data.order_no, [item.model_dump() for item in batch_data.items]
        )
        return {"code": 200, "msg": get_message("inventory_items_batch_updated", lang), "data": updated_items}
    except Exception as e:
        app_logger.error(f"Error batch updating inventory items: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.put("/order/{order_no}", summary='更新盘点单,更新状态等', )
async def update_existing_inventory_order(
        order_no: str,
        order_data: InventoryOrderUpdate,
        session=Depends(get_db),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        user_id=Depends(get_current_user)
):
    try:
        updated_order = await update_inventory_order(session, order_no, order_data.model_dump(exclude_unset=True))
        if not updated_order:
            return {"code": 404, "msg": get_message("inventory_order_not_found", lang)}
        return {"code": 200, "msg": get_message("inventory_order_updated", lang), "data": updated_order}
    except Exception as e:
        app_logger.error(f"Error updating inventory order: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.delete("/order/{order_no}", summary='删除盘点单')
async def delete_existing_inventory_order(
        order_no: str,
        session=Depends(get_db),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        user_id=Depends(get_current_user)
):
    try:
        success = await delete_inventory_order(session, order_no)
        if not success:
            return {"code": 404, "msg": get_message("inventory_order_not_found", lang)}
        return {"code": 200, "msg": get_message("inventory_order_deleted", lang)}
    except Exception as e:
        app_logger.error(f"Error deleting inventory order: {str(e)}")
        return {"code": 500, "msg": str(e)}


# ==================== InventoryItem 路由 ====================

@router.get("/item/list", summary="盘点单明细表")
async def list_inventory_items(
        order_no: Optional[str] = Query(None, description="盘点单号"),
        key_word: Optional[str] = Query(None, description="商品SKU代码"),
        page: int = Query(1, ge=1, description="页码"),
        page_size: int = Query(30, ge=1, le=1000, description="每页数量"),
        session=Depends(get_db),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_inventory_item_list(session, order_no, key_word, page, page_size, lang)
        return {"code": 200, "data": result}
    except Exception as e:
        app_logger.error(f"Error listing inventory items: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.delete("/item/{order_no}/{item_id}", summary='删除盘点单中一个商品')
async def delete_existing_inventory_item(
        order_no: str,
        item_id: str,
        session=Depends(get_db),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        user_id=Depends(get_current_user)
):
    try:
        success = await delete_inventory_item(session, order_no, item_id)
        if not success:
            return {"code": 200, "msg": get_message("inventory_item_deleted", lang)}
        return {"code": 200, "msg": get_message("inventory_item_deleted", lang)}
    except Exception as e:
        app_logger.error(f"Error deleting inventory item: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/order/zone/list", summary="获取盘点单货位列表")
async def list_order_zones(
        org_id: str = Query(..., description="组织ID"),
        task_id: int = Query(..., description="盘点任务ID"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        zones = await get_order_zone_list(session, org_id, task_id)
        return {"code": 200, "data": zones}
    except Exception as e:
        app_logger.error(f"Error listing order zones: {str(e)}")
        return {"code": 500, "msg": str(e)}


@router.get("/task/{task_id}/items", summary="获取盘点任务下所有商品明细")
async def list_items_by_task_id(
        task_id: int,
        key_word: Optional[str] = Query(None, description="关键字，模糊查询：SKU/UPC"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        result = await get_inventory_items_by_task_id(session, task_id, key_word, lang)
        return {"code": 200, "data": result}
    except ValueError as e:
        return {"code": 404, "msg": str(e)}
    except Exception as e:
        app_logger.error(f"Error listing inventory items by task_id: {str(e)}")
        return {"code": 500, "msg": str(e)}

@router.put("/task/{task_id}/business_owner", summary="更新業務担当者")
async def update_business_owner(
        task_id: int,
        business_owner: str = Query(..., description="業務担当者"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        task = await update_inventory_task_business_owner(session, task_id, business_owner, user_id)
        if not task:
            return {"code": 404, "msg": get_message("segment_not_found", lang)}
        return {"code": 200, "msg": get_message("business_owner_updated", lang),
                "data": {"task_id": task.task_id, "business_owner": task.business_owner}}
    except Exception as e:
        app_logger.error(f"Error updating business_owner: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.get("/export/difference", summary="导出盘点差异明细Excel")
async def export_difference_detail(
        task_ids: str = Query(..., description="盘点任务ID列表，逗号分隔，如 10001,10002"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        id_list = [int(x.strip()) for x in task_ids.split(",") if x.strip()]
        if not id_list:
            return {"code": 400, "msg": get_message("task_ids_empty", lang)}
        excel_bytes = await export_inventory_difference_detail(session, id_list, lang)
        output = io.BytesIO(excel_bytes)
        today = date.today().strftime("%Y_%m_%d")
        filename = f"Variance_Report_{today}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except ValueError as e:
        return {"code": 404, "msg": str(e)}
    except Exception as e:
        app_logger.error(f"Error exporting inventory difference detail: {str(e)}")
        return {"code": 500, "msg": str(e)}



@router.get("/export/sap_csv", summary="导出SAP盘点差异CSV")
async def export_sap_csv(
        task_ids: str = Query(..., description="盘点任务ID列表，逗号分隔，如 10001,10002"),
        lang: str = Query("en", description="语言: en/zh/jp/zh_tw"),
        session=Depends(get_db),
        user_id=Depends(get_current_user)
):
    try:
        app_logger.info(f"[export/sap_csv] 请求参数: task_ids={task_ids}, user_id={user_id}")
        id_list = [int(x.strip()) for x in task_ids.split(",") if x.strip()]
        if not id_list:
            return {"code": 400, "msg": get_message("task_ids_empty", lang)}
        xlsx_bytes = await export_inventory_sap_csv(session, id_list)
        output = io.BytesIO(xlsx_bytes)
        today = date.today().strftime("%Y_%m_%d")
        filename = f"Adjustment_file_for_SAP_{today}.xlsx"
        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except ValueError as e:
        app_logger.warning(f"[export/sap_csv] ValueError: {str(e)}")
        return {"code": 404, "msg": str(e)}
    except Exception as e:
        app_logger.error(f"[export/sap_csv] 异常: {str(e)}", exc_info=True)
        return {"code": 500, "msg": str(e)}