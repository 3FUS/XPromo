from fastapi import APIRouter, Depends
from utils.logger import app_logger
from utils.config_manager import config_manager
from core.security import get_current_user

router = APIRouter(prefix="/configuration", tags=["configuration"])


@router.get("/configuration")
async def get_configuration_json(org_id: str = None, user_id=Depends(get_current_user)):
    """
    获取配置文件内容（JSON格式）
    """
    try:
        if org_id:
            config_data = config_manager.get_config(org_id).copy()
        else:
            # config_manager.load_config(CONFIG_FILE_PATH)
            config_data = config_manager.get_config().copy()
        return {"code": 200, "data": config_data}
    except Exception as e:
        app_logger.error(f"Error reading config file: {str(e)}")
        return {"code": 301, "msg": "Failed to read configuration file"}


@router.post("/configuration")
async def update_configuration_json(config_data: dict, org_id: str = None, user_id=Depends(get_current_user)):
    """
    更新配置文件（JSON格式）
    """
    try:
        config_manager.update_config(config_data, org_id)
        return {"code": 200, "msg": "Configuration updated successfully"}
    except Exception as e:
        app_logger.error(f"Error updating config file: {str(e)}")
        return {"code": 301, "msg": "Failed to update configuration file"}
