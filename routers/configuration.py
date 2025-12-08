
from fastapi import APIRouter, Depends
from utils.logger import app_logger
from utils.config_manager import config_manager

router = APIRouter(prefix="/configuration", tags=["configuration"])


CONFIG_FILE_PATH = "config/config_template.yaml"

@router.get("/configuration")
async def get_configuration_json():
    """
    获取配置文件内容（JSON格式）
    """
    try:
        config_data = config_manager.get_config().copy()
        return {"code": 200, "data": config_data}
    except Exception as e:
        app_logger.error(f"Error reading config file: {str(e)}")
        return {"code": 301, "msg": "Failed to read configuration file"}


@router.post("/configuration")
async def update_configuration_json(config_data: dict):
    """
    更新配置文件（JSON格式）
    """
    try:
        config_manager.update_config(config_data)
        return {"code": 200, "msg": "Configuration updated successfully"}
    except Exception as e:
        app_logger.error(f"Error updating config file: {str(e)}")
        return {"code": 301, "msg": "Failed to update configuration file"}
