from typing import Dict, Any

import yaml
from utils.config_manager import config_manager
from utils.logger import app_logger
import os

from utils.path_utils import get_config_path


class AppConfig:
    def __init__(self):
        self.dict_config = {}
        self.directory = ""
        self.PT_PATH = ""
        self.Export_Type = ""
        self.PROMOTION_TABLES = []
        self.template_config = {}
        self.template_config_org: Dict[str, Dict[str, Any]] = {}
        self.load_config()

        self.org_config = {}
        self.load_org_config()

    def load_config(self):
        """加载配置文件"""
        try:
            # 加载 config.yaml
            with open('config/config.yaml', 'r', encoding='utf-8') as file_config:
                self.dict_config = yaml.safe_load(file_config)

            # 加载模板配置
            self.template_config = config_manager.get_config()

            # 设置目录
            self.directory = self.dict_config['MNT_PATH']
            self.PT_PATH = self.dict_config.get('PT_PATH', './price_tag')

            os.makedirs(self.directory, exist_ok=True)

            # 设置其他配置项
            self.Export_Type = self.dict_config['Export_Type']
            self.PROMOTION_TABLES = self.dict_config['PROMOTION_TABLES']

        except Exception as e:
            app_logger.error(f"Failed to load config: {str(e)}")
            raise

    def load_org_config(self):
        """加载组织配置文件"""
        try:
            # 使用统一路径工具获取组织配置文件路径
            org_config_path = get_config_path('organization_config.yaml')
            if not org_config_path.exists():
                app_logger.warning(f"Organization config file not found: {org_config_path}")
                # 使用空配置而不是抛出异常
                self.org_config = {"organizations": [], "field_descriptions": {}}
                return

            with open(org_config_path, 'r', encoding='utf-8') as file_config:
                self.org_config = yaml.safe_load(file_config)

            for org_item in self.org_config['organizations']:
                org_id = org_item.get('org_id')
                if org_id:
                    self.template_config_org[org_id] = config_manager.get_config(org_id)
                    app_logger.info(f"Loaded organization config for {org_id}")
        except Exception as e:
            app_logger.error(f"Failed to load organization config: {str(e)}")

    def get_config(self):
        """获取配置字典"""
        return self.dict_config

    def get_org_config(self):
        """获取组织配置字典"""
        return self.org_config

    def get_column_config(self, segment_type):
        """获取指定类型的列配置"""
        column_mapping = {
            'item': 'item_column',
            'location': 'location_column',
            'customer': 'customer_column'
        }
        return self.dict_config.get(column_mapping.get(segment_type, ''), [])


# 创建全局配置实例
app_config = AppConfig()


def reload_config():
    """重新加载配置"""
    app_config.load_config()
    app_config.load_org_config()
