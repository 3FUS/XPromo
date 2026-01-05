
import yaml
from utils.config_manager import config_manager
from utils.logger import app_logger
import os


class AppConfig:
    def __init__(self):
        self.dict_config = {}
        self.directory = ""
        self.Export_Type = ""
        self.PROMOTION_TABLES = []
        self.template_config = {}
        self.load_config()

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
            os.makedirs(self.directory, exist_ok=True)

            # 设置其他配置项
            self.Export_Type = self.dict_config['Export_Type']
            self.PROMOTION_TABLES = self.dict_config['PROMOTION_TABLES']

        except Exception as e:
            app_logger.error(f"Failed to load config: {str(e)}")
            raise

    def get_config(self):
        """获取配置字典"""
        return self.dict_config

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
