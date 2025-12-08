import yaml
from typing import Dict, Any, Callable
from pathlib import Path


class ConfigManager:
    def __init__(self, config_path: str = "config/config_template.yaml"):
        self.config_path = config_path
        self.config_data: Dict[str, Any] = {}
        self.callbacks: list[Callable] = []
        self.load_config()

    def load_config(self):
        """加载配置文件"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as file:
                self.config_data = yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Failed to load config: {str(e)}")

    def get_config(self) -> Dict[str, Any]:
        """获取当前配置"""
        return self.config_data

    def update_config(self, new_config: Dict[str, Any]):
        """更新配置并保存到文件"""
        try:
            # 保存到文件
            with open(self.config_path, 'w', encoding='utf-8') as file:
                yaml.dump(new_config, file, allow_unicode=True, default_flow_style=False, indent=2)

            # 更新内存中的配置
            self.config_data = new_config

            # 通知所有回调函数
            self._notify_callbacks()
        except Exception as e:
            raise Exception(f"Failed to update config: {str(e)}")

    def add_callback(self, callback: Callable):
        """添加配置更新回调函数"""
        self.callbacks.append(callback)

    def _notify_callbacks(self):
        """通知所有回调函数配置已更新"""
        for callback in self.callbacks:
            try:
                callback()
            except Exception as e:
                print(f"Error in config change callback: {e}")


# 创建全局实例
config_manager = ConfigManager()
