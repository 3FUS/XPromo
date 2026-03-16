import yaml
from typing import Dict, Any, Callable, Optional
from utils.path_utils import get_config_path


class ConfigManager:
    def __init__(self, config_filename: str = 'config_template.yaml'):
        self.config_path = get_config_path(config_filename)
        self.filename = config_filename
        self.config_data: Dict[str, Any] = {}
        self.org_configs: Dict[str, Dict[str, Any]] = {}
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

    def get_config(self, org_id: Optional[str] = None) -> Dict[str, Any]:
        """
        获取配置，支持按org_id获取特定配置

        Args:
            org_id: 组织ID，如果为None则返回默认配置

        Returns:
            配置字典
        """
        if not org_id:
            return self.config_data

        # 检查是否已经缓存了该org_id的配置
        if org_id in self.org_configs:
            return self.org_configs[org_id]

        # 加载组织特定配置
        org_config = self._load_org_config(org_id)
        if org_config:
            # 缓存配置
            self.org_configs[org_id] = org_config
            return org_config
        else:
            # 如果没有找到组织特定配置，返回默认配置
            return self.config_data

    def _load_org_config(self, org_id: str) -> Optional[Dict[str, Any]]:
        """
        加载特定组织的配置文件

        Args:
            org_id: 组织ID

        Returns:
            组织配置字典，如果文件不存在则返回None
        """
        try:
            # 构造组织特定配置文件路径
            org_config_path = get_config_path(f"{org_id}/{self.filename}")

            # 检查文件是否存在
            if not org_config_path.exists():
                print(f"config_template config file not found: {org_config_path}")
                return None

            # 加载组织特定配置
            with open(org_config_path, 'r', encoding='utf-8') as file:
                org_config_data = yaml.safe_load(file)

            print(f"Loaded config_template config for org_id: {org_id}")
            return org_config_data

        except Exception as e:
            print(f"Failed to load config_template config for {org_id}: {str(e)}")
            return None

    def update_config(self, new_config: Dict[str, Any], org_id: str = None):
        """更新配置并保存到文件"""
        try:
            if org_id:
                # 更新组织特定配置
                config_path = get_config_path(f"{org_id}/{self.filename}")
                # 确保目录存在
                # config_path.parent.mkdir(parents=True, exist_ok=True)

                # 保存到文件
                with open(config_path, 'w', encoding='utf-8') as file:
                    yaml.dump(new_config, file, allow_unicode=True, default_flow_style=False, indent=2)

                # 更新缓存
                self.org_configs[org_id] = new_config
            else:
                # 更新默认配置
                with open(self.config_path, 'w', encoding='utf-8') as file:
                    yaml.dump(new_config, file, allow_unicode=True, default_flow_style=False, indent=2)
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


config_manager = ConfigManager()
