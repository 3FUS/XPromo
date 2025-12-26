# utils/path_utils.py
import sys
from pathlib import Path

def get_config_dir() -> Path:
    """获取配置目录路径"""
    if getattr(sys, 'frozen', False):
        # 运行在打包的可执行文件中
        return Path(sys.executable).parent / 'config'
    else:
        # 运行在源代码环境中
        return Path(__file__).parent.parent / 'config'

def get_config_path(filename: str) -> Path:
    """获取指定配置文件的完整路径"""
    return get_config_dir() / filename
