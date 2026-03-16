# utils/path_utils.py
import sys
import os
from pathlib import Path


def get_config_dir() -> Path:
    """获取配置目录路径"""
    # 首先检查环境变量
    if 'CONFIG_PATH' in os.environ:
        return Path(os.environ['CONFIG_PATH'])

    if getattr(sys, 'frozen', False):
        # 运行在打包的可执行文件中
        return Path(sys.executable).parent / 'config'
    else:
        # 运行在源代码环境中
        return Path(__file__).parent.parent / 'config'


def get_config_path(filename: str) -> Path:
    """获取指定配置文件的完整路径"""
    config_dir = get_config_dir()
    file_path = config_dir / filename

    # 如果文件不存在，尝试在当前工作目录下查找
    if not file_path.exists():
        cwd_config = Path.cwd() / 'config' / filename
        if cwd_config.exists():
            return cwd_config

    return file_path


def get_absolute_config_path(filename: str) -> Path:
    """获取配置文件的绝对路径"""
    return get_config_path(filename).resolve()
