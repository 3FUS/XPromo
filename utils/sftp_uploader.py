from typing import Dict, Any

import paramiko
import os
from utils.logger import app_logger

from utils.app_config import app_config


def upload_file_to_sftp(
        hostname: str,
        port: int,
        username: str,
        password: str,
        local_path: str,
        remote_path: str
) -> bool:
    """
    通过SFTP上传文件到远程服务器

    Args:
        hostname: SFTP服务器地址
        port: SFTP服务器端口
        username: 登录用户名
        password: 登录密码
        local_path: 本地文件路径
        remote_path: 远程目标路径

    Returns:
        bool: 上传成功返回True，失败返回False
    """
    transport = None
    sftp = None

    try:
        # 创建SSH Transport对象
        transport = paramiko.Transport((hostname, port))

        # 建立连接
        transport.connect(username=username, password=password)

        # 创建SFTP客户端
        sftp = paramiko.SFTPClient.from_transport(transport)

        if not os.path.exists(local_path):
            app_logger.warning(f" {local_path} no....")
            return False

        sftp.put(local_path, remote_path)

        app_logger.info(f" {local_path} uploaded to {hostname}:{remote_path}")
        return True

    except Exception as e:
        app_logger.error(f"SFTP文件上传失败: {str(e)}", exc_info=True)
        return False

    finally:
        # 关闭连接
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def upload_mnt_file(local_path: str, filename: str) -> bool:
    remote_path = f"mnt/{filename}"
    return upload_file_to_sftp(
        '192.168.0.32',
        22,
        'jacky',
        'jacky',
        local_path,
        remote_path
    )


def get_sftp_config(config_name: str = 'DEFAULT') -> Dict[str, Any]:
    """
    从配置文件加载 SFTP 配置

    Args:
        config_name: 配置名称，如 'DEFAULT', 'MNT_UPLOAD'

    Returns:
        包含 SFTP 配置的字典
    """
    try:
        return app_config.get_sftp_config(config_name)
    except Exception as e:
        app_logger.error(f"加载 SFTP 配置失败：{str(e)}")
        return {}


def upload_sftp(local_path: str, filename: str,
                config_name: str = 'DEFAULT',
                remote_path: str = '') -> bool:
    """
    通过配置文件上传文件到 SFTP 服务器

    Args:
        local_path: 本地文件路径
        filename: 文件名
        config_name: 配置名称（从 SFTP_CONFIG 中读取）
        remote_path: 远程路径（可选，如果为空则使用配置中的 REMOTE_BASE_PATH）

    Returns:
        bool: 上传成功返回 True
    """
    try:
        config = get_sftp_config(config_name)

        if not config:
            app_logger.error(f"未找到 {config_name} 的 SFTP 配置")
            return False

        if not remote_path:
            app_logger.warning(f"未指定远程路径，将使用配置中的 REMOTE_BASE_PATH")
            remote_path = config.get('REMOTE_BASE_PATH', '')

        sftp_remote = f"{remote_path}/{filename}" if remote_path else filename
        app_logger.info(f"上传文件到 SFTP: {local_path} -> {sftp_remote}")
        return upload_file_to_sftp(
            config.get('HOST', ''),
            config.get('PORT', 22),
            config.get('USERNAME', ''),
            config.get('PASSWORD', ''),
            local_path,
            sftp_remote
        )
    except Exception as e:
        app_logger.error(f"上传文件到 SFTP 失败：{str(e)}")
        raise e
