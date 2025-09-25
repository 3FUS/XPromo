import paramiko
import logging
from utils.logger import setup_logger

# 初始化日志记录器
logger = setup_logger(__name__, "sftp_uploader.log")


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

        # 上传文件
        import os

        if not os.path.exists(local_path):
            logger.warning(f" {local_path} no....")
            return False

        sftp.put(local_path, remote_path)

        logger.info(f" {local_path} uploaded to {hostname}:{remote_path}")
        return True

    except Exception as e:
        logger.error(f"SFTP文件上传失败: {str(e)}", exc_info=True)
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
