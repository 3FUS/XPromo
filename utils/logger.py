# app/utils/logger.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os
from datetime import datetime


def setup_logger(name: str, log_file: str, level=logging.INFO):
    """
    创建一个日志记录器，每天自动备份日志文件

    Args:
        name: 日志记录器名称
        log_file: 日志文件路径
        level: 日志级别

    Returns:
        logging.Logger: 配置好的日志记录器
    """
    # 确保日志目录存在
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 创建日志记录器
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重复添加处理器
    if not logger.handlers:
        # 创建文件处理器，每天轮转
        file_handler = TimedRotatingFileHandler(
            log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding='utf-8'
        )
        file_handler.setLevel(level)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # 创建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # 设置格式化器
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # 添加处理器到日志记录器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


# 创建应用日志记录器
app_logger = setup_logger("app", "logs/promotion.log")
