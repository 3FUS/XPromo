# utils/logger.py
import logging
from logging.handlers import TimedRotatingFileHandler
import os


def setup_logger(name: str, log_file: str = "promotion.log", level=logging.INFO):
    """
    配置统一的日志器
    :param name: 日志器名称（推荐传入 __name__）
    :param log_file: 日志文件路径
    :param level: 默认日志级别
    :return: logging.Logger 实例
    """

    # 创建日志目录（如果不存在）
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 创建 logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 防止重复添加 handler
    if logger.handlers:
        return logger

    # 创建 formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # 控制台 handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 文件 handler（按天滚动）
    file_handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
