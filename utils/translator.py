import yaml
from logger import app_logger

from path_utils import get_config_path
# 加载翻译配置
def load_translations(config_filename: str = 'config_template.yaml'):
    config_path = get_config_path(config_filename)
    app_logger.info(f"Loading translations from: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


MESSAGES = load_translations()


def get_message(key, lang="en", **kwargs):
    """获取指定语言的消息，支持参数替换"""
    messages = MESSAGES.get(lang, MESSAGES["en"])
    message = messages.get(key, key)
    if kwargs:
        return message.format(**kwargs)
    return message
