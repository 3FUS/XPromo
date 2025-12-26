import yaml
from utils.logger import app_logger

from utils.path_utils import get_config_path
# 加载翻译配置
def load_translations(config_filename: str = 'translations_msg.yaml'):
    config_path = get_config_path(config_filename)
    app_logger.info(f"Loading translations from: {config_path}")

    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


MESSAGES = load_translations()


def get_message(key, lang="en", **kwargs):
    """获取指定语言的消息，支持参数替换"""

    if lang not in MESSAGES:
        app_logger.warning(
            f"Language '{lang}' not found in translations. Available languages: {list(MESSAGES.keys())}. Using 'en' as fallback.")
        messages = MESSAGES.get("en")
    else:
        messages = MESSAGES.get(lang)

    app_logger.debug(f"Messages for language '{lang}' loaded: {messages is not None}")

    message = messages.get(key, key) if messages else key

    app_logger.debug(f"Message lookup result - key: '{key}', result: '{message}'")

    if kwargs:
        try:
            result = message.format(**kwargs)
            app_logger.debug(f"Message formatting successful: '{message}' -> '{result}' with kwargs: {kwargs}")
            return result
        except KeyError as e:
            app_logger.error(f"Message formatting failed for key '{key}' with kwargs {kwargs}. Error: {e}")
            return message
    else:
        app_logger.debug(f"No kwargs provided, returning message: '{message}'")
        return message
