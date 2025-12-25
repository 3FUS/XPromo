
import yaml
import os

# 加载翻译配置
def load_translations():
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'translations_msg.yaml')
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
