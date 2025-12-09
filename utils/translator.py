
import yaml

import os
import sys

def load_translations():
    # 使用 getattr 和 getattr 来安全地确定基础路径
    if getattr(sys, 'frozen', False):
        config_path = os.path.join(os.path.dirname(sys.executable), 'config', 'translations_msg.yaml')
    else:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'translations_msg.yaml')

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Translation file not found at: {config_path}")

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
