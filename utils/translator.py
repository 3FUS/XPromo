
import yaml
import os


def load_translations():
    # 使用 getattr 和 getattr 来安全地确定基础路径
    if getattr(sys, 'frozen', False):
        # 如果是打包后的可执行文件
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    config_path = os.path.join(base_path, 'config', 'translations_msg.yaml')

    # 确保文件存在
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
