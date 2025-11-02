import re
import html
import base64


def extract_base64_links(text: str, min_length: int = 20) -> list[str]:
    """
    通用函数：从文本中提取并解码所有 Base64 编码的 URL 链接。
    自动反转义 HTML，过滤、去重。
    """
    text = html.unescape(text)
    b64_pattern = re.compile(
        r'(?<![A-Za-z0-9+/=])([A-Za-z0-9+/]{%d,}={0,2})(?![A-Za-z0-9+/=])' % min_length
    )

    links = set()
    for match in b64_pattern.findall(text):
        try:
            decoded = base64.b64decode(match).decode('utf-8', errors='ignore').strip()
            urls = re.findall(r'https?://[^\s"\'<>]+', decoded)
            for u in urls:
                links.add(u.strip())
        except Exception:
            continue
    return sorted(links)