import re


def clean_strange_chars(text: str) -> str:
    # 去除控制字符和私有区字符，但保留正常符号和字母
    return ''.join(
        ch for ch in text
        if (ch.isprintable() and not 0xE000 <= ord(ch) <= 0xF8FF)  # 去除私有区（如 ）
    )


def extract_https_in_quotes(text: str):
    """
    从类似 JS 对象中提取 URL，例如：
    "key":["https://example.com",0]
    或 ['https://example.com',0]
    """
    # 兼容单双引号、逗号前无空格、或有空格
    pattern = re.compile(r'["\']https?://[^"\']+["\']\s*,\s*\d+\]')
    matches = pattern.findall(text)

    # 去掉前后的引号，只保留纯 URL
    urls = [re.search(r'https?://[^"\']+', m).group(0) for m in matches]
    return urls

if __name__ == "__main__":
    sample_text = "123 Main St., Apt #4B! @New-Yorké"
    s1 = "1 Rue des 4 Cheminées, 92100 Boulogne-Billancourt, France"
    cleaned = clean_strange_chars(sample_text)
    print("Original:", sample_text)
    print("Cleaned:", cleaned)
    print("Original with strange chars:", s1)
    print("Cleaned:", clean_strange_chars(s1))

