import re
from datetime import datetime, time, timezone, timedelta
from zoneinfo import ZoneInfo

import pytz


def has_chinese(text):
    # 包含更多中文字符范围
    pattern = r'[\u4e00-\u9fff\u3400-\u4dbf\U00020000-\U0002a6df\U0002a700-\U0002b73f\U0002b740-\U0002b81f\U0002b820-\U0002ceaf]'
    return bool(re.search(pattern, text))


def remove_brackets(text):
    """
    Remove all content within brackets (including nested brackets).
    Handles both Chinese（）and English () brackets.

    Examples:
        "SUPER EYT (免費)(12月22日6時起改為'觀眾票選-追劇馬拉松 (免費)'" -> "SUPER EYT"
        "Channel (HD) (Test)" -> "Channel"
        "Nested (outer (inner) text)" -> "Nested"
    """
    result = []
    depth = 0  # Track bracket nesting depth

    for char in text:
        if char in '（(':
            depth += 1
        elif char in '）)':
            depth = max(0, depth - 1)  # Prevent negative depth
        elif depth == 0:
            result.append(char)

    # Clean up multiple consecutive spaces
    cleaned = ''.join(result).strip()
    return re.sub(r'\s+', ' ', cleaned)


def utc_and_duration_to_local(start_time, duration):
    # 1. 字符串转UTC时间对象
    start_time_utc = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S.000Z")
    start_time_utc = pytz.utc.localize(start_time_utc)

    # 2. 计算结束时间（UTC）
    end_time_utc = start_time_utc + timedelta(seconds=duration)

    # 3. 转成上海时间
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    start_time_shanghai = start_time_utc.astimezone(shanghai_tz)
    end_time_shanghai = end_time_utc.astimezone(shanghai_tz)
    return start_time_shanghai, end_time_shanghai


def utc_to_utc8_datetime(utc_timestamp):
    utc8_dt = datetime.fromtimestamp(utc_timestamp, tz=ZoneInfo('Asia/Shanghai'))
    return utc8_dt
