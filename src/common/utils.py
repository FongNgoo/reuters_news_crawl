from datetime import date, timedelta
import hashlib

def split_date_range(start: date, end: date, by: str = "month"):
    """
    Chia dòng thời gian thành các window có thời lượng khoản 1 tháng
    """

    windows = []
    current = start.replace(day=1)

    while current <= end:
        if current.month == 12:
            next_month = current.replace(year=current.year+1, month=1)
        else:
            next_month = current.replace(month=current.month+1)

        window_end = min(next_month-timedelta(days=1),end)
        windows.append((current,window_end))
        current = next_month

    return windows

def hash_url(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()
