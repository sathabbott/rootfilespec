from __future__ import annotations

from datetime import datetime


def fDatime_to_datetime(fDatime: int) -> datetime:
    """Convert fDatime to datetime

    Using the ROOT file convention
    (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second

    Args:
        fDatime (int): fDatime value

    Returns:
        datetime: datetime object
    """
    return datetime(
        year=(fDatime >> 26) + 1995,
        month=(fDatime >> 22) & 0xF,
        day=(fDatime >> 17) & 0x1F,
        hour=(fDatime >> 12) & 0x1F,
        minute=(fDatime >> 6) & 0x3F,
        second=(fDatime & 0x3F),
    )
