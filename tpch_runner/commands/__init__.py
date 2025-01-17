from datetime import datetime

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"])


def format_datetime(atime: datetime) -> str:
    if atime.date() == datetime.now().date():
        fmt_datetime_value = atime.strftime("%H:%M:%S")
    else:
        fmt_datetime_value = atime.strftime("%Y-%m-%d")

    return fmt_datetime_value
