from zoneinfo import ZoneInfo
from scrapy import Spider
from scrapy.exceptions import IgnoreRequest
from datetime import datetime, timedelta
import dateparser




class PreProcessingMiddleware:
    """
    Middleware to preprocess requests before forwarding.
    Handles deduplication
    """

    def process_request(self, request, spider: Spider) -> None:
        _dt = request.meta.pop("_dt", None)
        _dt_format = request.meta.pop("_dt_format", None)
        if _dt:
            if not self.is_recent(_dt, _dt_format, request.url, spider):
                raise IgnoreRequest
        return None
    
    def is_recent(self, date_str: str, date_format: str, debug_info: str, spider: Spider) -> bool:
        """
        Check if the date is recent (within the last 2 days).
        """
        try:
            if not date_str:
                return True
            utc_today = datetime.now(ZoneInfo('UTC')).date()
            input_date = dateparser.parse(date_string=date_str, date_formats=[date_format] if date_format is not None else None).date()
            return input_date >= (utc_today - timedelta(days=2))
        except Exception as e:
            spider.logger.error(f"{str(e)}: {debug_info} ")
            return False
