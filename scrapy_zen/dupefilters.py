import logging
from pathlib import Path
from typing import Self
from scrapy.settings import BaseSettings
import time
from scrapy.dupefilters import RFPDupeFilter
from scrapy.http import Request
from scrapy.utils.request import (
    RequestFingerprinter,
    RequestFingerprinterProtocol,
)

from scrapy_zen.utils import job_dir



class ZenDupeFilter(RFPDupeFilter):

    @classmethod
    def _from_settings(
        cls,
        settings: BaseSettings,
        *,
        fingerprinter: RequestFingerprinterProtocol | None = None,
    ) -> Self:
        debug = settings.getbool("DUPEFILTER_DEBUG")
        return cls(job_dir(settings), debug, fingerprinter=fingerprinter)


    def __init__(
        self,
        path: str | None = None,
        debug: bool = False,
        *,
        fingerprinter: RequestFingerprinterProtocol | None = None,
    ) -> None:
        self.file = None
        self.fingerprinter: RequestFingerprinterProtocol = (
            fingerprinter or RequestFingerprinter()
        )
        self.fingerprints: set[str] = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            self.file = Path(path, "requests.seen").open("a+", encoding="utf-8")
            self.file.seek(0)
            valid_entries = set()
            for x in self.file:
                fp, timestamp = x.rstrip().split("_")
                if self.is_timestamp_older_than(int(timestamp), 7):
                    continue
                self.fingerprints.add(fp)
                valid_entries.add(x.rstrip())

            self.logger.info(f"Loaded {len(valid_entries)} entries from {path}")
            self.file.truncate(0) # clear
            self.file.seek(0) # move to start
            for entry in valid_entries:
                self.file.write(entry + "\n")
            self.file.flush()


    def request_seen(self, request: Request) -> bool:
        fp = self.request_fingerprint(request)
        if fp in self.fingerprints:
            return True
        self.fingerprints.add(fp)
        if self.file:
            fp = f"{fp}_{int(time.time())}"
            self.file.write(fp + "\n")
        return False


    @staticmethod
    def is_timestamp_older_than(timestamp: int, days: int) -> bool:
        current_time = int(time.time())
        days_in_seconds = days * 24 * 60 * 60
        return (current_time - timestamp) > days_in_seconds
