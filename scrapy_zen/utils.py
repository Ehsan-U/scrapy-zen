from scrapy.settings import BaseSettings
from pathlib import Path


def job_dir(settings: BaseSettings) -> str | None:
    path: str | None = settings["ZEN_JOBDIR"]
    if not path:
        return None
    if not Path(path).exists():
        Path(path).mkdir(parents=True)
    return path
