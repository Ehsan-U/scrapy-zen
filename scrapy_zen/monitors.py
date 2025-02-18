from jinja2 import FileSystemLoader, Environment
from spidermon import MonitorSuite
from spidermon.contrib.actions.telegram.notifiers import SendTelegramMessageSpiderFinished
from spidermon.contrib.scrapy.monitors.monitors import CriticalCountMonitor, DownloaderExceptionMonitor,ErrorCountMonitor,UnwantedHTTPCodesMonitor



class CustomSendTelegramMessageSpiderFinished(SendTelegramMessageSpiderFinished):

    def get_template(self, name):
        loader = FileSystemLoader('scrapy_zen/templates')
        env = Environment(loader=loader)
        return env.get_template('message.jinja')


class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [
        CriticalCountMonitor,
        DownloaderExceptionMonitor,
        ErrorCountMonitor,
        UnwantedHTTPCodesMonitor,
    ]

    monitors_failed_actions = [
        CustomSendTelegramMessageSpiderFinished,
    ]
