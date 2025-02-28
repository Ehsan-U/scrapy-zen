from typing import Dict
from jinja2 import FileSystemLoader, Environment, Template
from pkg_resources import resource_filename
from spidermon import MonitorSuite, monitors
from scrapy.settings import Settings
from spidermon.contrib.actions.telegram.notifiers import SendTelegramMessageSpiderFinished
from spidermon.contrib.actions.discord.notifiers import SendDiscordMessageSpiderFinished
from spidermon.contrib.scrapy.monitors.monitors import CriticalCountMonitor, BaseScrapyMonitor,ErrorCountMonitor,UnwantedHTTPCodesMonitor
import logparser

    

@monitors.name("Downloader Exceptions monitor")
class CustomDownloaderExceptionMonitor(BaseScrapyMonitor):
    stat_name = "downloader/exception_count"
    ignored_stat_name = "downloader/exception_type_count/scrapy.exceptions.IgnoreRequest"
    threshold_setting = "SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS"
    assert_type = "<="
    fail_if_stat_missing = False

    @monitors.name("Should have no downloader exception")
    def test_downloader_exception(self):
        if self.stat_name not in self.stats:
            self.skipTest(f"Unable to find '{self.stat_name}' in job stats.")
        count = self.stats.get(self.stat_name) - self.stats.get(self.ignored_stat_name, 0)
        threshold = self.crawler.settings.get(self.threshold_setting)
        msg = f"Expecting '{self.stat_name}' to be '{self.assert_type}' to '{threshold}'. Current value: '{count}'"
        self.assertTrue(count <= threshold, msg)


class CustomSendDiscordMessageSpiderFinished(SendDiscordMessageSpiderFinished):
    message_template = None

    def get_template(self, name: str) -> Template:
        template_dir = resource_filename('scrapy_zen', 'templates')
        loader = FileSystemLoader(template_dir)
        env = Environment(loader=loader)
        return env.get_template('message.jinja')
    
    def get_template_context(self):
        logs: Dict = self.extract_errors_from_file(self.data['crawler'].settings)
        context = {
            "result": self.result,
            "data": self.data,
            "bot_name": self.data['crawler'].settings.get("BOT_NAME"),
            "monitors_passed": self.monitors_passed,
            "monitors_failed": self.monitors_failed,
            "include_ok_messages": self.include_ok_messages,
            "include_error_messages": self.include_error_messages,
        }
        if logs:
            context.update({**logs})
        context.update(self.context)
        return context
    
    def extract_errors_from_file(self, settings: Settings) -> Dict | None:
        f = settings.get("LOG_FILE")
        if not f:
            return            
        with open(f, "r") as f:
            logs = f.read()
        d = logparser.parse(logs)
        return {
            "critial_logs": "".join(d['log_categories']['critical_logs']['details']), 
            "error_logs": "".join(d['log_categories']['error_logs']['details'])
        }
    

class CustomSendTelegramMessageSpiderFinished(SendTelegramMessageSpiderFinished):
    message_template = None

    def get_template(self, name: str) -> Template:
        template_dir = resource_filename('scrapy_zen', 'templates')
        loader = FileSystemLoader(template_dir)
        env = Environment(loader=loader)
        return env.get_template('message.jinja')
    
    def get_template_context(self):
        logs: Dict = self.extract_errors_from_file(self.data['crawler'].settings)
        context = {
            "result": self.result,
            "data": self.data,
            "bot_name": self.data['crawler'].settings.get("BOT_NAME"),
            "monitors_passed": self.monitors_passed,
            "monitors_failed": self.monitors_failed,
            "include_ok_messages": self.include_ok_messages,
            "include_error_messages": self.include_error_messages,
        }
        if logs:
            context.update({**logs})
        context.update(self.context)
        return context
    
    def extract_errors_from_file(self, settings: Settings) -> Dict | None:
        f = settings.get("LOG_FILE")
        if not f:
            return            
        with open(f, "r") as f:
            logs = f.read()
        d = logparser.parse(logs)
        return {
            "critial_logs": "".join(d['log_categories']['critical_logs']['details']), 
            "error_logs": "".join(d['log_categories']['error_logs']['details'])
        }
    



class SpiderCloseMonitorSuite(MonitorSuite):
    monitors = [
        CriticalCountMonitor,
        CustomDownloaderExceptionMonitor,
        ErrorCountMonitor,
        UnwantedHTTPCodesMonitor,
    ]

    monitors_failed_actions = [
        CustomSendTelegramMessageSpiderFinished,
        # CustomSendDiscordMessageSpiderFinished,
    ]

