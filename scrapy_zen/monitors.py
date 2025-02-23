from typing import Dict
from jinja2 import FileSystemLoader, Environment, Template
from pkg_resources import resource_filename
from spidermon import MonitorSuite
from scrapy.settings import Settings
from spidermon.contrib.actions.telegram.notifiers import SendTelegramMessageSpiderFinished
from spidermon.contrib.actions.discord.notifiers import SendDiscordMessageSpiderFinished
from spidermon.contrib.scrapy.monitors.monitors import CriticalCountMonitor, DownloaderExceptionMonitor,ErrorCountMonitor,UnwantedHTTPCodesMonitor
import logparser


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
        # DownloaderExceptionMonitor,
        ErrorCountMonitor,
        UnwantedHTTPCodesMonitor,
    ]

    monitors_failed_actions = [
        CustomSendTelegramMessageSpiderFinished,
        # CustomSendDiscordMessageSpiderFinished,
    ]

