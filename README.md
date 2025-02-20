# scrapy-zen

A toolkit for Scrapy that provides multiple output pipelines, monitoring capabilities, and enhanced request handling.

## Features

- Unified download handler
- Request pre-processing and deduplication
- Item pre-processing and deduplication
- Spidermon integration for monitoring
- Support for Playwright, Browser Impersonation, and Zyte API
- Multiple output pipelines (Discord, Telegram, WebSocket, HTTP, gRPC, Synoptic)


## Installation

```bash
pip install scrapy-zen[all]
```

Available extras:
- `grpc` - gRPC pipeline dependencies
- `websocket` - WebSocket pipeline dependencies
- `monitoring` - Spidermon integration
- `playwright` - Playwright support
- `impersonate` - Browser impersonation support
- `zyte` - Zyte API support

## Configuration

`settings.py`
```
DB_EXPIRY_DAYS = 30  # Optional, defaults to 30 days
```

The following settings need to be configured in your .env file:

`.env`
```python
# Database settings (required for deduplication)
DB_NAME = "your_db_name"
DB_USER = "your_db_user"
DB_PASS = "your_db_password"
DB_HOST = "localhost"
DB_PORT = "5432"
```

### Optional Pipeline Settings

#### Discord Pipeline
```python
DISCORD_SERVER_URI = "your_discord_webhook_url"
```

#### Synoptic Pipeline
```python
SYNOPTIC_SERVER_URI = "your_synoptic_server_url"
SYNOPTIC_STREAM_ID = "your_stream_id"
SYNOPTIC_API_KEY = "your_api_key"
```

#### Telegram Pipeline
```python
TELEGRAM_SERVER_URI = "your_telegram_api_url"
TELEGRAM_TOKEN = "your_bot_token"
TELEGRAM_CHAT_ID = "your_chat_id"
```

#### gRPC Pipeline
```python
GRPC_SERVER_URI = "your_grpc_server"
GRPC_TOKEN = "your_token"
GRPC_ID = "your_id"
GRPC_PROTO_MODULE = "your_proto_module"
```

#### WebSocket Pipeline
```python
WS_SERVER_URI = "your_websocket_server_url"
```

#### HTTP Pipeline
```python
HTTP_SERVER_URI = "your_http_server_url"
HTTP_TOKEN = "your_auth_token"
```

### Zyte & Playwright Settings

`settings.py`
```python
# Playwright settings
PLAYWRIGHT_ABORT_REQUEST = lambda req: req.resource_type == "image"
PLAYWRIGHT_PROCESS_REQUEST_HEADERS = None

# Zyte API
ZYTE_ENABLED = True  # Enable Zyte API integration
```

### Monitoring Settings

`settings.py`
```python
# Spidermon settings
SPIDERMON_ENABLED = True
SPIDERMON_MAX_ERRORS = 0
SPIDERMON_MAX_CRITICALS = 0
SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS = 0
SPIDERMON_UNWANTED_HTTP_CODES = {403: 0, 429: 0}

# Discord notifications
SPIDERMON_DISCORD_WEBHOOK_URL = "your_discord_webhook"

# Telegram notifications (disabled at the moment)
SPIDERMON_TELEGRAM_SENDER_TOKEN = "your_telegram_token"
SPIDERMON_TELEGRAM_RECIPIENTS = ["your_chat_id"]
```

## Addons

### ZenAddon
It provides a plug-in-play experience by configuring all previous settings except monitoring.

### SpidermonAddon
It provides a plug-in-play experience by configuring monitoring settings.


```python
"ADDONS": {
    "scrapy_zen.addons.ZenAddon": 1,
    "scrapy_zen.addons.SpidermonAddon": 2,
}
```

## Usage

```python
"ADDONS": {
    "scrapy_zen.addons.ZenAddon": 1,
    "scrapy_zen.addons.SpidermonAddon": 2,
}
'ITEM_PIPELINES': {
    'scrapy_zen.pipelines.PreProcessingPipeline': 100,
    'scrapy_zen.pipelines.DiscordPipeline': 200,
    'scrapy_zen.pipelines.TelegramPipeline': 300,
    'scrapy_zen.pipelines.WSPipeline': 400,
    'scrapy_zen.pipelines.GRPCPipeline': 500,
    'scrapy_zen.pipelines.HttpPipeline': 600,
    'scrapy_zen.pipelines.SynopticPipeline': 700,
}
'DOWNLOADER_MIDDLEWARES': {
    'scrapy_zen.middlewares.PreProcessingMiddleware': 100,
}
```

```python
yield Request(
    url="http://example.com",
    meta={
        "_id": "unique_id",  # For deduplication
        "_dt": "2024-01-01",  # For date filtering
        "_dt_format": "%Y-%m-%d",  # Optional date format
    }
)
```
