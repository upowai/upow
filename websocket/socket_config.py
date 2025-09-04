"""WebSocket Configuration for Blockchain Block Broadcasting"""

import os
from typing import Optional

# Connection settings
MAX_CONNECTIONS_PER_IP = 5  # Maximum number of connections per IP address
MAX_TOTAL_CONNECTIONS = 1000  # Maximum number of total connections
HEARTBEAT_INTERVAL = 30  # seconds
CONNECTION_TIMEOUT = 300  # 5 minutes
MESSAGE_SIZE_LIMIT = 64 * 1024  # 64KB - blockchain messages are small

# Rate limiting
RATE_LIMIT_MESSAGES_PER_MINUTE = 60  # Maximum number of messages per minute
RATE_LIMIT_WINDOW = 60  # seconds

# Message types
ALLOWED_MESSAGE_TYPES = [
    "ping",
    "pong",
    "subscribe_block",
    "unsubscribe_block",
]

# Subscription channels
SUBSCRIPTION_CHANNELS = [
    "block",  # New blocks
    "transaction",  # New transactions
]

# Logging
WEBSOCKET_LOG_LEVEL = os.getenv("WEBSOCKET_LOG_LEVEL", "INFO")
LOG_CONNECTION_EVENTS = True
LOG_MESSAGE_EVENTS = False  # Set to True for debugging, False for production

# Security
REQUIRE_AUTH = False  # Allow unauthenticated connections for public data
VALIDATE_ORIGIN = True

# Performance
ENABLE_COMPRESSION = True
PING_INTERVAL = 20  # seconds
PING_TIMEOUT = 10  # seconds