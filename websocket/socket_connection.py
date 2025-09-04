"""WebSocket Connection Management
Handles individual client connections with proper lifecycle management
"""

import json
import time
import asyncio
import logging
from typing import Dict, Optional, Set, Any
from fastapi import WebSocket
from datetime import datetime, timezone
from dataclasses import dataclass, field
import uuid
from decimal import Decimal

from .socket_config import (
    HEARTBEAT_INTERVAL,
    CONNECTION_TIMEOUT,
    MESSAGE_SIZE_LIMIT,
    RATE_LIMIT_MESSAGES_PER_MINUTE,
    RATE_LIMIT_WINDOW,
    ALLOWED_MESSAGE_TYPES,
    SUBSCRIPTION_CHANNELS,
    LOG_CONNECTION_EVENTS,
    LOG_MESSAGE_EVENTS,
)

logger = logging.getLogger(__name__)


class UUIDEncoder(json.JSONEncoder):
    """Custom JSON encoder that converts UUID objects to strings and Decimal objects to float"""

    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


@dataclass
class ConnectionStats:
    """Statistics for a WebSocket connection"""

    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    messages_sent: int = 0
    messages_received: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    reconnect_count: int = 0


class RateLimiter:
    """Simple in-memory rate limiter for WebSocket messages"""

    def __init__(
        self,
        max_messages: int = RATE_LIMIT_MESSAGES_PER_MINUTE,
        window: int = RATE_LIMIT_WINDOW,
    ):
        self.max_messages = max_messages
        self.window = window
        self.message_times = []

    def is_allowed(self) -> bool:
        """Check if a message is allowed based on rate limiting"""
        now = time.time()
        # Remove old messages outside the window
        self.message_times = [
            msg_time for msg_time in self.message_times if now - msg_time < self.window
        ]

        if len(self.message_times) >= self.max_messages:
            return False

        self.message_times.append(now)
        return True


class WebSocketConnection:
    """Manages a single WebSocket connection with production-grade features"""

    def __init__(
        self, websocket: WebSocket, connection_id: str, user_id: Optional[int] = None
    ):
        self.websocket = websocket
        self.connection_id = connection_id
        self.user_id = user_id
        self.is_authenticated = False
        self.subscriptions: Set[str] = set()
        self.stats = ConnectionStats()
        self.rate_limiter = RateLimiter()
        self.is_alive = True
        self.last_heartbeat = time.time()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None

        if LOG_CONNECTION_EVENTS:
            logger.info(f"WebSocket connection created: {self.connection_id}")

    async def accept(self):
        """Accept the WebSocket connection"""
        await self.websocket.accept()
        self.stats.connected_at = datetime.now(timezone.utc)
        self.stats.last_activity = datetime.now(timezone.utc)

        # Start background tasks
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        if LOG_CONNECTION_EVENTS:
            logger.info(f"WebSocket connection accepted: {self.connection_id}")

    async def send_message(self, message: Dict[str, Any]):
        """Send a message to the client with error handling"""
        if not self.is_alive:
            return False

        try:
            # Use custom encoder to handle UUID objects
            message_str = json.dumps(message, cls=UUIDEncoder)
            message_size = len(message_str.encode("utf-8"))

            if message_size > MESSAGE_SIZE_LIMIT:
                logger.warning(
                    f"Message too large for connection {self.connection_id}: {message_size} bytes"
                )
                return False

            await self.websocket.send_text(message_str)
            self.stats.messages_sent += 1
            self.stats.bytes_sent += message_size
            self.stats.last_activity = datetime.now(timezone.utc)

            if LOG_MESSAGE_EVENTS:
                logger.debug(
                    f"Sent message to {self.connection_id}: {message.get('type', 'unknown')}"
                )

            return True

        except Exception as e:
            logger.error(f"Error sending message to {self.connection_id}: {str(e)}")
            await self.close()
            return False

    async def receive_message(self) -> Optional[Dict[str, Any]]:
        """Receive and validate a message from the client"""
        if not self.is_alive:
            return None

        try:
            # Check rate limiting
            if not self.rate_limiter.is_allowed():
                await self.send_error("RATE_LIMIT_EXCEEDED", "Too many messages sent")
                return None

            data = await self.websocket.receive_text()
            message_size = len(data.encode("utf-8"))

            if message_size > MESSAGE_SIZE_LIMIT:
                await self.send_error(
                    "MESSAGE_TOO_LARGE",
                    f"Message size exceeds {MESSAGE_SIZE_LIMIT} bytes",
                )
                return None

            message = json.loads(data)

            # Validate message structure
            if not isinstance(message, dict) or "type" not in message:
                await self.send_error(
                    "INVALID_MESSAGE", "Message must be JSON object with 'type' field"
                )
                return None

            # Validate message type
            if message["type"] not in ALLOWED_MESSAGE_TYPES:
                await self.send_error(
                    "INVALID_MESSAGE_TYPE",
                    f"Message type '{message['type']}' not allowed",
                )
                return None

            self.stats.messages_received += 1
            self.stats.bytes_received += message_size
            self.stats.last_activity = datetime.now(timezone.utc)
            self.last_heartbeat = time.time()

            if LOG_MESSAGE_EVENTS:
                logger.debug(
                    f"Received message from {self.connection_id}: {message['type']}"
                )

            # Log orderbook subscribe/unsubscribe messages for debugging
            if message["type"] in ["subscribe_orderbook", "unsubscribe_orderbook"]:
                logger.info(
                    f"ORDERBOOK_DEBUG: Received {message['type']} from {self.connection_id}: {message}"
                )

            return message

        except json.JSONDecodeError:
            await self.send_error("INVALID_JSON", "Message must be valid JSON")
            return None
        except Exception as e:
            logger.error(f"Error receiving message from {self.connection_id}: {str(e)}")
            await self.close()
            return None

    async def send_error(self, error_code: str, message: str):
        """Send an error message to the client"""
        error_message = {
            "type": "error",
            "error_code": error_code,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.send_message(error_message)

    async def send_success(self, message: str, data: Optional[Dict] = None):
        """Send a success message to the client"""
        success_message = {
            "type": "success",
            "message": message,
            "data": data or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.send_message(success_message)

    async def subscribe(self, channel: str) -> bool:
        """Subscribe to a channel"""
        if channel not in SUBSCRIPTION_CHANNELS:
            await self.send_error(
                "INVALID_CHANNEL", f"Channel '{channel}' not available"
            )
            return False

        # Only orderbook channel is available now - no auth requirement
        self.subscriptions.add(channel)
        await self.send_success(f"Subscribed to {channel}", {"channel": channel})

        if LOG_CONNECTION_EVENTS:
            logger.info(f"Connection {self.connection_id} subscribed to {channel}")

        return True

    async def unsubscribe(self, channel: str) -> bool:
        """Unsubscribe from a channel"""
        if channel in self.subscriptions:
            self.subscriptions.remove(channel)
            await self.send_success(
                f"Unsubscribed from {channel}", {"channel": channel}
            )

            if LOG_CONNECTION_EVENTS:
                logger.info(
                    f"Connection {self.connection_id} unsubscribed from {channel}"
                )

            return True
        else:
            await self.send_error(
                "NOT_SUBSCRIBED", f"Not subscribed to channel '{channel}'"
            )
            return False

    def authenticate(self, user_id: int):
        """Authenticate the connection"""
        self.user_id = user_id
        self.is_authenticated = True

        if LOG_CONNECTION_EVENTS:
            logger.info(
                f"Connection {self.connection_id} authenticated for user {user_id}"
            )

    def is_subscribed_to(self, channel: str) -> bool:
        """Check if connection is subscribed to a channel"""
        return channel in self.subscriptions

    def is_expired(self) -> bool:
        """Check if connection has expired"""
        return time.time() - self.last_heartbeat > CONNECTION_TIMEOUT

    async def ping(self):
        """Send a ping to the client"""
        ping_message = {
            "type": "ping",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.send_message(ping_message)

    async def pong(self):
        """Send a pong response to the client"""
        pong_message = {
            "type": "pong",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await self.send_message(pong_message)

    async def close(self, code: int = 1000, reason: str = "Connection closed"):
        """Close the WebSocket connection gracefully"""
        if not self.is_alive:
            return

        self.is_alive = False

        # Cancel background tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()

        try:
            await self.websocket.close(code=code, reason=reason)
        except Exception as e:
            logger.error(f"Error closing WebSocket {self.connection_id}: {str(e)}")

        if LOG_CONNECTION_EVENTS:
            logger.info(
                f"WebSocket connection closed: {self.connection_id} (reason: {reason})"
            )

    async def _heartbeat_loop(self):
        """Background task to send periodic heartbeats"""
        try:
            while self.is_alive:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                if self.is_alive:
                    await self.ping()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in heartbeat loop for {self.connection_id}: {str(e)}")

    async def _cleanup_loop(self):
        """Background task to check for expired connections"""
        try:
            while self.is_alive:
                await asyncio.sleep(30)  # Check every 30 seconds
                if self.is_expired():
                    logger.info(f"Connection {self.connection_id} expired, closing")
                    await self.close(code=1001, reason="Connection timeout")
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in cleanup loop for {self.connection_id}: {str(e)}")

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        uptime = datetime.now(timezone.utc) - self.stats.connected_at
        return {
            "connection_id": self.connection_id,
            "user_id": self.user_id,
            "is_authenticated": self.is_authenticated,
            "uptime_seconds": int(uptime.total_seconds()),
            "subscriptions": list(self.subscriptions),
            "messages_sent": self.stats.messages_sent,
            "messages_received": self.stats.messages_received,
            "bytes_sent": self.stats.bytes_sent,
            "bytes_received": self.stats.bytes_received,
            "last_activity": self.stats.last_activity.isoformat(),
            "reconnect_count": self.stats.reconnect_count,
        }
