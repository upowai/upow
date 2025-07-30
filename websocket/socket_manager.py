"""WebSocket Manager
Handles multiple WebSocket connections for blockchain functionality
"""

import uuid
import asyncio
import logging
from typing import Dict, List, Set, Optional, Any
from collections import defaultdict
from datetime import datetime, timezone

from .socket_connection import WebSocketConnection
from .socket_config import (
    MAX_CONNECTIONS_PER_IP,
    MAX_TOTAL_CONNECTIONS,
    LOG_CONNECTION_EVENTS,
    SUBSCRIPTION_CHANNELS,
)

logger = logging.getLogger(__name__)


class WebSocketManager:
    """WebSocket connection manager for blockchain functionality"""

    def __init__(self):
        # Store all active connections
        self.connections: Dict[str, WebSocketConnection] = {}

        # Store connections by user ID for efficient user-based operations
        self.user_connections: Dict[int, Set[str]] = defaultdict(set)

        # Store connections by subscription channel for efficient broadcasting
        self.channel_subscribers: Dict[str, Set[str]] = defaultdict(set)

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._stats_task: Optional[asyncio.Task] = None

        # Track if tasks are started
        self._tasks_started = False

    async def start_background_tasks(self):
        """Start background maintenance tasks - called when event loop is available"""
        if self._tasks_started:
            return

        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        self._stats_task = asyncio.create_task(self._stats_loop())
        self._tasks_started = True
        logger.info("WebSocket Manager background tasks started")

    def _start_background_tasks(self):
        """Legacy method - now deprecated, use start_background_tasks() instead"""
        logger.warning(
            "_start_background_tasks is deprecated - tasks should be started explicitly when event loop is available"
        )

    async def add_connection(
        self, websocket, user_id: Optional[int] = None
    ) -> WebSocketConnection:
        """Add a new WebSocket connection"""
        # Check total connection limit
        if len(self.connections) >= MAX_TOTAL_CONNECTIONS:
            logger.warning(
                f"Maximum total connections ({MAX_TOTAL_CONNECTIONS}) reached"
            )
            raise Exception("Maximum connections reached")

        # Check per-user connection limit if user is authenticated
        if user_id and len(self.user_connections[user_id]) >= MAX_CONNECTIONS_PER_IP:
            logger.warning(
                f"Maximum connections per user ({MAX_CONNECTIONS_PER_IP}) reached for user {user_id}"
            )
            raise Exception("Maximum connections per user reached")

        # Generate unique connection ID
        connection_id = str(uuid.uuid4())

        # Create connection
        connection = WebSocketConnection(websocket, connection_id, user_id)

        # Store connection
        self.connections[connection_id] = connection
        if user_id:
            self.user_connections[user_id].add(connection_id)
            connection.authenticate(user_id)

        # Accept the connection
        await connection.accept()

        if LOG_CONNECTION_EVENTS:
            logger.info(
                f"Added connection {connection_id} for user {user_id}. Total connections: {len(self.connections)}"
            )

        return connection

    async def remove_connection(self, connection_id: str):
        """Remove a WebSocket connection"""
        if connection_id not in self.connections:
            return

        connection = self.connections[connection_id]

        # Remove from user connections
        if connection.user_id:
            self.user_connections[connection.user_id].discard(connection_id)
            if not self.user_connections[connection.user_id]:
                del self.user_connections[connection.user_id]

        # Remove from channel subscriptions
        for channel in connection.subscriptions.copy():
            await self._unsubscribe_connection(connection_id, channel)

        # Close the connection
        await connection.close()

        # Remove from connections
        del self.connections[connection_id]

        if LOG_CONNECTION_EVENTS:
            logger.info(
                f"Removed connection {connection_id}. Total connections: {len(self.connections)}"
            )

    async def authenticate_connection(self, connection_id: str, user_id: int) -> bool:
        """Authenticate a connection with a user ID"""
        if connection_id not in self.connections:
            return False

        connection = self.connections[connection_id]

        # Check per-user connection limit
        if len(self.user_connections[user_id]) >= MAX_CONNECTIONS_PER_IP:
            await connection.send_error(
                "MAX_CONNECTIONS", "Maximum connections per user reached"
            )
            return False

        # Move connection to user connections
        if connection.user_id:
            self.user_connections[connection.user_id].discard(connection_id)

        connection.authenticate(user_id)
        self.user_connections[user_id].add(connection_id)

        if LOG_CONNECTION_EVENTS:
            logger.info(f"Authenticated connection {connection_id} for user {user_id}")

        return True

    async def subscribe_connection(self, connection_id: str, channel: str) -> bool:
        """Subscribe a connection to a channel"""
        if connection_id not in self.connections:
            return False

        connection = self.connections[connection_id]

        if await connection.subscribe(channel):
            self.channel_subscribers[channel].add(connection_id)
            return True

        return False
        
    async def add_channel_subscriber(self, connection_id: str, channel: str) -> bool:
        """Add a connection to a channel's subscribers"""
        if channel not in SUBSCRIPTION_CHANNELS:
            logger.warning(f"Attempted to subscribe to invalid channel: {channel}")
            return False
            
        return await self.subscribe_connection(connection_id, channel)

    async def _unsubscribe_connection(self, connection_id: str, channel: str):
        """Internal method to unsubscribe a connection from a channel"""
        self.channel_subscribers[channel].discard(connection_id)
        if not self.channel_subscribers[channel]:
            del self.channel_subscribers[channel]

    async def unsubscribe_connection(self, connection_id: str, channel: str) -> bool:
        """Unsubscribe a connection from a channel"""
        if connection_id not in self.connections:
            return False

        connection = self.connections[connection_id]

        if await connection.unsubscribe(channel):
            await self._unsubscribe_connection(connection_id, channel)
            return True

        return False
        
    async def remove_channel_subscriber(self, connection_id: str, channel: str) -> bool:
        """Remove a connection from a channel's subscribers"""
        if channel not in SUBSCRIPTION_CHANNELS:
            logger.warning(f"Attempted to unsubscribe from invalid channel: {channel}")
            return False
            
        return await self.unsubscribe_connection(connection_id, channel)

    async def broadcast_to_channel(
        self,
        channel: str,
        message: Dict[str, Any],
        exclude_connection: Optional[str] = None,
    ):
        """Broadcast a message to all subscribers of a channel"""
        if channel not in self.channel_subscribers:
            return 0

        sent_count = 0
        failed_connections = []

        for connection_id in self.channel_subscribers[channel].copy():
            if connection_id == exclude_connection:
                continue

            if connection_id in self.connections:
                connection = self.connections[connection_id]
                success = await connection.send_message(message)
                if success:
                    sent_count += 1
                else:
                    failed_connections.append(connection_id)

        # Clean up failed connections
        for failed_id in failed_connections:
            await self.remove_connection(failed_id)

        logger.debug(f"Broadcast to channel '{channel}': {sent_count} messages sent")
        return sent_count

    async def send_to_user(self, user_id: int, message: Dict[str, Any]) -> int:
        """Send a message to all connections of a specific user"""
        if user_id not in self.user_connections:
            return 0

        sent_count = 0
        failed_connections = []

        for connection_id in self.user_connections[user_id].copy():
            if connection_id in self.connections:
                connection = self.connections[connection_id]
                success = await connection.send_message(message)
                if success:
                    sent_count += 1
                else:
                    failed_connections.append(connection_id)

        # Clean up failed connections
        for failed_id in failed_connections:
            await self.remove_connection(failed_id)

        logger.debug(f"Sent to user {user_id}: {sent_count} messages sent")
        return sent_count

    async def send_to_connection(
        self, connection_id: str, message: Dict[str, Any]
    ) -> bool:
        """Send a message to a specific connection"""
        if connection_id not in self.connections:
            return False

        connection = self.connections[connection_id]
        success = await connection.send_message(message)

        if not success:
            await self.remove_connection(connection_id)

        return success

    def get_connection(self, connection_id: str) -> Optional[WebSocketConnection]:
        """Get a connection by ID"""
        return self.connections.get(connection_id)

    def get_user_connections(self, user_id: int) -> List[WebSocketConnection]:
        """Get all connections for a user"""
        connection_ids = self.user_connections.get(user_id, set())
        return [
            self.connections[conn_id]
            for conn_id in connection_ids
            if conn_id in self.connections
        ]

    def get_channel_subscribers(self, channel: str) -> List[WebSocketConnection]:
        """Get all connections subscribed to a channel"""
        connection_ids = self.channel_subscribers.get(channel, set())
        return [
            self.connections[conn_id]
            for conn_id in connection_ids
            if conn_id in self.connections
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Get manager statistics"""
        total_connections = len(self.connections)
        authenticated_connections = len(
            [c for c in self.connections.values() if c.is_authenticated]
        )

        channel_stats = {}
        for channel in SUBSCRIPTION_CHANNELS:
            subscriber_count = len(self.channel_subscribers.get(channel, set()))
            channel_stats[channel] = subscriber_count

        user_stats = {
            "total_users": len(self.user_connections),
            "max_connections_per_user": max(
                [len(conns) for conns in self.user_connections.values()] + [0]
            ),
        }

        return {
            "total_connections": total_connections,
            "authenticated_connections": authenticated_connections,
            "anonymous_connections": total_connections - authenticated_connections,
            "channel_subscribers": channel_stats,
            "user_stats": user_stats,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def get_detailed_stats(self) -> Dict[str, Any]:
        """Get detailed statistics including per-connection stats"""
        stats = self.get_stats()

        connection_details = []
        for connection in self.connections.values():
            connection_details.append(connection.get_stats())

        stats["connections"] = connection_details
        return stats

    async def _cleanup_loop(self):
        """Background task to clean up dead connections"""
        try:
            while True:
                await asyncio.sleep(60)  # Run every minute

                dead_connections = []
                for connection_id, connection in self.connections.items():
                    if not connection.is_alive or connection.is_expired():
                        dead_connections.append(connection_id)

                for connection_id in dead_connections:
                    logger.info(f"Cleaning up dead connection: {connection_id}")
                    await self.remove_connection(connection_id)

                if dead_connections:
                    logger.info(f"Cleaned up {len(dead_connections)} dead connections")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in cleanup loop: {str(e)}")

    async def _stats_loop(self):
        """Background task to log periodic statistics"""
        try:
            while True:
                await asyncio.sleep(300)  # Log every 5 minutes

                stats = self.get_stats()
                logger.info(
                    f"WebSocket Manager Stats: {stats['total_connections']} total, "
                    f"{stats['authenticated_connections']} authenticated, "
                    f"{stats['user_stats']['total_users']} users"
                )

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in stats loop: {str(e)}")

    async def shutdown(self):
        """Gracefully shutdown the manager"""
        logger.info("Shutting down WebSocket Manager")

        # Cancel background tasks
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        if self._stats_task:
            self._stats_task.cancel()
            try:
                await self._stats_task
            except asyncio.CancelledError:
                pass

        # Reset task state
        self._tasks_started = False
        self._cleanup_task = None
        self._stats_task = None

        # Close all connections
        connection_ids = list(self.connections.keys())
        for connection_id in connection_ids:
            await self.remove_connection(connection_id)

        logger.info("WebSocket Manager shutdown complete")


# Global manager instance
websocket_manager = WebSocketManager()
