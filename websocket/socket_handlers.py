"""WebSocket Message Handlers
Handles different types of WebSocket messages for blockchain functionality
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

from .socket_connection import WebSocketConnection
from .socket_manager import websocket_manager
from .socket_config import (
    SUBSCRIPTION_CHANNELS,
    ALLOWED_MESSAGE_TYPES,
)

logger = logging.getLogger(__name__)


class WebSocketMessageHandler:
    """Handles incoming WebSocket messages for blockchain functionality"""

    def __init__(self):
        # Message type to handler mapping
        self.handlers = {
            "ping": self.handle_ping,
            "pong": self.handle_pong,
            "subscribe_block": self.handle_subscribe_block,
            "unsubscribe_block": self.handle_unsubscribe_block,
            "subscribe_transaction": self.handle_subscribe_transaction,
            "unsubscribe_transaction": self.handle_unsubscribe_transaction,
        }

    async def handle_message(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Route message to appropriate handler"""
        message_type = message.get("type")

        if not message_type:
            await connection.send_error("INVALID_MESSAGE", "Message type is required")
            return False

        if message_type not in ALLOWED_MESSAGE_TYPES:
            await connection.send_error(
                "INVALID_MESSAGE_TYPE", f"Message type '{message_type}' not allowed"
            )
            return False

        handler = self.handlers.get(message_type)
        if not handler:
            await connection.send_error(
                "HANDLER_NOT_FOUND", f"No handler for message type '{message_type}'"
            )
            return False

        try:
            return await handler(connection, message)
        except Exception as e:
            logger.error(
                f"Error handling message type '{message_type}' for connection {connection.connection_id}: {str(e)}"
            )
            await connection.send_error(
                "HANDLER_ERROR", f"Error processing {message_type} message"
            )
            return False

    async def handle_ping(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle ping messages"""
        await connection.pong()
        return True

    async def handle_pong(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle pong responses"""
        # Just update the last heartbeat time (already done in receive_message)
        logger.debug(f"Received pong from connection {connection.connection_id}")
        return True

    async def handle_authenticate(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle authentication messages"""
        # This is handled by the authenticator during initial connection
        # If we receive it here, the connection is already established
        await connection.send_error(
            "ALREADY_CONNECTED", "Connection already established"
        )
        return False

    async def handle_subscribe_block(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle block subscription requests"""
        try:
            # Subscribe to block updates
            await websocket_manager.add_channel_subscriber(
                connection.connection_id, "block"
            )

            await connection.send_success(
                "Subscribed to block updates",
                {"type": "block_subscription"},
            )
            logger.info(
                f"Connection {connection.connection_id} subscribed to block updates"
            )
            return True

        except Exception as e:
            logger.error(f"Error handling block subscription: {str(e)}")
            await connection.send_error(
                "SUBSCRIPTION_ERROR", "Error processing block subscription"
            )
            return False

    async def handle_unsubscribe_block(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle block unsubscription requests"""
        try:
            # Unsubscribe from block updates
            await websocket_manager.remove_channel_subscriber(
                connection.connection_id, "block"
            )

            await connection.send_success(
                "Unsubscribed from block updates",
                {"type": "block_unsubscription"},
            )
            logger.info(
                f"Connection {connection.connection_id} unsubscribed from block updates"
            )
            return True

        except Exception as e:
            logger.error(f"Error handling block unsubscription: {str(e)}")
            await connection.send_error(
                "UNSUBSCRIPTION_ERROR", "Error processing block unsubscription"
            )
            return False

    async def handle_subscribe_transaction(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle transaction subscription requests"""
        try:
            # Subscribe to transaction updates
            await websocket_manager.add_channel_subscriber(
                connection.connection_id, "transaction"
            )

            await connection.send_success(
                "Subscribed to transaction updates",
                {"type": "transaction_subscription"},
            )
            logger.info(
                f"Connection {connection.connection_id} subscribed to transaction updates"
            )
            return True

        except Exception as e:
            logger.error(f"Error handling transaction subscription: {str(e)}")
            await connection.send_error(
                "SUBSCRIPTION_ERROR", "Error processing transaction subscription"
            )
            return False

    async def handle_unsubscribe_transaction(
        self, connection: WebSocketConnection, message: Dict[str, Any]
    ) -> bool:
        """Handle transaction unsubscription requests"""
        try:
            # Unsubscribe from transaction updates
            await websocket_manager.remove_channel_subscriber(
                connection.connection_id, "transaction"
            )

            await connection.send_success(
                "Unsubscribed from transaction updates",
                {"type": "transaction_unsubscription"},
            )
            logger.info(
                f"Connection {connection.connection_id} unsubscribed from transaction updates"
            )
            return True

        except Exception as e:
            logger.error(f"Error handling transaction unsubscription: {str(e)}")
            await connection.send_error(
                "UNSUBSCRIPTION_ERROR", "Error processing transaction unsubscription"
            )
            return False


# Global message handler instance
message_handler = WebSocketMessageHandler()
