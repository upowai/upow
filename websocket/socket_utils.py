"""WebSocket Utilities
Helper functions for broadcasting blockchain messages from other parts of the application
"""

import logging
from typing import Dict, Any
from datetime import datetime, timezone

from .socket_manager import websocket_manager

logger = logging.getLogger(__name__)


class WebSocketBroadcaster:
    """Utility class for broadcasting WebSocket blockchain messages from other application components"""

    @staticmethod
    async def broadcast_new_block(block_data: Dict[str, Any]) -> int:
        """
        Broadcast new block to block subscribers

        Args:
            block_data: Block data containing block_no, block_hash, miner, etc.

        Returns:
            Number of connections the message was sent to
        """
        try:
            message = {
                "type": "new_block",
                "data": block_data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            sent_count = await websocket_manager.broadcast_to_channel("block", message)
            logger.info(
                f"New block broadcast: Block #{block_data.get('block_no')} - sent to {sent_count} connections"
            )
            return sent_count

        except Exception as e:
            logger.error(f"Error broadcasting new block: {str(e)}")
            return 0

    @staticmethod
    async def broadcast_new_transaction(tx_data: Dict[str, Any]) -> int:
        """
        Broadcast new transaction to transaction subscribers

        Args:
            tx_data: Transaction data containing tx_hash, etc.

        Returns:
            Number of connections the message was sent to
        """
        try:
            message = {
                "type": "new_transaction",
                "data": tx_data,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            sent_count = await websocket_manager.broadcast_to_channel(
                "transaction", message
            )
            logger.debug(
                f"New transaction broadcast: {tx_data.get('tx_hash')} - sent to {sent_count} connections"
            )
            return sent_count

        except Exception as e:
            logger.error(f"Error broadcasting new transaction: {str(e)}")
            return 0


# Global broadcaster instance
websocket_broadcaster = WebSocketBroadcaster()
