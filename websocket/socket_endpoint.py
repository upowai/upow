"""WebSocket Endpoint
Provides FastAPI WebSocket endpoint for blockchain notifications
"""

import logging
from fastapi import WebSocket, WebSocketDisconnect, APIRouter
from starlette.background import BackgroundTasks

from .socket_manager import websocket_manager
from .socket_connection import WebSocketConnection
from .socket_handlers import WebSocketMessageHandler
from .socket_utils import WebSocketBroadcaster

logger = logging.getLogger(__name__)

# Create a router for WebSocket endpoints
websocket_router = APIRouter()

# Create message handler
message_handler = WebSocketMessageHandler()

# Create broadcaster
broadcaster = WebSocketBroadcaster()


@websocket_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for blockchain notifications"""
    connection = None
    try:
        # Add connection to manager
        connection = await websocket_manager.add_connection(websocket)

        # Handle incoming messages
        while True:
            message = await connection.receive_message()
            if message is None:
                break

            # Process the message
            await message_handler.handle_message(connection, message)

    except WebSocketDisconnect:
        logger.info(
            f"WebSocket client disconnected: {connection.connection_id if connection else 'unknown'}"
        )
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
    finally:
        # Remove connection from manager if it exists
        if connection:
            await websocket_manager.remove_connection(connection.connection_id)


async def broadcast_new_block(block_data, background_tasks: BackgroundTasks = None):
    """Broadcast a new block to all subscribed clients

    Args:
        block_data: The block data to broadcast
        background_tasks: Optional background tasks to run after broadcasting
    """
    await broadcaster.broadcast_new_block(block_data)


async def broadcast_new_transaction(tx_data, background_tasks: BackgroundTasks = None):
    """Broadcast a new transaction to all subscribed clients

    Args:
        tx_data: The transaction data to broadcast
        background_tasks: Optional background tasks to run after broadcasting
    """
    await broadcaster.broadcast_new_transaction(tx_data)


async def start_websocket_manager():
    """Start the WebSocket manager background tasks"""
    await websocket_manager.start_background_tasks()


async def shutdown_websocket_manager():
    """Shutdown the WebSocket manager"""
    await websocket_manager.shutdown()
