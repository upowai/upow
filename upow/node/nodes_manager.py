import json
import os
from json import JSONDecodeError
from os.path import dirname, exists
from random import sample
from filelock import FileLock

import httpx
import pickledb

from ..constants import MAX_BLOCK_SIZE_HEX
from ..helpers import timestamp

import config as config


CORE_URL = (
    getattr(config, "CORE_URL", "http://localhost:3006/")
    if hasattr(config, "CORE_URL") and config.CORE_URL
    else "http://localhost:3006/"
)


ACTIVE_NODES_DELTA = 60 * 60 * 24 * 7  # 7 days
INACTIVE_NODES_DELTA = 60 * 60 * 24 * 90  # 3 months
MAX_NODES_COUNT = 100

path = dirname(os.path.realpath(__file__)) + "/nodes.json"
lock_path = path + ".lock"
file_lock = FileLock(lock_path, timeout=10)  # 10 second timeout

if not exists(path):
    with file_lock:
        if not exists(path):  # Double-check after acquiring lock
            json.dump({}, open(path, "wt"))
else:
    with file_lock, open(path, "rt") as file:
        data = file.read()
        if not data.strip():
            # If the file is empty, initialize the database with an empty dictionary
            json.dump({}, open(path, "wt"))

db = pickledb.load(path, False)  # Disable auto-dump


class NodesManager:
    _initialized = False  # New flag to ensure init() runs only once per process
    last_messages: dict = None
    nodes: list = None
    db = db

    timeout = httpx.Timeout(5)
    async_client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)

    @staticmethod
    def canonicalize_url(url: str) -> str:
        """Converts a node URL to a canonical form (e.g., replaces localhost with 127.0.0.1, strips trailing slash)."""
        url = url.strip().rstrip('/')
        if 'localhost' in url:
            url = url.replace('localhost', '127.0.0.1')
        return url

    @staticmethod
    def init():
        if NodesManager._initialized:
            return

        core_url = NodesManager.canonicalize_url(CORE_URL)
        with file_lock:
            try:
                NodesManager.db._loaddb()
            except JSONDecodeError as e:
                print(e)
                with open(path, "wt") as f:
                    json.dump({}, f)
                NodesManager.db._loaddb()

            # Canonicalize URLs when loading from DB and ensure uniqueness, filtering out empty strings
            loaded_nodes = [NodesManager.canonicalize_url(n) for n in (NodesManager.db.get("nodes") or []) if n]
            if core_url not in loaded_nodes:
                loaded_nodes.insert(0, core_url) # Add to front if not present
            NodesManager.nodes = [n for n in list(dict.fromkeys(loaded_nodes)) if n] # Remove duplicates and ensure no empty strings

            loaded_last_messages = NodesManager.db.get("last_messages") or {}
            NodesManager.last_messages = {
                NodesManager.canonicalize_url(k): v for k, v in loaded_last_messages.items() if k # Filter empty keys
            }
            if core_url not in NodesManager.last_messages:
                NodesManager.last_messages[core_url] = timestamp()

            NodesManager._initialized = True # Set flag after successful initialization

    @staticmethod
    def sync():
        with file_lock:
            NodesManager.db.set("nodes", NodesManager.nodes)
            NodesManager.db.set("last_messages", NodesManager.last_messages)
            NodesManager.db.dump()

    @staticmethod
    async def request(url: str, method: str = "GET", **kwargs):
        async with NodesManager.async_client.stream(method, url, **kwargs) as response:
            res = ""
            async for chunk in response.aiter_text():
                res += chunk
                if len(res) > MAX_BLOCK_SIZE_HEX * 10:
                    break
        return json.loads(res)

    @staticmethod
    async def is_node_working(node: str):
        try:
            await NodesManager.request(node)
            return True
        except:
            return False

    @staticmethod
    def add_node(node: str):
        node = NodesManager.canonicalize_url(node) # Canonicalize here
        if not node: # Do not add empty string nodes
            return
        if (
            len(NodesManager.nodes) > MAX_NODES_COUNT
            or len(NodesManager.get_zero_nodes()) > 10
        ):
            NodesManager.clear_old_nodes()
        if len(NodesManager.nodes) > MAX_NODES_COUNT:
            raise Exception("Too many nodes")
        NodesManager.init() # This will now only load from disk once
        if node not in NodesManager.nodes: # Check for existence after canonicalization
            NodesManager.nodes.append(node)
        NodesManager.sync()

    @staticmethod
    def get_nodes():
        NodesManager.init() # This will now only load from disk once
        # Canonicalize keys from last_messages before extending, filtering out empty strings
        canonical_last_messages_keys = [NodesManager.canonicalize_url(k) for k in NodesManager.last_messages.keys() if k]
        # Combine existing nodes with new canonicalized keys, then deduplicate
        combined_nodes = NodesManager.nodes + canonical_last_messages_keys
        # All nodes should already be canonicalized by init() or add_node(),
        # so just deduplicate.
        NodesManager.nodes = list(dict.fromkeys(combined_nodes))
        NodesManager.sync()
        return NodesManager.nodes

    @staticmethod
    def get_recent_nodes():
        full_nodes = {
            node_url: NodesManager.get_last_message(node_url)
            for node_url in NodesManager.get_nodes()
        }
        return [
            item[0]
            for item in sorted(
                full_nodes.items(), key=lambda item: item[1], reverse=True
            )
            if item[1] > timestamp() - ACTIVE_NODES_DELTA
        ]

    @staticmethod
    def get_zero_nodes():
        return [
            node
            for node in NodesManager.get_nodes()
            if NodesManager.get_last_message(node) == 0
        ]

    @staticmethod
    def get_propagate_nodes():
        active_nodes = NodesManager.get_recent_nodes()
        zero_nodes = NodesManager.get_zero_nodes()
        return (
            sample(active_nodes, k=10) if len(active_nodes) > 10 else active_nodes
        ) + (sample(zero_nodes, k=10) if len(zero_nodes) > 10 else zero_nodes)

    @staticmethod
    def clear_old_nodes():
        NodesManager.init() # This will now only load from disk once
        # Ensure nodes are canonicalized before filtering
        canonical_nodes = [NodesManager.canonicalize_url(node) for node in NodesManager.get_nodes() if node] # Filter empty strings
        NodesManager.nodes = [
            node
            for node in canonical_nodes
            if NodesManager.get_last_message(node) > timestamp() - INACTIVE_NODES_DELTA
        ]
        NodesManager.sync()

    @staticmethod
    def get_last_message(node_url: str):
        NodesManager.init() # This will now only load from disk once
        canonical_node_url = NodesManager.canonicalize_url(node_url) # Canonicalize here
        if not canonical_node_url: # If URL is empty after canonicalization, it's not a valid node
            return 0
        last_messages = NodesManager.last_messages
        return last_messages[canonical_node_url] if canonical_node_url in last_messages else 0

    @staticmethod
    def update_last_message(node_url: str):
        NodesManager.init() # This will now only load from disk once
        canonical_node_url = NodesManager.canonicalize_url(node_url) # Canonicalize here
        if not canonical_node_url: # Do not update for empty string nodes
            return
        NodesManager.last_messages[canonical_node_url] = timestamp()
        NodesManager.sync()


class NodeInterface:
    def __init__(self, url: str):
        # Canonicalize URL when creating NodeInterface instance
        self.url = NodesManager.canonicalize_url(url)
        self.base_url = self.url.replace("http://", "", 1).replace("https://", "", 1)

    async def get_block(self, block_no: int, full_transactions: bool = False):
        res = await self.request(
            "get_block", {"block": block_no, "full_transactions": full_transactions}
        )
        return res["result"]

    async def get_blocks(self, offset: int, limit: int):
        res = await self.request("get_blocks", {"offset": offset, "limit": limit})
        if "result" not in res:
            # todo improve error handling
            raise Exception(res["error"])
        return res["result"]

    async def get_nodes(self):
        res = await self.request("get_nodes")
        return res["result"]

    async def get_mining_info(self):
        res = await self.request("get_mining_info")
        return res["result"]

    async def request(self, path: str, data: dict = {}, sender_node: str = ""):
        headers = {"Sender-Node": sender_node}
        if path in ("push_block", "push_tx"):
            r = await NodesManager.request(
                f"{self.url}/{path}",
                method="POST",
                json=data,
                headers=headers,
                timeout=10,
            )
        else:
            r = await NodesManager.request(
                f"{self.url}/{path}", params=data, headers=headers, timeout=10
            )
        return r
