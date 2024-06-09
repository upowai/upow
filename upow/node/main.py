import json
import random
import re
from asyncio import gather
from collections import deque, defaultdict
from os import environ, path
from typing import Annotated, Union

from asyncpg import UniqueViolationError
from fastapi import FastAPI, Body, Query, Header
from fastapi.responses import RedirectResponse
from icecream import ic
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from starlette.background import BackgroundTasks, BackgroundTask
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

import upow.helpers
from upow.constants import VERSION, ENDIAN, MAX_SUPPLY
from upow.database import Database
from upow.helpers import timestamp, sha256
from upow.manager import (
    create_block,
    get_difficulty,
    Manager,
    get_transactions_merkle_tree,
    split_block_content,
    calculate_difficulty,
    clear_pending_transactions,
    block_to_bytes,
    get_circulating_supply, create_block_in_syncing_old,
)
from upow.node.nodes_manager import NodesManager, NodeInterface
from upow.node.utils import ip_is_local
from upow.upow_transactions import Transaction, CoinbaseTransaction
from upow.upow_wallet.utils import create_transaction

limiter = Limiter(key_func=get_remote_address)
app = FastAPI()
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
db: Database = None
NodesManager.init()
started = False
is_syncing = False
self_url = None

print = ic

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


async def propagate(path: str, args: dict, ignore_url=None, nodes: list = None):
    global self_url
    self_node = NodeInterface(self_url or "")
    ignore_node = NodeInterface(ignore_url or "")
    aws = []
    for node_url in nodes or NodesManager.get_propagate_nodes():
        node_interface = NodeInterface(node_url)
        if (
            node_interface.base_url == self_node.base_url
            or node_interface.base_url == ignore_node.base_url
        ):
            continue
        aws.append(node_interface.request(path, args, self_node.url))
    for response in await gather(*aws, return_exceptions=True):
        pass
        # print(path, response)


async def create_blocks(blocks: list):
    _, last_block = await calculate_difficulty()
    last_block["id"] = last_block["id"] if last_block != {} else 0
    last_block["hash"] = (
        last_block["hash"]
        if "hash" in last_block
        else (18_884_643).to_bytes(32, ENDIAN).hex()
    )
    i = last_block["id"] + 1
    for block_info in blocks:
        block = block_info["block"]
        txs_hex = block_info["transactions"]
        txs = [await Transaction.from_hex(tx) for tx in txs_hex]
        cb_tx = None
        for tx in txs:
            if isinstance(tx, CoinbaseTransaction):
                txs.remove(tx)
                cb_tx = tx
                break
        hex_txs = [tx.hex() for tx in txs]
        block["merkle_tree"] = get_transactions_merkle_tree(hex_txs)
        # if i > 22500 else get_transactions_merkle_tree_ordered(hex_txs)
        block_content = block.get("content") or block_to_bytes(
            last_block["hash"], block
        )

        # if i <= 22500 and sha256(block_content) != block['hash'] and i != 17972:
        #     from itertools import permutations
        #     for l in permutations(hex_txs):
        #         _hex_txs = list(l)
        #         block['merkle_tree'] = get_transactions_merkle_tree_ordered(_hex_txs)
        #         block_content = block_to_bytes(last_block['hash'], block)
        #         if sha256(block_content) == block['hash']:
        #             break
        # elif 131309 < i < 150000 and sha256(block_content) != block['hash']:
        #     for diff in range(0, 100):
        #         block['difficulty'] = diff / 10
        #         block_content = block_to_bytes(last_block['hash'], block)
        #         if sha256(block_content) == block['hash']:
        #             break
        assert i == block["id"]
        if i <= 48308:
            if not await create_block_in_syncing_old(
                    block_content.hex() if isinstance(block_content, bytes) else block_content,
                    txs,
                    cb_tx,
                    last_block,
            ):
                return False
        else:
            if not await create_block(
                block_content.hex() if isinstance(block_content, bytes) else block_content,
                txs,
                last_block,
            ):
                return False
        last_block = block
        i += 1
    return True


async def _sync_blockchain(node_url: str = None):
    print("sync blockchain")
    if not node_url:
        nodes = NodesManager.get_recent_nodes()
        if not nodes:
            return
        node_url = random.choice(nodes)
    node_url = node_url.strip("/")
    _, last_block = await calculate_difficulty()
    starting_from = i = await db.get_next_block_id()
    node_interface = NodeInterface(node_url)
    local_cache = None
    if last_block != {} and last_block["id"] > 500:
        remote_last_block = (await node_interface.get_block(i - 1))["block"]
        if remote_last_block["hash"] != last_block["hash"]:
            print(remote_last_block["hash"])
            offset, limit = i - 500, 500
            remote_blocks = await node_interface.get_blocks(offset, limit)
            local_blocks = await db.get_blocks(offset, limit)
            local_blocks = local_blocks[: len(remote_blocks)]
            local_blocks.reverse()
            remote_blocks.reverse()
            print(len(remote_blocks), len(local_blocks))
            for n, local_block in enumerate(local_blocks):
                if local_block["block"]["hash"] == remote_blocks[n]["block"]["hash"]:
                    print(local_block, remote_blocks[n])
                    last_common_block = local_block["block"]["id"]
                    local_cache = local_blocks[:n]
                    local_cache.reverse()
                    await db.remove_blocks(last_common_block + 1)
                    break

    # return
    limit = 1000
    while True:
        i = await db.get_next_block_id()
        try:
            blocks = await node_interface.get_blocks(i, limit)
        except Exception as e:
            print(e)
            # NodesManager.get_nodes().remove(node_url)
            NodesManager.sync()
            break
        try:
            _, last_block = await calculate_difficulty()
            if not blocks:
                print("syncing complete")
                if last_block["id"] > starting_from:
                    NodesManager.update_last_message(node_url)
                    if timestamp() - last_block["timestamp"] < 86400:
                        # if last block is from less than a day ago, propagate it
                        txs_hashes = await db.get_block_transaction_hashes(
                            last_block["hash"]
                        )
                        await propagate(
                            "push_block",
                            {
                                "block_content": last_block["content"],
                                "txs": txs_hashes,
                                "block_no": last_block["id"],
                            },
                            node_url,
                        )
                break
            assert await create_blocks(blocks)
        except Exception as e:
            print(e)
            if local_cache is not None:
                print("sync failed, reverting back to previous chain")
                await db.delete_blocks(last_common_block)
                await create_blocks(local_cache)
            return


async def sync_blockchain(node_url: str = None):
    try:
        await _sync_blockchain(node_url)
    except Exception as e:
        print(e)
        return


@app.on_event("startup")
async def startup():
    global db
    db = await Database.create(
        user=environ.get("UPOW_DATABASE_USER", "upow"),
        password=environ.get("UPOW_DATABASE_PASSWORD", "12345"),
        database=environ.get("UPOW_DATABASE_NAME", "upow"),
        host=environ.get("UPOW_DATABASE_HOST", None),
    )


@app.get("/")
@limiter.limit("3/minute")
async def root(request: Request):
    return {
        "ok": True,
        "version": VERSION,
        "unspent_outputs_hash": await db.get_unspent_outputs_hash(),
    }


async def propagate_old_transactions(propagate_txs):
    await db.update_pending_transactions_propagation_time(
        [sha256(tx_hex) for tx_hex in propagate_txs]
    )
    for tx_hex in propagate_txs:
        await propagate("push_tx", {"tx_hex": tx_hex})


@app.middleware("http")
async def middleware(request: Request, call_next):
    global started, self_url
    nodes = NodesManager.get_recent_nodes()
    hostname = request.base_url.hostname

    # Normalize the URL path by removing extra slashes
    normalized_path = re.sub("/+", "/", request.scope["path"])
    if normalized_path != request.scope["path"]:
        url = request.url
        new_url = str(url).replace(request.scope["path"], normalized_path)
        # Redirect to normalized URL
        return RedirectResponse(new_url)

    if "Sender-Node" in request.headers:
        NodesManager.add_node(request.headers["Sender-Node"])

    if normalized_path == '/send_to_address' and not (ip_is_local(hostname) or hostname == "localhost"):
        return JSONResponse(
            status_code=403,
            content={"ok": False, "error": "Access forbidden. This endpoint can only be accessed from localhost."},
        )

    if nodes and not started or (ip_is_local(hostname) or hostname == "localhost"):
        try:
            node_url = nodes[0]
            # requests.get(f'{node_url}/add_node', {'url': })
            j = await NodesManager.request(f"{node_url}/get_nodes")
            nodes.extend(j["result"])
            NodesManager.sync()
        except:
            pass

        if not (ip_is_local(hostname) or hostname == "localhost"):
            started = True

            self_url = str(request.base_url).strip("/")
            try:
                nodes.remove(self_url)
            except ValueError:
                pass
            try:
                nodes.remove(self_url.replace("http://", "https://"))
            except ValueError:
                pass

            NodesManager.sync()

            try:
                await propagate("add_node", {"url": self_url})
                cousin_nodes = sum(
                    await NodeInterface(url).get_nodes() for url in nodes
                )
                await propagate("add_node", {"url": self_url}, nodes=cousin_nodes)
            except:
                pass
    propagate_txs = await db.get_need_propagate_transactions()
    try:
        response = await call_next(request)
        response.headers["Access-Control-Allow-Origin"] = "*"
        if propagate_txs:
            response.background = BackgroundTask(
                propagate_old_transactions, propagate_txs
            )
        return response
    except:
        raise
        return {"ok": False, "error": "Internal error"}


@app.exception_handler(Exception)
async def exception_handler(request: Request, e: Exception):
    if type(e).__name__ == 'Exception' or type(e).__name__ == 'AssertionError':
        return JSONResponse(
            status_code=500,
            content={"ok": False, "error": f"Exception: {str(e)}"},
        )

    return JSONResponse(
        status_code=500,
        content={"ok": False, "error": f"Uncaught {type(e).__name__} exception"},
    )


transactions_cache = deque(maxlen=100)


async def verify_and_push_tx(tx: Transaction, request: Request,
                             background_tasks: BackgroundTasks):
    tx_hash = tx.hash()
    if tx_hash in transactions_cache:
        return {"ok": False, "error": "Transaction just added"}
    try:
        if await db.add_pending_transaction(tx):
            if "Sender-Node" in request.headers:
                NodesManager.update_last_message(request.headers["Sender-Node"])
            background_tasks.add_task(propagate, "push_tx", {"tx_hex": tx.hex()})
            transactions_cache.append(tx_hash)
            return {"ok": True, "result": "Transaction has been accepted", "tx_hash": tx_hash}
        else:
            return {"ok": False, "error": "Transaction has not been added"}
    except UniqueViolationError:
        return {"ok": False, "error": "Transaction already present"}


@app.get("/push_tx")
@app.post("/push_tx")
async def push_tx(
    request: Request,
    background_tasks: BackgroundTasks,
    tx_hex: str = None,
    body=Body(False),
):
    if is_syncing:
        return {"ok": False, "error": "Node is already syncing"}
    if body and tx_hex is None:
        tx_hex = body["tx_hex"]
    tx = await Transaction.from_hex(tx_hex)
    result = await verify_and_push_tx(tx, request, background_tasks)
    return result


@app.get("/send_to_address")
@app.post("/send_to_address")
async def send_to_address(
    request: Request,
    background_tasks: BackgroundTasks,
    to_address: str = None,
    amount=None,
    body=Body(False),
    authorization: Annotated[Union[str], Header()] = None,
):
    if body:
        if "to_address" in body:
            to_address = body["to_address"]
        if "amount" in body:
            amount = body["amount"]

    if not to_address or not amount:
        return JSONResponse(
            status_code=422,
            content={"ok": False, "error": f"Missing required params."})

    amount = str(amount)
    current_dir = path.dirname(path.abspath(__file__))
    json_file_path = path.join(current_dir, '..', 'upow_wallet', 'key_pair_list.json')

    selected_private_key = None
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)
        for key in data.get("keys"):
            if key.get("public_key") == authorization:
                selected_private_key = key.get("private_key")

    if not selected_private_key:
        return {"ok": False, "error": "Unauthorized"}
    tx = await create_transaction(
        selected_private_key, to_address, amount, None
    )

    result = await verify_and_push_tx(tx, request, background_tasks)
    return result


@app.post("/push_block")
@app.get("/push_block")
async def push_block(
    request: Request,
    background_tasks: BackgroundTasks,
    block_content: str = "",
    txs="",
    block_no: int = None,
    body=Body(False),
):
    if is_syncing:
        return {"ok": False, "error": "Node is already syncing"}
    if body:
        txs = body["txs"]
        if "block_content" in body:
            block_content = body["block_content"]
        if "id" in body:
            return {"ok": False, "error": "Deprecated"}
        if "block_no" in body:
            block_no = body["block_no"]
    if isinstance(txs, str):
        txs = txs.split(",")
        if txs == [""]:
            txs = []
    previous_hash = split_block_content(block_content)[0]
    next_block_id = await db.get_next_block_id()
    if block_no is None:
        previous_block = await db.get_block(previous_hash)
        if previous_block is None:
            if "Sender-Node" in request.headers:
                background_tasks.add_task(
                    sync_blockchain, request.headers["Sender-Node"]
                )
                return {
                    "ok": False,
                    "error": "Previous hash not found, had to sync according to sender node, block may have been accepted",
                }
            else:
                return {"ok": False, "error": "Previous hash not found"}
        block_no = previous_block["id"] + 1
    if next_block_id < block_no:
        background_tasks.add_task(
            sync_blockchain,
            (
                request.headers["Sender-Node"]
                if "Sender-Node" in request.headers
                else None
            ),
        )
        return {
            "ok": False,
            "error": "Blocks missing, had to sync according to sender node, block may have been accepted",
        }
    if next_block_id > block_no:
        return {"ok": False, "error": "Too old block"}
    final_transactions = []
    hashes = []
    for tx_hex in txs:
        if len(tx_hex) == 64:  # it's an hash
            hashes.append(tx_hex)
        else:
            final_transactions.append(await Transaction.from_hex(tx_hex))
    if hashes:
        pending_transactions = await db.get_pending_transactions_by_hash(hashes)
        if len(pending_transactions) < len(hashes):  # one or more tx not found
            if "Sender-Node" in request.headers:
                background_tasks.add_task(
                    sync_blockchain, request.headers["Sender-Node"]
                )
                return {
                    "ok": False,
                    "error": "Transaction hash not found, had to sync according to sender node, block may have been accepted",
                }
            else:
                return {"ok": False, "error": "Transaction hash not found"}
        final_transactions.extend(pending_transactions)
    error_list = []
    if not await create_block(block_content, final_transactions, error_list=error_list):
        return {"ok": False, "error": error_list[0]} if error_list else {"ok": False}

    if "Sender-Node" in request.headers:
        NodesManager.update_last_message(request.headers["Sender-Node"])

    background_tasks.add_task(
        propagate,
        "push_block",
        {
            "block_content": block_content,
            "txs": (
                [tx.hex() for tx in final_transactions]
                if len(final_transactions) < 10
                else txs
            ),
            "block_no": block_no,
        },
    )
    return {"ok": True}


@app.get("/sync_blockchain")
@limiter.limit("10/minute")
async def sync(request: Request, node_url: str = None):
    global is_syncing
    if is_syncing:
        return {"ok": False, "error": "Node is already syncing"}
    is_syncing = True
    upow.helpers.is_blockchain_syncing = True
    await sync_blockchain(node_url)
    is_syncing = False
    upow.helpers.is_blockchain_syncing = False


LAST_PENDING_TRANSACTIONS_CLEAN = [0]


@app.get("/get_mining_info")
@limiter.limit("30/minute")
async def get_mining_info(request: Request, background_tasks: BackgroundTasks):
    Manager.difficulty = None
    difficulty, last_block = await get_difficulty()
    pending_transactions = await db.get_pending_transactions_limit(hex_only=True)
    pending_transactions = sorted(pending_transactions)
    if LAST_PENDING_TRANSACTIONS_CLEAN[0] < timestamp() - 600:
        print(LAST_PENDING_TRANSACTIONS_CLEAN[0])
        LAST_PENDING_TRANSACTIONS_CLEAN[0] = timestamp()
        background_tasks.add_task(clear_pending_transactions, pending_transactions)
    return {
        "ok": True,
        "result": {
            "difficulty": difficulty,
            "last_block": last_block,
            "pending_transactions": pending_transactions[:10],
            "pending_transactions_hashes": [sha256(tx) for tx in pending_transactions],
            "merkle_root": get_transactions_merkle_tree(pending_transactions[:10]),
        },
    }


@app.get("/get_validators_info")
async def get_validators_info(
    background_tasks: BackgroundTasks,
    inode: str = None,
    offset: int = 0,
    limit: int = Query(default=100, le=1000),
):
    if inode:
        inode_ballot = await db.get_inode_ballot_by_address(offset, limit, inode=inode)
    else:
        inode_ballot = await db.get_inode_ballot(offset, limit)
    result_dict = defaultdict(lambda: {"validator": "", "vote": []})

    for tx_hash, inode_address, votes, validator, index in inode_ballot:
        result_dict[validator]["validator"] = validator
        result_dict[validator]["vote"].append(
            {
                "wallet": inode_address,
                "vote_count": votes,
                "tx_hash": tx_hash,
                "index": index,
            }
        )
        result_dict[validator]["totalStake"] = await db.get_validators_stake(
            validator, check_pending_txs=True
        )
    result_list = list(result_dict.values())
    return result_list


@app.get("/get_delegates_info")
async def get_delegates_info(
    background_tasks: BackgroundTasks,
    validator: str = None,
    offset: int = 0,
    limit: int = Query(default=100, le=1000),
):
    if validator:
        validator_ballot = await db.get_validator_ballot_by_address(
            offset, limit, validator=validator
        )
    else:
        validator_ballot = await db.get_validator_ballot(offset, limit)

    result_dict = defaultdict(lambda: {"delegate": "", "vote": []})

    for tx_hash, validator_address, votes, delegate, index in validator_ballot:
        result_dict[delegate]["delegate"] = delegate
        result_dict[delegate]["vote"].append(
            {
                "wallet": validator_address,
                "vote_count": votes,
                "tx_hash": tx_hash,
                "index": index,
            }
        )
        result_dict[delegate]["totalStake"] = await db.get_address_stake(
            delegate, check_pending_txs=True
        )

    result_list = list(result_dict.values())

    return result_list


@app.get("/get_address_info")
@limiter.limit("5/minute")
async def get_address_info(
    request: Request,
    address: str,
    transactions_count_limit: int = Query(default=5, le=50),
    page: int = Query(default=1, ge=1),
    show_pending: bool = False,
    verify: bool = False,
    stake_outputs: bool = False,
    delegate_spent_votes: bool = False,
    delegate_unspent_votes: bool = False,
    address_state: bool = False,
    inode_registration_outputs: bool = False,
    validator_unspent_votes: bool = False,
    validator_spent_votes: bool = False,
):
    outputs = await db.get_spendable_outputs(address)
    stake = await db.get_address_stake(address)
    balance = sum(output.amount for output in outputs)

    offset = (page - 1) * transactions_count_limit
    transactions = (
        await db.get_address_transactions(
            address,
            limit=transactions_count_limit,
            offset=offset,
            check_signatures=True,
        )
        if transactions_count_limit > 0
        else []
    )
    return {
        "ok": True,
        "result": {
            "balance": "{:f}".format(balance),
            "stake": "{:f}".format(stake),
            "spendable_outputs": [
                {
                    "amount": "{:f}".format(output.amount),
                    "tx_hash": output.tx_hash,
                    "index": output.index,
                }
                for output in outputs
            ],
            "transactions": [
                await db.get_nice_transaction(tx.hash(), address if verify else None)
                for tx in transactions
            ],
            "pending_transactions": (
                [
                    await db.get_nice_transaction(
                        tx.hash(), address if verify else None
                    )
                    for tx in await db.get_address_pending_transactions(address, True)
                ]
                if show_pending
                else None
            ),
            "pending_spent_outputs": (
                await db.get_address_pending_spent_outputs(address)
                if show_pending
                else None
            ),
            "stake_outputs": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_stake_outputs(address)
                ]
                if stake_outputs
                else None
            ),
            "delegate_spent_votes": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_delegates_spent_votes(address)
                ]
                if delegate_spent_votes
                else None
            ),
            "delegate_unspent_votes": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_delegates_voting_power(address)
                ]
                if delegate_unspent_votes
                else None
            ),
            "inode_registration_outputs": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_inode_registration_outputs(address)
                ]
                if inode_registration_outputs
                else None
            ),
            "validator_unspent_votes": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_validators_voting_power(address)
                ]
                if validator_unspent_votes
                else None
            ),
            "validator_spent_votes": (
                [
                    {
                        "amount": "{:f}".format(output.amount),
                        "tx_hash": output.tx_hash,
                        "index": output.index,
                    }
                    for output in await db.get_validators_spent_votes(address)
                ]
                if validator_spent_votes
                else None
            ),
            "is_inode": (
                await db.is_inode_registered(address) if address_state else None
            ),
            "is_inode_active": (
                any(
                    entry.get("wallet") == address
                    for entry in await db.get_active_inodes(check_pending_txs=True)
                )
                if address_state
                else None
            ),
            "is_validator": (
                await db.is_validator_registered(address) if address_state else None
            ),
        },
    }


@app.get("/get_address_transactions")
@limiter.limit("10/second")
async def get_address_transactions(
        request: Request,
        address: str,
        page: int = Query(default=1, ge=1),
        limit: int = Query(default=5, le=20)):
    offset = (page - 1) * limit
    transactions = (
        await db.get_address_transactions(
            address,
            limit=limit,
            offset=offset,
            check_signatures=True,
        )
        if limit > 0
        else []
    )

    return {"ok": True,
            "result": {
                "transactions": [
                    await db.get_nice_transaction(tx.hash())
                    for tx in transactions
                ]
            }
            }


@app.get("/add_node")
@limiter.limit("10/minute")
async def add_node(request: Request, url: str, background_tasks: BackgroundTasks):
    nodes = NodesManager.get_nodes()
    url = url.strip("/")
    if url == self_url:
        return {"ok": False, "error": "Recursively adding node"}
    if url in nodes:
        return {"ok": False, "error": "Node already present"}
    else:
        try:
            assert await NodesManager.is_node_working(url)
            background_tasks.add_task(propagate, "add_node", {"url": url}, url)
            NodesManager.add_node(url)
            return {"ok": True, "result": "Node added"}
        except Exception as e:
            print(e)
            return {"ok": False, "error": "Could not add node"}


@app.get("/get_nodes")
async def get_nodes():
    return {"ok": True, "result": NodesManager.get_recent_nodes()[:100]}


@app.get("/get_pending_transactions")
async def get_pending_transactions():
    return {
        "ok": True,
        "result": [tx.hex() for tx in await db.get_pending_transactions_limit(1000)],
    }


@app.get("/get_transaction")
@limiter.limit("2/second")
async def get_transaction(request: Request, tx_hash: str, verify: bool = False):
    tx = await db.get_nice_transaction(tx_hash)
    if tx is None:
        return {"ok": False, "error": "Transaction not found"}
    return {"ok": True, "result": tx}


@app.get("/get_block")
@limiter.limit("30/minute")
async def get_block(request: Request, block: str, full_transactions: bool = False):
    if block.isdecimal():
        block_info = await db.get_block_by_id(int(block))
        if block_info is not None:
            block_hash = block_info["hash"]
        else:
            return {"ok": False, "error": "Block not found"}
    else:
        block_hash = block
        block_info = await db.get_block(block_hash)
    if block_info:
        return {
            "ok": True,
            "result": {
                "block": block_info,
                "transactions": (
                    await db.get_block_transactions(block_hash, hex_only=True)
                    if not full_transactions
                    else None
                ),
                "full_transactions": (
                    await db.get_block_nice_transactions(block_hash)
                    if full_transactions
                    else None
                ),
            },
        }
    else:
        return {"ok": False, "error": "Block not found"}


@app.get("/get_block_details")
@limiter.limit("10/minute")
async def get_block_details(request: Request, block: str):
    if block.isdecimal():
        block_info = await db.get_block_by_id(int(block))
        if block_info is not None:
            block_hash = block_info["hash"]
        else:
            return {"ok": False, "error": "Block not found"}
    else:
        block_hash = block
        block_info = await db.get_block(block_hash)
    if block_info:
        return {
            "ok": True,
            "result": {
                "block": block_info,
                "transactions": [
                    await db.get_nice_transaction(tx_hash)
                    for tx_hash in await db.get_block_transactions_hashes(block_hash)
                ],
            },
        }
    else:
        return {"ok": False, "error": "Block not found"}


@app.get("/get_blocks")
@limiter.limit("40/minute")
async def get_blocks(
    request: Request, offset: int, limit: int = Query(default=..., le=1000)
):
    blocks = await db.get_blocks(offset, limit)
    return {"ok": True, "result": blocks}


@app.get("/get_blocks_details")
@limiter.limit("10/minute")
async def get_blocks_details(
    request: Request, offset: int, limit: int = Query(default=..., le=1000)
):
    blocks = await db.get_blocks(offset, limit, tx_details=True)
    return {"ok": True, "result": blocks}


@app.get("/dobby_info")
@limiter.limit("10/minute")
async def dobby_info(request: Request):
    # inode_with_vote = await db.get_all_registered_inode_with_vote(check_pending_txs=True)
    inode_with_vote = await db.get_active_inodes()
    for item in inode_with_vote:
        item["emission"] = str(item["emission"]) + "%"
    return {"ok": True, "result": inode_with_vote}


@app.get("/get_supply_info")
@limiter.limit("20/minute")
async def get_supply_info(request: Request):
    last_block = await db.get_last_block()
    last_block_id = last_block["id"]
    circulating_supply = get_circulating_supply(last_block_id)
    supply_info = {"max_supply": MAX_SUPPLY,
                   "circulating_supply": circulating_supply,
                   "last_block": last_block
                   }
    return {"ok": True, "result": supply_info}
