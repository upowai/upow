import hashlib
import sys
import time
from math import ceil
from multiprocessing import Process

import requests

from upow.constants import ENDIAN
from upow.helpers import string_to_bytes, timestamp


def calculate_merkle_root(transactions):
    return hashlib.sha256(
        b"".join(bytes.fromhex(tx) for tx in transactions)
    ).hexdigest()


MINING_NODE_URL = (
    sys.argv[3].strip("/") + "/" if len(sys.argv) >= 4 else "http://localhost:3006/"
)


def mine_block(
    start_nonce: int = 0, nonce_increment: int = 1, mining_params: dict = None
):
    miner_wallet = sys.argv[1]
    mining_difficulty = mining_params["difficulty"]
    fractional_difficulty = mining_difficulty % 1
    previous_block = mining_params["last_block"]
    previous_block["hash"] = (
        previous_block["hash"]
        if "hash" in previous_block
        else (18_884_643).to_bytes(32, ENDIAN).hex()
    )
    previous_block["id"] = previous_block["id"] if "id" in previous_block else 0
    target_prefix = previous_block["hash"][-int(mining_difficulty) :]

    allowed_chars = "0123456789abcdef"
    if fractional_difficulty > 0:
        char_limit = ceil(16 * (1 - fractional_difficulty))
        allowed_chars = allowed_chars[:char_limit]
        int_difficulty = int(mining_difficulty)

        def is_valid_block(block_data: bytes) -> bool:
            block_digest = hashlib.sha256(block_data).hexdigest()
            return (
                block_digest.startswith(target_prefix)
                and block_digest[int_difficulty] in allowed_chars
            )

    else:

        def is_valid_block(block_data: bytes) -> bool:
            return hashlib.sha256(block_data).hexdigest().startswith(target_prefix)

    wallet_bytes = string_to_bytes(miner_wallet)
    start_time = time.time()
    nonce = start_nonce
    current_timestamp = timestamp()
    pending_tx_hashes = mining_params["pending_transactions_hashes"]
    merkle_root = calculate_merkle_root(pending_tx_hashes)
    assert all(len(tx) == 64 for tx in pending_tx_hashes)
    if start_nonce == 0:
        print(f"difficulty: {mining_difficulty}")
        print(f'block number: {previous_block["id"]}')
        print(f"Confirming {len(pending_tx_hashes)} transactions")
    block_prefix = (
        bytes.fromhex(previous_block["hash"])
        + wallet_bytes
        + bytes.fromhex(merkle_root)
        + current_timestamp.to_bytes(4, byteorder=ENDIAN)
        + int(mining_difficulty * 10).to_bytes(2, ENDIAN)
    )
    if len(wallet_bytes) == 33:
        block_prefix = (2).to_bytes(1, ENDIAN) + block_prefix
    while True:
        search_complete = True
        check_interval = 5000000 * nonce_increment
        while not is_valid_block(
            block_candidate := block_prefix + nonce.to_bytes(4, ENDIAN)
        ):
            if ((nonce := nonce + nonce_increment) - start_nonce) % check_interval == 0:
                elapsed = time.time() - start_time
                print(
                    f"Worker {start_nonce + 1}: "
                    + str(int(nonce / nonce_increment / elapsed / 1000))
                    + "k hash/s"
                )
                if elapsed > 90:
                    search_complete = False
                    break
        if search_complete:
            print(block_candidate.hex())
            print(",".join(pending_tx_hashes))
            response = requests.post(
                MINING_NODE_URL + "push_block",
                json={
                    "block_content": block_candidate.hex(),
                    "txs": pending_tx_hashes,
                    "id": previous_block["id"] + 1,
                },
                timeout=20 + int((len(pending_tx_hashes) or 1) / 3),
            )
            print(result := response.json())
            if result["ok"]:
                print("BLOCK MINED\n\n")
            exit()


def mining_worker(start_nonce: int, nonce_increment: int, mining_params: dict):
    while True:
        try:
            mine_block(start_nonce, nonce_increment, mining_params)
        except Exception:
            raise
            time.sleep(3)


if __name__ == "__main__":
    num_workers = int(sys.argv[2]) if len(sys.argv) >= 3 else 1
    while True:
        print(f"Starting {num_workers} workers")
        mining_info = None
        while mining_info is None:
            try:
                response = requests.get(MINING_NODE_URL + "get_mining_info", timeout=5)
                mining_info = response.json()["result"]
            except Exception as e:
                print(e)
                time.sleep(1)
                pass
        worker_processes = []
        for i in range(1, num_workers + 1):
            print(f"Starting worker n.{i}")
            process = Process(
                target=mining_worker,
                daemon=True,
                args=(i - 1, num_workers, mining_info),
            )
            process.start()
            worker_processes.append(process)
        time_passed = 0
        while all(p.is_alive() for p in worker_processes):
            time.sleep(1)
            time_passed += 1
            if time_passed > 100:
                break
        for p in worker_processes:
            p.terminate()
