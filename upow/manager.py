import asyncio
import decimal
import hashlib
from decimal import Decimal
from io import BytesIO
from math import ceil, floor, log
from time import perf_counter
from typing import Tuple, List, Union

from icecream import ic

from .constants import MAX_SUPPLY, ENDIAN, MAX_BLOCK_SIZE_HEX
from .database import Database, emission_details
from .helpers import (
    sha256,
    timestamp,
    bytes_to_string,
    string_to_bytes,
    TransactionType,
    round_up_decimal, round_up_decimal_new,
)
from .my_logger import CustomLogger
from .upow_transactions import CoinbaseTransaction, Transaction, TransactionOutput
from datetime import datetime, timedelta

BLOCK_TIME = 60
BLOCKS_COUNT = Decimal(100)
LAST_BLOCK_FOR_GENESIS_KEY = 10000
START_DIFFICULTY = Decimal("6.0")
cache = {}
cache_expiration = timedelta(minutes=5)
cache_updating = False

_print = print
print = ic
logger = CustomLogger(__name__).get_logger()


def difficulty_to_hashrate_old(difficulty: Decimal) -> int:
    decimal = difficulty % 1 or 1 / 16
    return Decimal(16 ** int(difficulty) * (16 * decimal))


def difficulty_to_hashrate(difficulty: Decimal) -> int:
    decimal = difficulty % 1
    return Decimal(16 ** int(difficulty) * (16 / ceil(16 * (1 - decimal))))


def hashrate_to_difficulty_old(hashrate: int) -> Decimal:
    difficulty = int(log(hashrate, 16))
    if hashrate == 16**difficulty:
        return Decimal(difficulty)
    return Decimal(difficulty + (hashrate / Decimal(16) ** difficulty) / 16)


def hashrate_to_difficulty_wrong(hashrate: int) -> Decimal:
    difficulty = int(log(hashrate, 16))
    if hashrate == 16**difficulty:
        return Decimal(difficulty)
    ratio = hashrate / 16**difficulty

    decimal = 16 / ratio / 16
    decimal = 1 - floor(decimal * 10) / Decimal(10)
    return Decimal(difficulty + decimal)


def hashrate_to_difficulty(hashrate: int) -> Decimal:
    difficulty = int(log(hashrate, 16))
    ratio = hashrate / 16**difficulty

    for i in range(0, 10):
        coeff = 16 / ceil(16 * (1 - i / 10))
        if coeff > ratio:
            decimal = (i - 1) / Decimal(10)
            return Decimal(difficulty + decimal)
        if coeff == ratio:
            decimal = i / Decimal(10)
            return Decimal(difficulty + decimal)

    return Decimal(difficulty) + Decimal("0.9")


async def calculate_difficulty() -> Tuple[Decimal, dict]:
    database = Database.instance
    last_block = await database.get_last_block()
    if last_block is None:
        return START_DIFFICULTY, dict()
    last_block = dict(last_block)
    last_block["address"] = last_block["address"].strip(" ")
    if last_block["id"] < BLOCKS_COUNT:
        return START_DIFFICULTY, last_block

    if last_block["id"] % BLOCKS_COUNT == 0:
        logger.info(f'calculate_difficulty for {last_block["id"]}')
        last_adjust_block = await database.get_block_by_id(
            last_block["id"] - BLOCKS_COUNT + 1
        )
        logger.info(f'calculate_difficulty last_adjust_block {last_adjust_block["id"]}')
        elapsed = last_block["timestamp"] - last_adjust_block["timestamp"]
        logger.info(f'calculate_difficulty elapsed {elapsed}')
        average_per_block = elapsed / BLOCKS_COUNT
        logger.info(f'calculate_difficulty average_per_block {average_per_block}')
        last_difficulty = last_block["difficulty"]
        logger.info(f'calculate_difficulty last_difficulty {last_difficulty}')
        hashrate = difficulty_to_hashrate(last_difficulty)
        logger.info(f'calculate_difficulty difficulty_to_hashrate {hashrate}')
        ratio = BLOCK_TIME / average_per_block
        logger.info(f'calculate_difficulty ratio {ratio}')
        if last_block["id"] >= 180_000:  # from block 180k, allow difficulty to double at most
            ratio = min(ratio, 2)
        hashrate *= ratio
        logger.info(f'calculate_difficulty hashrate *= ratio {hashrate}')
        new_difficulty = hashrate_to_difficulty(hashrate)
        if new_difficulty < START_DIFFICULTY and last_block["id"] >= 590600:
            logger.info(f'new_difficulty < START_DIFFICULTY: new_difficulty {START_DIFFICULTY}')
            return START_DIFFICULTY, last_block
        logger.info(f'calculate_difficulty hashrate_to_difficulty block {last_block["id"]}, new_difficulty {new_difficulty}')
        return new_difficulty, last_block

    # logger.info(f'calculate_difficulty for {last_block["id"]}, difficulty: {last_block["difficulty"]}')
    return last_block["difficulty"], last_block


async def get_difficulty() -> Tuple[Decimal, dict]:
    if Manager.difficulty is None:
        Manager.difficulty = await calculate_difficulty()
    return Manager.difficulty


async def check_block_is_valid(block_content: str, mining_info: tuple = None) -> bool:
    if mining_info is None:
        mining_info = await get_difficulty()
    difficulty, last_block = mining_info

    block_hash = sha256(block_content)

    if "hash" not in last_block:
        return True

    last_block_hash = last_block["hash"]

    decimal = difficulty % 1
    difficulty = floor(difficulty)
    if decimal > 0:
        charset = "0123456789abcdef"
        count = ceil(16 * (1 - decimal))
        return (
            block_hash.startswith(last_block_hash[-difficulty:])
            and block_hash[difficulty] in charset[:count]
        )
    return block_hash.startswith(last_block_hash[-difficulty:])


def get_block_reward(block_no):
    assert block_no > 0
    halving_interval = 1576800  # 3 years in minutes

    nine_halving_interval = 14191200
    if block_no > nine_halving_interval:
        return Decimal(0)
    coins_per_block = 6
    # Calculate the number of halvings that have occurred
    num_halvings = block_no // halving_interval
    if block_no % halving_interval == 0:
        num_halvings = num_halvings - 1
    current_reward = coins_per_block / (2 ** num_halvings)

    return Decimal(current_reward)


def get_inode_rewards(reward, inode_address_details, block_no=1):
    total_percent = sum(entry["emission"] for entry in inode_address_details)
    if not inode_address_details or total_percent <= 0:
        return reward, {}
    miner_reward = reward * Decimal(0.5)
    distribution_reward = reward * Decimal(0.5)
    distributed_rewards = {}
    redistribution_reward = Decimal(0)

    with decimal.localcontext() as ctx:
        ctx.prec = 9 if block_no > 39000 else ctx.prec
        for address_detail in inode_address_details:
            percent = address_detail["emission"]
            address_reward = distribution_reward * Decimal(percent) / Decimal(total_percent)
            if block_no > 39000:
                address_reward = round_up_decimal_new(address_reward)
            else:
                address_reward = round_up_decimal(address_reward)
            if percent >= 1:
                distributed_rewards[address_detail["wallet"]] = address_reward
            else:
                redistribution_reward += (
                    distribution_reward * Decimal(percent) / Decimal(total_percent)
                )

            # Redistribute reward among addresses with 1 percent or more
            if redistribution_reward > 0:
                num_eligible_addresses = sum(
                    1 for entry in inode_address_details if entry["emission"] >= 1
                )
                redistribution_amount = redistribution_reward / num_eligible_addresses
                if block_no > 39000:
                    redistribution_amount = round_up_decimal_new(redistribution_amount)
                else:
                    redistribution_amount = round_up_decimal(redistribution_amount)
                for inode_address_detail in inode_address_details:
                    if inode_address_detail["emission"] >= 1:
                        distributed_rewards[
                            inode_address_detail["wallet"]
                        ] += redistribution_amount

    return miner_reward, distributed_rewards


def get_circulating_supply(block_no):
    halving_interval = 3 * 365 * 24 * 60  # 3 years in minutes
    initial_coins_per_block = 6

    if block_no > halving_interval * 9:
        return Decimal(MAX_SUPPLY)

    # Calculate the circulating supply based on the block number
    circulating_supply = 0
    num_halvings = block_no // halving_interval
    remaining_blocks = block_no % halving_interval
    if remaining_blocks == 0:
        num_halvings = num_halvings - 1
    for i in range(num_halvings + 1):
        current_reward = initial_coins_per_block / (2 ** i)
        if i == num_halvings and remaining_blocks > 0:
            circulating_supply += current_reward * remaining_blocks
        else:
            circulating_supply += current_reward * halving_interval
    return circulating_supply


def __check():
    i = 1
    r = 0
    index = {}
    while n := get_block_reward(i):
        if n not in index:
            index[n] = 0
        index[n] += 1
        i += 1
        r += n

    print(r)
    print(MAX_SUPPLY - r)
    print(index)


async def clear_pending_transactions(transactions=None):
    database: Database = Database.instance
    await database.clear_duplicate_pending_transactions()
    transactions = transactions or await database.get_pending_transactions_limit(
        hex_only=True
    )
    used_inputs = []
    inode_used_inputs = []
    regular_used_inputs = []
    validator_voting_power = []
    delegate_voting_power = []
    inode_ballot_inputs = []
    validator_ballot_inputs = []
    for transaction in transactions:
        if isinstance(transaction, str):
            tx_hash = sha256(transaction)
            transaction = await Transaction.from_hex(
                transaction, check_signatures=False
            )
        else:
            tx_hash = sha256(transaction.hex())
        tx_inputs = [
            (tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs
        ]
        if any(used_input in tx_inputs for used_input in used_inputs):
            await database.remove_pending_transaction(tx_hash)
            print(f"removed {tx_hash}")
            return await clear_pending_transactions()
        used_inputs += tx_inputs
        if transaction.transaction_type == TransactionType.INODE_DE_REGISTRATION:
            inode_used_inputs += tx_inputs
        elif transaction.transaction_type == TransactionType.VOTE_AS_VALIDATOR:
            validator_voting_power += tx_inputs
        elif transaction.transaction_type == TransactionType.VOTE_AS_DELEGATE:
            delegate_voting_power += tx_inputs
        elif transaction.transaction_type == TransactionType.REVOKE_AS_VALIDATOR:
            inode_ballot_inputs += tx_inputs
        elif transaction.transaction_type == TransactionType.REVOKE_AS_DELEGATE:
            validator_ballot_inputs += tx_inputs
        else:
            regular_used_inputs += tx_inputs

    if regular_used_inputs:
        unspent_outputs = await database.get_unspent_outputs(regular_used_inputs)
        await verify_outputs(regular_used_inputs, unspent_outputs)

    if inode_used_inputs:
        inode_outputs = await database.get_inode_outputs(inode_used_inputs)
        await verify_outputs(inode_used_inputs, inode_outputs)

    if validator_voting_power:
        validator_voting_power_outputs = (
            await database.get_validator_voting_power_outputs(validator_voting_power)
        )
        await verify_outputs(validator_voting_power, validator_voting_power_outputs)

    if delegate_voting_power:
        delegate_voting_power_outputs = (
            await database.get_delegates_voting_power_outputs(delegate_voting_power)
        )
        await verify_outputs(delegate_voting_power, delegate_voting_power_outputs)

    if inode_ballot_inputs:
        inode_ballot_outputs = await database.get_inodes_ballot_outputs(
            inode_ballot_inputs
        )
        await verify_outputs(inode_ballot_inputs, inode_ballot_outputs)

    if validator_ballot_inputs:
        validator_ballot_outputs = await database.get_validators_ballot_outputs(
            validator_ballot_inputs
        )
        await verify_outputs(validator_ballot_inputs, validator_ballot_outputs)
    # unspent_outputs = await database.get_unspent_outputs(used_inputs)
    # double_spend_inputs = set(used_inputs) - set(unspent_outputs)
    # if double_spend_inputs == set(used_inputs):
    #     await database.remove_pending_transactions()
    # elif double_spend_inputs:
    #     await database.remove_pending_transactions_by_contains(
    #         [tx_input[0] + bytes([tx_input[1]]).hex() for tx_input in double_spend_inputs])


async def verify_outputs(used_inputs, outputs):
    database: Database = Database.instance

    double_spend_inputs = set(used_inputs) - set(outputs)
    if double_spend_inputs == set(used_inputs):
        await database.remove_pending_transactions()
    elif double_spend_inputs:
        await database.remove_pending_transactions_by_contains(
            [
                tx_input[0] + bytes([tx_input[1]]).hex()
                for tx_input in double_spend_inputs
            ]
        )


def get_transactions_merkle_tree_ordered(transactions: List[Union[Transaction, str]]):
    _bytes = bytes()
    for transaction in transactions:
        _bytes += hashlib.sha256(
            bytes.fromhex(
                transaction.hex()
                if isinstance(transaction, Transaction)
                else transaction
            )
        ).digest()
    return hashlib.sha256(_bytes).hexdigest()


def get_transactions_merkle_tree(transactions: List[Union[Transaction, str]]):
    _bytes = bytes()
    transactions_bytes = []
    for transaction in transactions:
        transactions_bytes.append(
            bytes.fromhex(
                transaction.hex()
                if isinstance(transaction, Transaction)
                else transaction
            )
        )
    for transaction in sorted(transactions_bytes):
        _bytes += hashlib.sha256(transaction).digest()
    return hashlib.sha256(_bytes).hexdigest()


def get_transactions_size(transactions: List[Transaction]):
    return sum(len(transaction.hex()) for transaction in transactions)


def block_to_bytes(last_block_hash: str, block: dict) -> bytes:
    address_bytes = string_to_bytes(block["address"])
    version = bytes([])
    if len(address_bytes) != 64:
        version = bytes([2])
    return (
        version
        + bytes.fromhex(last_block_hash)
        + address_bytes
        + bytes.fromhex(block["merkle_tree"])
        + block["timestamp"].to_bytes(4, byteorder=ENDIAN)
        + int(float(block["difficulty"]) * 10).to_bytes(2, ENDIAN)
        + block["random"].to_bytes(4, ENDIAN)
    )


def split_block_content(block_content: str):
    _bytes = bytes.fromhex(block_content)
    stream = BytesIO(_bytes)
    if len(_bytes) == 138:
        version = 1
    else:
        version = int.from_bytes(stream.read(1), ENDIAN)
        assert version > 1
        if version == 2:
            assert len(_bytes) == 108
        else:
            raise NotImplementedError()
    previous_hash = stream.read(32).hex()
    address = bytes_to_string(stream.read(64 if version == 1 else 33))
    merkle_tree = stream.read(32).hex()
    timestamp = int.from_bytes(stream.read(4), ENDIAN)
    difficulty = int.from_bytes(stream.read(2), ENDIAN) / Decimal(10)
    random = int.from_bytes(stream.read(4), ENDIAN)
    return previous_hash, address, merkle_tree, timestamp, difficulty, random


async def check_block(
    block_content: str, transactions: List[Transaction], mining_info: tuple = None, error_list=None
):
    if error_list is None:
        error_list = []
    if mining_info is None:
        mining_info = await calculate_difficulty()
    difficulty, last_block = mining_info
    block_no = last_block["id"] + 1 if last_block != {} else 1
    previous_hash, address, merkle_tree, content_time, content_difficulty, random = (
        split_block_content(block_content)
    )
    if not await check_block_is_valid(block_content, mining_info):
        error_list.append('block not valid')
        logger.error('block not valid')
        return False

    content_time = int(content_time)
    if last_block != {} and previous_hash != last_block["hash"]:
        error_list.append(error := "Previous hash is not matched")
        logger.error(error)
        return False

    if (last_block["timestamp"] if "timestamp" in last_block else 0) > content_time:
        error_list.append(error := "timestamp younger than previous block")
        logger.error(error)
        return False

    if ((last_block["timestamp"] if "timestamp" in last_block else 0)
            == content_time):
        error_list.append(error := "timestamp younger than previous block")
        logger.error(error)
        return False

    current_timestamp = timestamp()
    if content_time > current_timestamp:
        error_list.append(error := f"timestamp in the future content_time: {content_time}, current_timestamp {current_timestamp}")
        logger.error(error)
        return False

    five_minutes_ago = current_timestamp - 5 * 60  # 5 minutes ago in seconds
    if content_time < five_minutes_ago:
        error_list.append(error := "block older than 5 minutes")
        logger.error(error)
        return False

    database: Database = Database.instance
    transactions = [tx for tx in transactions if isinstance(tx, Transaction)]
    if get_transactions_size(transactions) > MAX_BLOCK_SIZE_HEX:
        error_list.append(error := "block is too big")
        logger.error(error)
        return False

    if transactions:
        check_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type
                not in (
                    TransactionType.INODE_DE_REGISTRATION,
                    TransactionType.VOTE_AS_VALIDATOR,
                    TransactionType.VOTE_AS_DELEGATE,
                    TransactionType.REVOKE_AS_VALIDATOR,
                    TransactionType.REVOKE_AS_DELEGATE,
                )
            ],
            [],
        )

        check_inode_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type == TransactionType.INODE_DE_REGISTRATION
            ],
            [],
        )

        validator_power_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type == TransactionType.VOTE_AS_VALIDATOR
            ],
            [],
        )

        delegate_power_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type == TransactionType.VOTE_AS_DELEGATE
            ],
            [],
        )

        inode_ballot_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type == TransactionType.REVOKE_AS_VALIDATOR
            ],
            [],
        )

        validator_ballot_inputs = sum(
            [
                [(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs]
                for transaction in transactions
                if transaction.transaction_type == TransactionType.REVOKE_AS_DELEGATE
            ],
            [],
        )

        unspent_outputs = await database.get_unspent_outputs(check_inputs)
        unspent_inode_outputs = await database.get_inode_outputs(check_inode_inputs)
        unspent_validator_power_outputs = (
            await database.get_validator_voting_power_outputs(validator_power_inputs)
        )
        unspent_delegate_power_outputs = (
            await database.get_delegates_voting_power_outputs(delegate_power_inputs)
        )
        inode_ballot_outputs = await database.get_inodes_ballot_outputs(
            inode_ballot_inputs
        )
        validator_ballot_outputs = await database.get_validators_ballot_outputs(
            validator_ballot_inputs
        )
        if (
            len(set(check_inputs)) != len(check_inputs)
            or set(check_inputs) - set(unspent_outputs) != set()
        ):
            spent_outputs = set(check_inputs) - set(unspent_outputs)

            if block_no in double_spend_dict:
                allowed_spent_outputs = set(double_spend_dict[block_no])
                # Allowable double-spend check for the current block number
                if spent_outputs - allowed_spent_outputs == set():
                    # All spent outputs in this block are allowable as per double_spend_dict
                    pass
                else:
                    error_list.append(error := f"double spend in block: {block_no}, utxo: {spent_outputs}")
                    logger.error(error)
                    return False
            else:
                error_list.append(error := f"double spend in block: {block_no}, utxo: {spent_outputs}")
                logger.error(error)
                return False

        if (
            len(set(check_inode_inputs)) != len(check_inode_inputs)
            or set(check_inode_inputs) - set(unspent_inode_outputs) != set()
        ):
            error_list.append(error := "double spend in inode transaction in block")
            logger.error(error)
            used_outputs = set(check_inode_inputs) - set(unspent_inode_outputs)
            print(len(used_outputs))
            return False
        if (
            len(set(validator_power_inputs)) != len(validator_power_inputs)
            or set(validator_power_inputs) - set(unspent_validator_power_outputs)
            != set()
        ):
            error_list.append(error := "double spend in validator power transaction in block")
            logger.error(error)
            used_outputs = set(validator_power_inputs) - set(
                unspent_validator_power_outputs
            )
            print(len(used_outputs))
            return False
        if (
            len(set(delegate_power_inputs)) != len(delegate_power_inputs)
            or set(delegate_power_inputs) - set(unspent_delegate_power_outputs) != set()
        ):
            error_list.append(error := "double spend in delegate power transaction in block")
            logger.error(error)
            used_outputs = set(delegate_power_inputs) - set(
                unspent_delegate_power_outputs
            )
            print(len(used_outputs))
            return False
        if (
            len(set(inode_ballot_inputs)) != len(inode_ballot_inputs)
            or set(inode_ballot_inputs) - set(inode_ballot_outputs) != set()
        ):
            error_list.append(error := "double spend in inode_ballot revoke transaction in block")
            logger.error(error)
            used_outputs = set(inode_ballot_inputs) - set(inode_ballot_outputs)
            print(len(used_outputs))
            return False
        if (
            len(set(validator_ballot_inputs)) != len(validator_ballot_inputs)
            or set(validator_ballot_inputs) - set(validator_ballot_outputs) != set()
        ):
            error_list.append(error := "double spend in validators_ballot revoke transaction in block")
            logger.error(error)
            used_outputs = set(validator_ballot_inputs) - set(validator_ballot_outputs)
            print(len(used_outputs))
            return False
        input_txs_hash = sum(
            [
                [tx_input.tx_hash for tx_input in transaction.inputs]
                for transaction in transactions
            ],
            [],
        )
        input_txs = await database.get_transactions_info(input_txs_hash)
        # move after pp('after get_transactions', time.time() - t)
        for transaction in transactions:
            await transaction._fill_transaction_inputs(input_txs)

    for transaction in transactions:
        if not await transaction.verify(check_double_spend=False):
            error_list.append(error := f"transaction {transaction.hash()} has been not verified")
            logger.error(error)
            return False

    # transactions_merkle_tree = (
    #     get_transactions_merkle_tree(transactions)
    #     if block_no >= 22500
    #     else get_transactions_merkle_tree_ordered(transactions)
    # )
    transactions_merkle_tree = get_transactions_merkle_tree(transactions)
    if merkle_tree != transactions_merkle_tree:
        if block_no == 340510 and merkle_tree == '54e7e3fbfe5c3c7b2a74d14efd22a61c231d157b2c5c2476fca67736736b9ac8':
            return True
        error_list.append(error := "merkle tree does not match")
        logger.error(error)
        return False

    return True


async def create_block(
    block_content: str, transactions: List[Transaction], last_block: dict = None, error_list=None
):
    if error_list is None:
        error_list = []
    create_start_time = perf_counter()
    Manager.difficulty = None
    if last_block is None or last_block["id"] % BLOCKS_COUNT == 0:
        difficulty, last_block = await calculate_difficulty()
    else:
        # fixme temp fix
        difficulty, last_block = await get_difficulty()
        # difficulty = Decimal(str(last_block['difficulty']))
    block_no = last_block["id"] + 1 if last_block != {} else 1
    logger.info(f'Creating block no. {block_no}')
    if not await check_block(block_content, transactions, (difficulty, last_block), error_list=error_list):
        return False

    database: Database = Database.instance
    block_hash = sha256(block_content)
    previous_hash, address, merkle_tree, content_time, content_difficulty, random = (
        split_block_content(block_content)
    )

    active_inodes = await database.get_active_inodes()
    update_active_inodes_cache_with_data(active_inodes)

    block_reward = get_block_reward(block_no)
    miner_reward, inode_rewards = get_inode_rewards(block_reward, active_inodes, block_no=block_no)
    genesis_block_content = await database.get_genesis_block()
    if genesis_block_content is not None:
        _, genesis_address, _, _, _, _ = split_block_content(genesis_block_content)
        if address == genesis_address and block_no <= LAST_BLOCK_FOR_GENESIS_KEY:
            pass
        elif inode_rewards:
            pass
        else:
            error_list.append(error := "Emission detail is not formed. Hence you cannot mine currently.")
            logger.error(error)
            return False

    fees = sum(transaction.fees for transaction in transactions)

    coinbase_transaction = CoinbaseTransaction(block_hash, address, miner_reward + fees)
    if inode_rewards:
        coinbase_transaction.outputs.extend(
            [
                TransactionOutput(inode_address, reward)
                for inode_address, reward in inode_rewards.items()
            ]
        )

    # if not coinbase_transaction.outputs[0].verify():
    if not all(tx_output.verify() for tx_output in coinbase_transaction.outputs):
        return False

    await database.add_block(
        block_no,
        block_hash,
        block_content,
        address,
        random,
        difficulty,
        block_reward + fees,
        content_time,
    )
    await database.add_transaction(coinbase_transaction, block_hash)

    try:
        await database.add_transactions(transactions, block_hash)
    except Exception as e:
        logger.error(f"Transaction of {block_no} has not been added in block {e}")
        await database.delete_block(block_no)
        return False
    await database.add_transaction_outputs(transactions + [coinbase_transaction])
    if transactions:
        await database.remove_pending_transactions_by_hash(
            [transaction.hash() for transaction in transactions]
        )
        await database.remove_outputs(transactions)
        await database.remove_pending_spent_outputs(transactions)

    block_duration = perf_counter() - create_start_time
    logger.info(
        f"Added {len(transactions)} transactions in block {block_no}. Reward: {block_reward}, Fees: {fees} "
        f"in {block_duration:.3f} seconds"
    )
    if block_no % 10 == 0:
        unspent_outputs_hash = await database.get_unspent_outputs_hash()
        logger.info(f'unspent_outputs_hash on block no. {block_no}: {unspent_outputs_hash}')
    Manager.difficulty = None
    try:
        inode_power_emission_n_rewards = []

        for inode in active_inodes:
            wallet = inode["wallet"]
            reward = str(inode_rewards.get(wallet, ""))
            inode_power_emission_n_rewards.append({
                "power": str(inode["power"]),
                "emission": str(inode["emission"]),
                "wallet": wallet,
                "inode_reward": reward
            })
        emission_details.set(str(block_no), inode_power_emission_n_rewards)
    except Exception as e:
        logger.error(f'Error in creating block: {block_no} {str(e)}')
        pass
    return True


async def create_block_in_syncing_old(
    block_content: str, transactions: List[Transaction],
        cb_transaction: CoinbaseTransaction,
        last_block: dict = None, error_list=None
):
    if error_list is None:
        error_list = []
    create_start_time = perf_counter()
    Manager.difficulty = None
    if last_block is None or last_block["id"] % BLOCKS_COUNT == 0:
        difficulty, last_block = await calculate_difficulty()
    else:
        # fixme temp fix
        difficulty, last_block = await get_difficulty()
        # difficulty = Decimal(str(last_block['difficulty']))
    block_no = last_block["id"] + 1 if last_block != {} else 1
    logger.info(f"Syncing block no. {block_no}")
    if not await check_block(block_content, transactions, (difficulty, last_block), error_list=error_list):
        return False

    database: Database = Database.instance
    block_hash = sha256(block_content)
    previous_hash, address, merkle_tree, content_time, content_difficulty, random = (
        split_block_content(block_content)
    )

    block_reward = get_block_reward(block_no)

    fees = sum(transaction.fees for transaction in transactions)

    coinbase_transaction = cb_transaction

    # if not coinbase_transaction.outputs[0].verify():
    if not all(tx_output.verify() for tx_output in coinbase_transaction.outputs):
        return False

    await database.add_block(
        block_no,
        block_hash,
        block_content,
        address,
        random,
        difficulty,
        block_reward + fees,
        content_time,
    )
    await database.add_transaction(coinbase_transaction, block_hash)

    try:
        await database.add_transactions(transactions, block_hash)
    except Exception as e:
        logger.error(f"a transaction has not been added in block {block_no}", e)
        await database.delete_block(block_no)
        return False
    # await database.add_unspent_transactions_outputs(transactions + [coinbase_transaction])
    await database.add_transaction_outputs(transactions + [coinbase_transaction])
    if transactions:
        await database.remove_pending_transactions_by_hash(
            [transaction.hash() for transaction in transactions]
        )
        # await database.remove_unspent_outputs(transactions)
        await database.remove_outputs(transactions)
        await database.remove_pending_spent_outputs(transactions)

    block_duration = perf_counter() - create_start_time
    logger.info(
        f"Added {len(transactions)} transactions in block {block_no}. Reward: {block_reward}, Fees: {fees} "
        f"in {block_duration:.3f} seconds"
    )

    if block_no % 10 == 0:
        unspent_outputs_hash = await database.get_unspent_outputs_hash()
        logger.info(f'unspent_outputs_hash on block no. {block_no}: {unspent_outputs_hash}')

    Manager.difficulty = None
    return True

double_spend_dict = {
    286523: [
        ('16c519171bfa7ee7d42af0d84fe731433048a1aedfd5df692b8beaa755ef6eb9', 0),
        ('747d753fcfecdce5d3a080666ff139ca9123d72d2eb529386f2c3f9f4a55f983', 1),
        ('856b36ecd55a3a427cc988550457435ee9dd7580a423bc3177c1d173b50ff101', 1),
        ('af33808f839698734d801e907f1eb1c24c3547d4cdd984ed0f2e41c58c6d1d9a', 1),
        ('db843078e1fd5f1bbf1c2f550f87548df6fe714ccd12a0ba4a1e25e10fea3ae0', 1),
        ('eb10fd11319aeee7a21766b85c89580f6c3f509a6afaf743df717ca91d33e0da', 1)
    ],
    347027: [
        ('4fd22d5ca99eaa044288de9f850385cbf758efdc4967a92623138e986ce4316e', 2),
        ('b88e9beef7559d48d99ea82e71f7c0601981d6972021feb929c04bc7b52368c2', 1),
        ('ed0f9e07d97ab8a5dc7b8e68ad631a5e78f3cfb6ee6f2aa013854caa64a7b1ae', 1),
    ],
    347034: [
        ('047f5c343dcd15a16c44b3f05fe98bc467002405490ecfb517652207e5425858', 2)
    ],
    349122: [
        ('691695269d8baa441b8e1638a17b3b8497295ec8322c750e8b5312768d4b9ce5', 1),
        ('f7894d0cab92445bd1bb7681106d8fb18d9b4af2465db8a73efbdb97431f855f', 1),
    ],
    395735: [
        ('461c359b956773ff97af6d2189ae84bcc52740e077224efc80b8b5826b51cb92', 1),
        ('ef573f3543ef22b087387fd81493cc7bc977adcc1ff4198483a98a67a6d10e6b', 1),
        ('9efcb290e4c24843bab40dc50591680ac897e52a28db62c7594e4a2b07702291', 1),
    ],
    395736: [
        ('d8421370cef17939c4a2b17c21c7674059c0c24766e80d6129c666f11e886e08', 1),
        ('af2422540ef2f4570b998b262c242b37f7f0e44fbadabcb0f52684dd0ce1ace5', 1)
    ]
}


async def update_cache() -> None:
    """Fetch new inodes from the database and update the cache."""
    global cache_updating
    if cache_updating:
        return
    cache_updating = True
    db: Database = Database.instance
    cache['inodes'] = await db.get_active_inodes()
    cache['timestamp'] = datetime.utcnow()
    cache_updating = False


async def update_active_inodes_cache_with_data(active_inodes) -> None:
    cache['inodes'] = active_inodes
    cache['timestamp'] = datetime.utcnow()

async def get_inodes_from_cache() -> List[dict]:
    now = datetime.utcnow()

    # Check if the cache is valid
    if 'inodes' in cache and (now - cache['timestamp']) < cache_expiration:
        # If the cache is valid, return it immediately
        return cache['inodes']

    if not cache:
        await asyncio.create_task(update_cache())
    else:
        asyncio.create_task(update_cache())

    # Optionally, return the last known cache or an empty list while updating
    return cache.get('inodes', [])


class Manager:
    difficulty: Tuple[float, dict] = None
