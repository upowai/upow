import asyncio
import os
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import mean
from time import perf_counter
from typing import List, Union, Tuple, Dict, Any, Set

import asyncpg
import pickledb
from asyncpg import Connection, Pool, UndefinedColumnError, UndefinedTableError

import upow
from .constants import MAX_BLOCK_SIZE_HEX, SMALLEST
from .helpers import sha256, point_to_string, string_to_point, point_to_bytes, AddressFormat, normalize_block, \
    TransactionType, OutputType, round_up_decimal
from .my_logger import CustomLogger
from .upow_transactions import Transaction, CoinbaseTransaction, TransactionInput

dir_path = os.path.dirname(os.path.realpath(__file__))
emission_details = pickledb.load(dir_path + '/emission_details.json', True)
logger = CustomLogger(__name__).get_logger()


class Database:
    connection: Connection = None
    credentials = {}
    instance = None
    pool: Pool = None
    is_indexed = False

    @staticmethod
    async def create(user='upow', password='', database='upow', host='127.0.0.1', ignore: bool = False):
        self = Database()
        self.pool = await asyncpg.create_pool(
            user=user,
            password=password,
            database=database,
            host=host,
            command_timeout=30,
            min_size=3
        )
        if not ignore:
            async with self.pool.acquire() as connection:
                try:
                    await connection.fetchrow('SELECT outputs_addresses FROM transactions LIMIT 1')
                except UndefinedColumnError:
                    await connection.execute('ALTER TABLE transactions ADD COLUMN outputs_addresses TEXT[];'
                                             'ALTER TABLE transactions ADD COLUMN outputs_amounts BIGINT[];')
                try:
                    await connection.fetchrow('SELECT content FROM blocks LIMIT 1')
                except UndefinedColumnError:
                    await connection.execute('ALTER TABLE blocks ADD COLUMN content TEXT')
                try:
                    await connection.fetchrow('SELECT * FROM pending_spent_outputs LIMIT 1')
                except UndefinedTableError:
                    print('Creating pending_spent_outputs table')
                    await connection.execute("""CREATE TABLE IF NOT EXISTS pending_spent_outputs (
                        tx_hash CHAR(64) REFERENCES transactions(tx_hash) ON DELETE CASCADE,
                        index SMALLINT NOT NULL
                    )""")
                    print('Retrieving pending transactions')
                    txs = await connection.fetch('SELECT tx_hex FROM pending_transactions')
                    print('Adding pending transactions spent outputs')
                    await self.add_transactions_pending_spent_outputs(
                        [await Transaction.from_hex(tx['tx_hex'], False) for tx in txs])
                    print('Done.')
                res = await connection.fetchrow(
                    'SELECT outputs_addresses FROM transactions WHERE outputs_addresses IS NULL AND tx_hash = ANY(SELECT tx_hash FROM unspent_outputs);')
                self.is_indexed = res is None
                try:
                    await connection.fetchrow('SELECT address FROM unspent_outputs LIMIT 1')
                except UndefinedColumnError:
                    await connection.execute('ALTER TABLE unspent_outputs ADD COLUMN address TEXT NULL')
                    await self.set_unspent_outputs_addresses()

                try:
                    await connection.fetchrow('SELECT propagation_time FROM pending_transactions LIMIT 1')
                except UndefinedColumnError:
                    await connection.execute(
                        'ALTER TABLE pending_transactions ADD COLUMN propagation_time TIMESTAMP(0) NOT NULL DEFAULT NOW()')

        Database.instance = self
        return self

    @staticmethod
    async def get():
        if Database.instance is None:
            await Database.create(**Database.credentials)
        return Database.instance

    async def add_pending_transaction(self, transaction: Transaction, verify: bool = True):
        logger.info('Adding in pending transaction')
        if isinstance(transaction, CoinbaseTransaction):
            logger.error('CoinbaseTransaction in add_pending_transaction')
            return False
        tx_hex = transaction.hex()
        if verify and not await transaction.verify_pending():
            logger.error('Error in adding transaction.')
            return False
        async with self.pool.acquire() as connection:
            await connection.execute(
                'INSERT INTO pending_transactions (tx_hash, tx_hex, inputs_addresses, fees) VALUES ($1, $2, $3, $4)',
                sha256(tx_hex),
                tx_hex,
                [point_to_string(await tx_input.get_public_key()) for tx_input in transaction.inputs],
                transaction.fees
            )
        await self.add_transactions_pending_spent_outputs([transaction])
        return True

    async def remove_pending_transaction(self, tx_hash: str):
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM pending_transactions WHERE tx_hash = $1', tx_hash)

    async def remove_pending_transactions_by_hash(self, tx_hashes: List[str]):
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM pending_transactions WHERE tx_hash = ANY($1)', tx_hashes)

    async def remove_pending_transactions(self):
        async with self.pool.acquire() as connection:
            deleted_transactions = await connection.fetch(
                'DELETE FROM pending_transactions RETURNING *'
            )
            await connection.execute('DELETE FROM pending_spent_outputs')
            if deleted_transactions:
                deleted_tx_hashes = [record['tx_hash'] for record in deleted_transactions]
                logger.info(
                    f"remove_pending_transactions: removed {len(deleted_tx_hashes)} transactions: {deleted_tx_hashes}")
            else:
                logger.info("remove_pending_transactions: no transactions to remove")

    async def delete_blockchain(self):
        async with self.pool.acquire() as connection:
            await connection.execute('TRUNCATE transactions, blocks RESTART IDENTITY')

    async def delete_block(self, id: int):
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM blocks WHERE id = $1', id)

    async def delete_blocks(self, offset: int):
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM blocks WHERE id > $1', offset, timeout=600)

    async def remove_blocks(self, block_no: int):
        blocks_to_remove = await self.get_blocks(block_no, 500)
        transactions_to_remove = []
        # cache overwritten tx hashes
        transactions_hashes = []
        for block_to_remove in blocks_to_remove:
            # load transactions of overwritten blocks
            transactions_to_remove.extend(
                [await Transaction.from_hex(tx, False) for tx in block_to_remove['transactions']])
            transactions_hashes.extend([sha256(tx) for tx in block_to_remove['transactions']])
        outputs_to_be_restored = []
        for transaction in transactions_to_remove:
            if isinstance(transaction, Transaction):
                # load outputs that has been spent in the overwritten transactions that has not been generated in the overwritten transactions
                outputs_to_be_restored.extend([(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs if
                                               tx_input.tx_hash not in transactions_hashes])
        async with self.pool.acquire() as connection:
            # delete the blocks, it will also delete transactions and outputs thanks to references
            await connection.execute('DELETE FROM blocks WHERE id >= $1', block_no, timeout=600)
        # add back the outputs to revert the whole chain to the previous state
        await self.add_unspent_outputs(outputs_to_be_restored)
        # add removed transactions to pending transactions, this could be improved by adding only the ones who spend only old inputs
        # for tx in transactions_to_remove:
        #    await self.add_pending_transaction(tx, verify=False)

    async def get_pending_transactions_limit(self, limit: int = MAX_BLOCK_SIZE_HEX, hex_only: bool = False,
                                             check_signatures: bool = True) -> List[Union[Transaction, str]]:
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                f'SELECT tx_hex FROM pending_transactions ORDER BY fees / LENGTH(tx_hex) DESC, LENGTH(tx_hex), tx_hex')
        txs_hex = [tx['tx_hex'] for tx in txs]
        return_txs = []
        size = 0
        for tx in txs_hex:
            if size + len(tx) > limit:
                break
            return_txs.append(tx)
            size += len(tx)
        if hex_only:
            return return_txs
        return [await Transaction.from_hex(tx_hex, check_signatures) for tx_hex in return_txs]

    async def get_need_propagate_transactions(self, last_propagation_delta: int = 600,
                                              limit: int = MAX_BLOCK_SIZE_HEX) -> List[Union[Transaction, str]]:
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                f"SELECT tx_hex, NOW() - propagation_time as delta FROM pending_transactions ORDER BY fees / LENGTH(tx_hex) DESC, LENGTH(tx_hex), tx_hex")
        return_txs = []
        size = 0
        for tx in txs:
            tx_hex = tx['tx_hex']
            if size + len(tx_hex) > limit:
                break
            size += len(tx_hex)
            if tx['delta'].total_seconds() > last_propagation_delta:
                return_txs.append(tx_hex)
        return return_txs

    async def update_pending_transactions_propagation_time(self, txs_hash: List[str]):
        async with self.pool.acquire() as connection:
            await connection.execute("UPDATE pending_transactions SET propagation_time = NOW() WHERE tx_hash = ANY($1)",
                                     txs_hash)

    async def get_next_block_average_fee(self):
        limit = MAX_BLOCK_SIZE_HEX
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                f'SELECT LENGTH(tx_hex) as size, fees FROM pending_transactions ORDER BY fees / LENGTH(tx_hex) DESC, LENGTH(tx_hex) ASC')
        fees = []
        size = 0
        for tx in txs:
            if size + tx['size'] > limit:
                break
            fees.append(tx['fees'])
            size += tx['size']
        return int(mean(fees) * SMALLEST) // Decimal(SMALLEST)

    async def get_pending_blocks_count(self):
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(f'SELECT LENGTH(tx_hex) as size FROM pending_transactions')
        return int(sum([tx['size'] for tx in txs]) / MAX_BLOCK_SIZE_HEX + 1)

    async def clear_duplicate_pending_transactions(self):
        async with self.pool.acquire() as connection:
            await connection.execute(
                'DELETE FROM pending_transactions WHERE tx_hash = ANY(SELECT tx_hash FROM transactions)')

    async def add_transaction(self, transaction: Union[Transaction, CoinbaseTransaction], block_hash: str):
        await self.add_transactions([transaction], block_hash)

    async def add_transactions(self, transactions: List[Union[Transaction, CoinbaseTransaction]], block_hash: str):
        data = []
        for transaction in transactions:
            data.append((
                block_hash,
                transaction.hash(),
                transaction.hex(),
                [point_to_string(await tx_input.get_public_key()) for tx_input in transaction.inputs] if isinstance(
                    transaction, Transaction) else [],
                [tx_output.address for tx_output in transaction.outputs],
                [tx_output.amount * SMALLEST for tx_output in transaction.outputs],
                transaction.fees if isinstance(transaction, Transaction) else 0
            ))
        async with self.pool.acquire() as connection:
            stmt = await connection.prepare(
                'INSERT INTO transactions (block_hash, tx_hash, tx_hex, inputs_addresses, outputs_addresses, outputs_amounts, fees) VALUES ($1, $2, $3, $4, $5, $6, $7)')
            await stmt.executemany(data)

    async def add_block(self, id: int, block_hash: str, block_content: str, address: str, random: int,
                        difficulty: Decimal, reward: Decimal, timestamp: Union[datetime, int]):
        async with self.pool.acquire() as connection:
            stmt = await connection.prepare(
                'INSERT INTO blocks (id, hash, content, address, random, difficulty, reward, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)')
            await stmt.fetchval(
                id,
                block_hash,
                block_content,
                address,
                random,
                difficulty,
                reward,
                timestamp if isinstance(timestamp, datetime) else datetime.utcfromtimestamp(timestamp)
            )
        from .manager import Manager
        Manager.difficulty = None

    async def get_transaction(self, tx_hash: str, check_signatures: bool = True) -> Union[
        Transaction, CoinbaseTransaction]:
        async with self.pool.acquire() as connection:
            res = tx = await connection.fetchrow('SELECT tx_hex, block_hash FROM transactions WHERE tx_hash = $1',
                                                 tx_hash)
        if res is not None:
            tx = await Transaction.from_hex(res['tx_hex'], check_signatures)
            tx.block_hash = res['block_hash']
        return tx

    async def get_transaction_info(self, tx_hash: str) -> dict:
        async with self.pool.acquire() as connection:
            res = await connection.fetchrow('SELECT * FROM transactions WHERE tx_hash = $1', tx_hash)
        return dict(res) if res is not None else None

    async def get_transactions_info(self, tx_hashes: List[str]) -> Dict[str, dict]:
        async with self.pool.acquire() as connection:
            res = await connection.fetch('SELECT * FROM transactions WHERE tx_hash = ANY($1)', tx_hashes)
        return {res['tx_hash']: dict(res) for res in res}

    async def get_pending_transaction(self, tx_hash: str, check_signatures: bool = True) -> Transaction:
        async with self.pool.acquire() as connection:
            res = await connection.fetchrow('SELECT tx_hex FROM pending_transactions WHERE tx_hash = $1', tx_hash)
        return await Transaction.from_hex(res['tx_hex'], check_signatures) if res is not None else None

    async def get_pending_transactions_by_hash(self, hashes: List[str], check_signatures: bool = True) -> List[
        Transaction]:
        async with self.pool.acquire() as connection:
            res = await connection.fetch('SELECT tx_hex FROM pending_transactions WHERE tx_hash = ANY($1)', hashes)
        return [await Transaction.from_hex(tx['tx_hex'], check_signatures) for tx in res]

    async def get_transactions(self, tx_hashes: List[str]):
        async with self.pool.acquire() as connection:
            res = await connection.fetch('SELECT tx_hex FROM transactions WHERE tx_hash = ANY($1)', tx_hashes)
        return {sha256(res['tx_hex']): await Transaction.from_hex(res['tx_hex']) for res in res}

    async def get_transaction_hash_by_contains_multi(self, contains: List[str], ignore: str = None):
        async with self.pool.acquire() as connection:
            if ignore is not None:
                res = await connection.fetchrow(
                    'SELECT tx_hash FROM transactions WHERE tx_hex LIKE ANY($1) AND tx_hash != $2 LIMIT 1',
                    [f"%{contains}%" for contains in contains],
                    ignore
                )
            else:
                res = await connection.fetchrow(
                    'SELECT tx_hash FROM transactions WHERE tx_hex LIKE ANY($1) LIMIT 1',
                    [f"%{contains}%" for contains in contains],
                )
        return res['tx_hash'] if res is not None else None

    async def get_pending_transactions_by_contains(self, contains: str):
        async with self.pool.acquire() as connection:
            res = await connection.fetch(
                'SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE $1 AND tx_hash != $2', f"%{contains}%",
                contains)
        return [await Transaction.from_hex(res['tx_hex']) for res in res] if res is not None else None

    async def remove_pending_transactions_by_contains(self, search: List[str]) -> None:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                deleted_transactions = await connection.fetch(
                    'DELETE FROM pending_transactions WHERE tx_hex LIKE ANY($1) RETURNING *',
                    [f"%{c}%" for c in search]
                )

                if deleted_transactions:
                    deleted_tx_hashes = [record['tx_hash'] for record in deleted_transactions]
                    logger.info(
                        f"remove_pending_transactions_by_contains: removed {len(deleted_tx_hashes)} transactions "
                        f"deleted_tx_hashes: {deleted_tx_hashes}"
                    )
                else:
                    logger.info(
                        f"remove_pending_transactions_by_contains: no transactions matched patterns {search}"
                    )

    async def get_pending_transaction_by_contains_multi(self, contains: List[str], ignore: str = None):
        async with self.pool.acquire() as connection:
            if ignore is not None:
                res = await connection.fetchrow(
                    'SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1) AND tx_hash != $2 LIMIT 1',
                    [f"%{contains}%" for contains in contains],
                    ignore
                )
            else:
                res = await connection.fetchrow(
                    'SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1) LIMIT 1',
                    [f"%{contains}%" for contains in contains],
                )
        return await Transaction.from_hex(res['tx_hex']) if res is not None else None

    async def get_last_block(self) -> dict:
        async with self.pool.acquire() as connection:
            last_block = await connection.fetchrow("SELECT * FROM blocks ORDER BY id DESC LIMIT 1")
        return normalize_block(last_block) if last_block is not None else None

    async def get_next_block_id(self) -> int:
        async with self.pool.acquire() as connection:
            last_id = await connection.fetchval('SELECT id FROM blocks ORDER BY id DESC LIMIT 1', column=0)
        last_id = last_id if last_id is not None else 0
        return last_id + 1

    async def get_block(self, block_hash: str) -> dict:
        async with self.pool.acquire() as connection:
            block = await connection.fetchrow('SELECT * FROM blocks WHERE hash = $1', block_hash)
        return normalize_block(block) if block is not None else None

    async def get_blocks(self, offset: int, limit: int, tx_details: bool = False) -> list:
        async with self.pool.acquire() as connection:
            transactions: list = await connection.fetch(
                f'SELECT tx_hex, tx_hash, block_hash FROM transactions WHERE block_hash = ANY(SELECT hash FROM blocks WHERE id >= $1 ORDER BY id LIMIT $2)',
                offset, limit)
            blocks = await connection.fetch(f'SELECT * FROM blocks WHERE id >= $1 ORDER BY id LIMIT $2', offset, limit)

        index = {block['hash']: [] for block in blocks}
        index_tx_hash = {block['hash']: [] for block in blocks}
        for transaction in transactions:
            index[transaction['block_hash']].append(transaction['tx_hex'])
            index_tx_hash[transaction['block_hash']].append(transaction['tx_hash'])

        result = []
        size = 0
        for block in blocks:
            block = normalize_block(block)
            block_hash = block['hash']
            txs = index[block_hash]
            tx_hashes = index_tx_hash[block_hash]
            size += sum(len(tx) for tx in txs)
            if size > MAX_BLOCK_SIZE_HEX * 8:
                break
            result.append({
                'block': block,
                'transactions': txs if not tx_details else [await self.get_nice_transaction(tx_hash)
                                                            for tx_hash in tx_hashes]
            })
        return result

    async def get_block_by_id(self, block_id: int) -> dict:
        async with self.pool.acquire() as connection:
            block = await connection.fetchrow('SELECT * FROM blocks WHERE id = $1', block_id)
        return normalize_block(block) if block is not None else None

    async def get_block_transactions(self, block_hash: str, check_signatures: bool = True, hex_only: bool = False) -> \
            List[Union[Transaction, CoinbaseTransaction]]:
        async with self.pool.acquire() as connection:
            txs = await connection.fetch('SELECT tx_hex FROM transactions WHERE block_hash = $1', block_hash)
        return [tx['tx_hex'] if hex_only else await Transaction.from_hex(tx['tx_hex'], check_signatures) for tx in txs]

    async def get_block_transactions_hashes(self, block_hash: str):
        async with self.pool.acquire() as connection:
            txs = await connection.fetch('SELECT tx_hash FROM transactions WHERE block_hash = $1', block_hash)
        return [tx['tx_hash'] for tx in txs]

    async def get_block_transaction_hashes(self, block_hash: str) -> List[str]:
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                "SELECT tx_hash FROM transactions WHERE block_hash = $1 AND tx_hex NOT LIKE '%' || block_hash || '%'",
                block_hash)
        return [tx['tx_hash'] for tx in txs]

    async def get_block_nice_transactions(self, block_hash: str) -> List[dict]:
        async with self.pool.acquire() as connection:
            txs = await connection.fetch('SELECT tx_hash, inputs_addresses FROM transactions WHERE block_hash = $1',
                                         block_hash)
        return [{'hash': tx['tx_hash'], 'is_coinbase': not tx['inputs_addresses']} for tx in txs]

    async def add_unspent_outputs(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO unspent_outputs (tx_hash, index) VALUES ($1, $2)', outputs)
            elif len(outputs[0]) == 4:
                await connection.executemany(
                    'INSERT INTO unspent_outputs (tx_hash, index, address, is_stake) VALUES ($1, $2, $3, $4)', outputs)

    async def add_inode_registration_outputs(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO inode_registration_output (tx_hash, index) VALUES ($1, $2)',
                                             outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO inode_registration_output (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_validator_registration_outputs(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany(
                    'INSERT INTO validator_registration_output (tx_hash, index) VALUES ($1, $2)', outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO validator_registration_output (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_validator_voting_power(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO validators_voting_power (tx_hash, index) VALUES ($1, $2)',
                                             outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO validators_voting_power (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_delegates_voting_power(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO delegates_voting_power (tx_hash, index) VALUES ($1, $2)',
                                             outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO delegates_voting_power (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_vote_to_inode_ballots(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO inodes_ballot (tx_hash, index) VALUES ($1, $2)', outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO inodes_ballot (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_vote_to_validators_ballot(self, outputs: List[Tuple[str, int]]) -> None:
        if not outputs:
            return
        async with self.pool.acquire() as connection:
            if len(outputs[0]) == 2:
                await connection.executemany('INSERT INTO validators_ballot (tx_hash, index) VALUES ($1, $2)', outputs)
            elif len(outputs[0]) == 3:
                await connection.executemany(
                    'INSERT INTO validators_ballot (tx_hash, index, address) VALUES ($1, $2, $3)', outputs)

    async def add_pending_spent_outputs(self, outputs: List[Tuple[str, int]]) -> None:
        async with self.pool.acquire() as connection:
            await connection.executemany('INSERT INTO pending_spent_outputs (tx_hash, index) VALUES ($1, $2)', outputs)

    async def add_transactions_pending_spent_outputs(self, transactions: List[Transaction]) -> None:
        outputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        async with self.pool.acquire() as connection:
            await connection.executemany('INSERT INTO pending_spent_outputs (tx_hash, index) VALUES ($1, $2)', outputs)

    async def add_transaction_outputs(self, transactions: List[Transaction]):
        unspent_outputs = sum(
            [[(transaction.hash(), index, output.address, output.is_stake) for index, output in
              enumerate(transaction.outputs) if output.transaction_type in
              (OutputType.REGULAR, OutputType.STAKE, OutputType.UN_STAKE)] for transaction in transactions], [])

        inode_registration_outputs = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.INODE_REGISTRATION] for
             transaction in
             transactions], [])

        validator_registration_outputs = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.VALIDATOR_REGISTRATION] for
             transaction in transactions], [])

        validator_voting_power = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.VALIDATOR_VOTING_POWER] for
             transaction in transactions], [])

        delegate_voting_power = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.DELEGATE_VOTING_POWER] for
             transaction in transactions], [])

        inode_votes = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.VOTE_AS_VALIDATOR] for
             transaction in transactions], [])

        validator_votes = sum(
            [[(transaction.hash(), index, output.address) for index, output in
              enumerate(transaction.outputs) if output.transaction_type == OutputType.VOTE_AS_DELEGATE] for
             transaction in transactions], [])

        if unspent_outputs:
            await self.add_unspent_outputs(unspent_outputs)

        if inode_registration_outputs:
            await self.add_inode_registration_outputs(inode_registration_outputs)

        if validator_voting_power:
            await self.add_validator_voting_power(validator_voting_power)

        if delegate_voting_power:
            await self.add_delegates_voting_power(delegate_voting_power)

        if validator_registration_outputs:
            await self.add_validator_registration_outputs(validator_registration_outputs)

        if inode_votes:
            await self.add_vote_to_inode_ballots(inode_votes)

        if validator_votes:
            await self.add_vote_to_validators_ballot(validator_votes)

    async def add_unspent_transactions_outputs(self, transactions: List[Transaction]) -> None:
        outputs = sum(
            [[(transaction.hash(), index, output.address, output.is_stake) for index, output in
              enumerate(transaction.outputs)] for
             transaction in transactions], [])
        await self.add_unspent_outputs(outputs)

    async def remove_outputs(self, transactions: List[Transaction]):
        inode_inputs = [transaction for transaction in transactions
                        if transaction.transaction_type == TransactionType.INODE_DE_REGISTRATION]

        validator_vote_input = [transaction for transaction in transactions
                                if transaction.transaction_type == TransactionType.VOTE_AS_VALIDATOR]

        delegate_vote_input = [transaction for transaction in transactions
                               if transaction.transaction_type == TransactionType.VOTE_AS_DELEGATE]

        inode_ballot_input = [transaction for transaction in transactions
                              if transaction.transaction_type == TransactionType.REVOKE_AS_VALIDATOR]

        validator_ballot_input = [transaction for transaction in transactions
                                  if transaction.transaction_type == TransactionType.REVOKE_AS_DELEGATE]

        unspent_inputs = [transaction for transaction in transactions if transaction.transaction_type not in
                          (TransactionType.INODE_DE_REGISTRATION, TransactionType.VOTE_AS_VALIDATOR,
                           TransactionType.VOTE_AS_DELEGATE, TransactionType.REVOKE_AS_VALIDATOR,
                           TransactionType.REVOKE_AS_DELEGATE)]

        if inode_inputs:
            await self.remove_inode_registration_output(inode_inputs)
        if unspent_inputs:
            await self.remove_unspent_outputs(unspent_inputs)
        if validator_vote_input:
            await self.remove_validators_voting_power(validator_vote_input)
        if delegate_vote_input:
            await self.remove_delegates_voting_power(delegate_vote_input)
        if inode_ballot_input:
            await self.remove_inode_ballot_votes(inode_ballot_input)
        if validator_ballot_input:
            await self.remove_validator_ballot_votes(validator_ballot_input)

    async def remove_unspent_outputs(self, transactions: List[Transaction], max_retries: int = 3) -> bool:
        """
        Removes unspent outputs with proper error handling and retries.
        Returns True if successful, False otherwise.
        """
        start_time = perf_counter()
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index)
              for tx_input in transaction.inputs]
             for transaction in transactions],
            [])

        if not inputs:
            return True

        retry_delay = 0.2  # initial delay
        try:
            async with self.pool.acquire() as connection:
                # Ensure serializable isolation for consistency
                async with connection.transaction(isolation='serializable'):
                    # Extract the input values into separate arrays
                    tx_hashes, indices = zip(*inputs)

                    # Delete the UTXOs
                    delete_query = """
                    DELETE FROM unspent_outputs 
                    WHERE (tx_hash, index)
                    IN (SELECT * FROM unnest($1::text[], $2::integer[]))
                    RETURNING tx_hash, index
                    """
                    deleted = await connection.fetch(delete_query, tx_hashes, indices)

                    # Verify the deletion
                    if len(deleted) != len(inputs):
                        logger.error(f"Failed to delete all UTXOs: {len(deleted)} of {len(inputs)} deleted")
                        logger.error(f"Not deleted: {set(inputs) - set(deleted)}")
                        return False

                    duration = perf_counter() - start_time
                    logger.info(f"Successfully removed {len(inputs)} unspent outputs in {duration:.3f} seconds")
                    return True

        except Exception as e:
            if max_retries > 0:
                await asyncio.sleep(retry_delay)
                logger.error(f"Retrying remove_unspent_outputs due to error: {type(e).__name__}: {str(e)}")
                return await self.remove_unspent_outputs(transactions, max_retries=max_retries-1)
            else:
                logger.error(f"Un success in remove_unspent_outputs due to error: {type(e).__name__}: {str(e)}")
                return False

    async def remove_inode_registration_output(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    'DELETE FROM inode_registration_output WHERE (tx_hash, index) = ANY($1::tx_output[])',
                    inputs)
        except:
            await self.remove_inode_registration_output(transactions)

    async def remove_validators_voting_power(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    'DELETE FROM validators_voting_power WHERE (tx_hash, index) = ANY($1::tx_output[])',
                    inputs)
        except:
            await self.remove_validators_voting_power(transactions)

    async def remove_delegates_voting_power(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(
                    'DELETE FROM delegates_voting_power WHERE (tx_hash, index) = ANY($1::tx_output[])',
                    inputs)
        except:
            await self.remove_delegates_voting_power(transactions)

    async def remove_inode_ballot_votes(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        try:
            async with self.pool.acquire() as connection:
                await connection.execute('DELETE FROM inodes_ballot WHERE (tx_hash, index) = ANY($1::tx_output[])',
                                         inputs)
        except:
            await self.remove_inode_ballot_votes(transactions)

    async def remove_validator_ballot_votes(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        try:
            async with self.pool.acquire() as connection:
                await connection.execute('DELETE FROM validators_ballot WHERE (tx_hash, index) = ANY($1::tx_output[])',
                                         inputs)
        except:
            await self.remove_validator_ballot_votes(transactions)

    async def remove_pending_spent_outputs(self, transactions: List[Transaction]) -> None:
        inputs = sum(
            [[(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs] for transaction in transactions],
            [])
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM pending_spent_outputs WHERE (tx_hash, index) = ANY($1::tx_output[])',
                                     inputs)

    async def remove_pending_spent_outputs_by_tuple(self, inputs: List[Tuple[str, int]]) -> None:
        async with self.pool.acquire() as connection:
            await connection.execute('DELETE FROM pending_spent_outputs WHERE (tx_hash, index) = ANY($1::tx_output[])',
                                     inputs)

    async def get_unspent_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM unspent_outputs WHERE (tx_hash, index) = ANY($1::tx_output[])', outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_inode_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM inode_registration_output WHERE (tx_hash, index) = ANY($1::tx_output[])',
                outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_validator_voting_power_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM validators_voting_power WHERE (tx_hash, index) = ANY($1::tx_output[])',
                outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_delegates_voting_power_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM delegates_voting_power WHERE (tx_hash, index) = ANY($1::tx_output[])',
                outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_inodes_ballot_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM inodes_ballot WHERE (tx_hash, index) = ANY($1::tx_output[])', outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_validators_ballot_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM validators_ballot WHERE (tx_hash, index) = ANY($1::tx_output[])', outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def get_unspent_outputs_hash(self) -> str:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch('SELECT tx_hash, index FROM unspent_outputs ORDER BY tx_hash, index')
            return sha256(''.join(row['tx_hash'] + bytes([row['index']]).hex() for row in rows))

    async def get_pending_spent_outputs(self, outputs: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
        async with self.pool.acquire() as connection:
            results = await connection.fetch(
                'SELECT tx_hash, index FROM pending_spent_outputs WHERE (tx_hash, index) = ANY($1::tx_output[])',
                outputs)
            return [(row['tx_hash'], row['index']) for row in results]

    async def set_unspent_outputs_addresses(self):
        assert self.is_indexed, 'cannot set unspent outputs addresses if addresses are not indexed'
        async with self.pool.acquire() as connection:
            await connection.execute(
                "UPDATE unspent_outputs SET address = (SELECT outputs_addresses[index + 1] FROM transactions where "
                "tx_hash = unspent_outputs.tx_hash)")

    async def get_unspent_outputs_from_all_transactions(self):
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                'SELECT tx_hex, blocks.id AS block_no FROM transactions INNER JOIN blocks ON (transactions.block_hash '
                '= blocks.hash) ORDER BY blocks.id ASC')
        outputs = set()
        last_block_no = 0
        for tx in txs:
            if tx['block_no'] != last_block_no:
                last_block_no = tx['block_no']
                print(f'{len(outputs)} utxos at block {last_block_no - 1}')
            tx_hash = sha256(tx['tx_hex'])
            transaction = await Transaction.from_hex(tx['tx_hex'], check_signatures=False)
            if isinstance(transaction, Transaction):
                outputs = outputs.difference({(tx_input.tx_hash, tx_input.index) for tx_input in transaction.inputs})
            outputs.update({(tx_hash, index) for index in range(len(transaction.outputs))})  # append
        return list(outputs)

    async def get_address_transactions(self, address: str, check_pending_txs: bool = False,
                                       check_signatures: bool = False, limit: int = 50, offset: int = 0) -> List[
        Union[Transaction, CoinbaseTransaction]]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                'SELECT tx_hex, blocks.id AS block_no FROM transactions INNER JOIN blocks ON (transactions.block_hash '
                '= blocks.hash) WHERE $1 && inputs_addresses OR $1 && outputs_addresses ORDER BY block_no DESC LIMIT '
                '$2 OFFSET $3',
                addresses, limit, offset)
            if check_pending_txs:
                txs = await connection.fetch(
                    "SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1) OR $2 && inputs_addresses",
                    search, addresses) + txs
        return [await Transaction.from_hex(tx['tx_hex'], check_signatures) for tx in txs]

    async def get_address_pending_transactions(self, address: str, check_signatures: bool = False) -> List[
        Union[Transaction, CoinbaseTransaction]]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                "SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1) OR $2 && inputs_addresses", search,
                addresses)
        return [await Transaction.from_hex(tx['tx_hex'], check_signatures) for tx in txs]

    async def get_address_pending_spent_outputs(self, address: str, check_signatures: bool = False) -> List[
        Union[Transaction, CoinbaseTransaction]]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        async with self.pool.acquire() as connection:
            txs = await connection.fetch(
                "SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1) OR $2 && inputs_addresses", search,
                                         addresses)
            txs = [await Transaction.from_hex(tx['tx_hex'], check_signatures) for tx in txs]
        return sum([[{'tx_hash': tx_input.tx_hash, 'index': tx_input.index} for tx_input in tx.inputs] for tx in txs],
                   [])

    async def get_spendable_outputs(self, address: str, check_pending_txs: bool = False) -> List[TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if await connection.fetchrow(
                    'SELECT tx_hash, index FROM unspent_outputs WHERE address IS NULL') is not None:
                await self.set_unspent_outputs_addresses()
            if not check_pending_txs:
                unspent_outputs = await connection.fetch(
                    'SELECT unspent_outputs.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM '
                    'unspent_outputs INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash) '
                    'WHERE address = ANY($1) '
                    'AND (unspent_outputs.is_stake IS NULL OR unspent_outputs.is_stake = FALSE)',
                    addresses)
            else:
                unspent_outputs = await connection.fetch(
                    'SELECT unspent_outputs.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM '
                    'unspent_outputs INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash) '
                    'WHERE address = ANY($1) AND '
                    '(unspent_outputs.is_stake IS NULL OR unspent_outputs.is_stake = FALSE) AND '
                    'CONCAT(unspent_outputs.tx_hash, unspent_outputs.index) != ALL (SELECT CONCAT('
                    'pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs) ',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in unspent_outputs]

    async def get_inode_ballot(self, offset: int, limit: int, check_pending_txs: bool = False) -> List[
        Tuple[Any, Any, Union[Decimal, Any], Any, Any]]:
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, transactions.outputs_amounts[index + 1]
                     AS vote, transactions.inputs_addresses[index + 1] AS validator, index FROM inodes_ballot 
                     INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash)
                     ORDER BY inodes_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset)
            else:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, transactions.outputs_amounts[index + 1]
                    AS vote, transactions.inputs_addresses[index + 1] AS validator, index FROM inodes_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash) AND
                    CONCAT(inodes_ballot.tx_hash, inodes_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)
                    ORDER BY inodes_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, timeout=60)
        return [(tx_hash, inode, Decimal(vote) / SMALLEST, validator, index) for tx_hash, inode, vote, validator, index
                in inode_ballot]

    async def get_inode_ballot_by_address(self, offset: int, limit: int, inode: str, check_pending_txs: bool = False) -> \
            List[Tuple[Any, Any, Union[Decimal, Any], Any, Any]]:
        point = string_to_point(inode)
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, transactions.outputs_amounts[index + 1]
                     AS vote, transactions.inputs_addresses[index + 1] AS validator, index FROM inodes_ballot 
                     INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash)
                     WHERE inodes_ballot.address = ANY($3)
                     ORDER BY inodes_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, addresses)
            else:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, transactions.outputs_amounts[index + 1]
                    AS vote, transactions.inputs_addresses[index + 1] AS validator, index FROM inodes_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash) 
                    WHERE inodes_ballot.address = ANY($3) AND
                    CONCAT(inodes_ballot.tx_hash, inodes_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)
                    ORDER BY inodes_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, addresses, timeout=60)
        return [(tx_hash, inode, Decimal(vote) / SMALLEST, validator, index) for tx_hash, inode, vote, validator, index
                in inode_ballot]

    async def get_inode_ballot_input_by_address(self, validator_address: str, vote_receiver_address: str,
                                                check_pending_txs: bool = False) -> \
            List[TransactionInput]:
        async with self.pool.acquire() as connection:
            point = string_to_point(validator_address)
            search = ['%' + point_to_bytes(string_to_point(validator_address), address_format).hex() + '%' for
                      address_format in
                      list(AddressFormat)]
            addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
            addresses.reverse()
            search.reverse()

            receiver_point = string_to_point(vote_receiver_address)
            receiver_search = ['%' + point_to_bytes(string_to_point(vote_receiver_address), address_format).hex() + '%'
                               for address_format in
                               list(AddressFormat)]
            receiver_addresses = [point_to_string(receiver_point, address_format) for address_format in
                                  list(AddressFormat)]
            receiver_addresses.reverse()
            receiver_search.reverse()
            if not check_pending_txs:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, 
                    transactions.outputs_amounts[index + 1] AS vote, transactions.inputs_addresses[index + 1] 
                    AS validator, index FROM inodes_ballot INNER JOIN transactions ON 
                    (transactions.tx_hash = inodes_ballot.tx_hash) WHERE transactions.inputs_addresses[index + 1] = ANY($1)
                    AND inodes_ballot.address = ANY($2)''',
                    addresses, receiver_addresses)
            else:
                inode_ballot = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, inodes_ballot.address AS inode, 
                    transactions.outputs_amounts[index + 1] AS vote, transactions.inputs_addresses[index + 1] 
                    AS validator, index FROM inodes_ballot INNER JOIN transactions ON 
                    (transactions.tx_hash = inodes_ballot.tx_hash) WHERE transactions.inputs_addresses[index + 1] = ANY($1)
                    AND inodes_ballot.address = ANY($2) AND
                    CONCAT(inodes_ballot.tx_hash, inodes_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, receiver_addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(vote) / SMALLEST, public_key=point)
                for tx_hash, inode, vote, validator, index in inode_ballot]

    async def get_validator_ballot_input_by_address(self, delegate_address: str, vote_receiver_address: str,
                                                    check_pending_txs: bool = False) -> \
            List[TransactionInput]:
        async with self.pool.acquire() as connection:
            point = string_to_point(delegate_address)
            search = ['%' + point_to_bytes(string_to_point(delegate_address), address_format).hex() + '%' for
                      address_format in
                      list(AddressFormat)]
            addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
            addresses.reverse()
            search.reverse()

            receiver_point = string_to_point(vote_receiver_address)
            receiver_search = ['%' + point_to_bytes(string_to_point(vote_receiver_address), address_format).hex() + '%'
                               for address_format in
                               list(AddressFormat)]
            receiver_addresses = [point_to_string(receiver_point, address_format) for address_format in
                                  list(AddressFormat)]
            receiver_addresses.reverse()
            receiver_search.reverse()
            if not check_pending_txs:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS inode, 
                    transactions.outputs_amounts[index + 1] AS vote, transactions.inputs_addresses[index + 1] 
                    AS validator, index FROM validators_ballot INNER JOIN transactions ON 
                    (transactions.tx_hash = validators_ballot.tx_hash) WHERE transactions.inputs_addresses[index + 1] = ANY($1)
                    AND validators_ballot.address = ANY($2)''',
                    addresses, receiver_addresses)
            else:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS inode, 
                    transactions.outputs_amounts[index + 1] AS vote, transactions.inputs_addresses[index + 1] 
                    AS validator, index FROM validators_ballot INNER JOIN transactions ON 
                    (transactions.tx_hash = validators_ballot.tx_hash) WHERE transactions.inputs_addresses[index + 1] = ANY($1)
                    AND validators_ballot.address = ANY($2) AND
                    CONCAT(validators_ballot.tx_hash, validators_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, receiver_addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(vote) / SMALLEST, public_key=point)
                for tx_hash, inode, vote, validator, index in validators_ballot]

    async def get_transaction_time(self, tx_hash):
        async with self.pool.acquire() as connection:
            timestamp_rec = await connection.fetch('''SELECT timestamp FROM blocks INNER JOIN transactions 
            ON (blocks.hash = transactions.block_hash) where transactions.tx_hash = $1''', tx_hash)
            assert timestamp_rec
            timestamp = timestamp_rec[0]['timestamp']
            return timestamp

    async def is_revoke_valid(self, tx_hash) -> bool:
        timestamp = await self.get_transaction_time(tx_hash)
        time_difference = datetime.utcnow() - timestamp
        return time_difference >= timedelta(hours=48)

    async def get_validator_ballot(self, offset: int, limit: int, check_pending_txs: bool = False) -> List[
        Tuple[Any, Any, Union[Decimal, Any], Any, Any]]:
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS validator, 
                    transactions.outputs_amounts[index + 1] 
                    AS vote, transactions.inputs_addresses[index + 1] AS delegate, index FROM validators_ballot  
                    INNER JOIN transactions ON (transactions.tx_hash = validators_ballot.tx_hash)
                    ORDER BY validators_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset)
            else:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS validator, transactions.outputs_amounts[index + 1]
                    AS vote, transactions.inputs_addresses[index + 1] AS delegate, index FROM validators_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = validators_ballot.tx_hash) AND
                    CONCAT(validators_ballot.tx_hash, validators_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)
                    ORDER BY validators_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, timeout=60)
        return [(tx_hash, validator, Decimal(vote) / SMALLEST, delegate, index) for tx_hash, validator, vote, delegate,
                                                                             index in validators_ballot]

    async def get_validator_ballot_by_address(self, offset: int, limit: int, validator: str,
                                              check_pending_txs: bool = False) -> List[
        Tuple[Any, Any, Union[Decimal, Any], Any, Any]]:
        point = string_to_point(validator)
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]

        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS validator, 
                    transactions.outputs_amounts[index + 1] 
                    AS vote, transactions.inputs_addresses[index + 1] AS delegate, index FROM validators_ballot  
                    INNER JOIN transactions ON (transactions.tx_hash = validators_ballot.tx_hash)
                    WHERE validators_ballot.address = ANY($3) 
                    ORDER BY validators_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, addresses)
            else:
                validators_ballot = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, validators_ballot.address AS validator, transactions.outputs_amounts[index + 1]
                    AS vote, transactions.inputs_addresses[index + 1] AS delegate, index FROM validators_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = validators_ballot.tx_hash) 
                    WHERE validators_ballot.address = ANY($3) AND
                    CONCAT(validators_ballot.tx_hash, validators_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)
                    ORDER BY validators_ballot.tx_hash LIMIT $1 OFFSET $2''', limit, offset, addresses, timeout=60)
        return [(tx_hash, validator, Decimal(vote) / SMALLEST, delegate, index) for
                tx_hash, validator, vote, delegate, index in
                validators_ballot]

    async def get_validators_stake(self, validator: str, check_pending_txs: bool = False):
        offset = 0
        limit = 100000
        validator_ballot = await self.get_validator_ballot_by_address(offset, limit, validator=validator,
                                                                      check_pending_txs=check_pending_txs)
        vote_stake_ratio = [(vote * await self.get_address_stake(delegate)) / 10
                            for tx_hash, validator_address, vote, delegate, index in validator_ballot]
        sum_vote_stake_ratio = sum(vote_stake_ratio, Decimal(0))
        sum_vote_stake_ratio = round_up_decimal(sum_vote_stake_ratio)
        return sum_vote_stake_ratio

    async def get_address_balance(self, address: str, check_pending_txs: bool = False) -> Decimal:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        tx_inputs = await self.get_spendable_outputs(address, check_pending_txs=check_pending_txs)
        balance = sum([tx_input.amount for tx_input in tx_inputs], Decimal(0))
        if check_pending_txs:
            async with self.pool.acquire() as connection:
                txs = await connection.fetch('SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1)',
                                             search)
            for tx in txs:
                tx = await Transaction.from_hex(tx['tx_hex'], check_signatures=False)
                for i, tx_output in enumerate(tx.outputs):
                    if tx_output.address in addresses and tx_output.transaction_type is OutputType.REGULAR and \
                            (tx_output.is_stake is False or tx_output.is_stake is None):
                        balance += tx_output.amount
        return balance

    async def get_pending_stake_transaction(self, address: str):
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        pending_stake_transaction = []
        async with self.pool.acquire() as connection:
            txs = await connection.fetch('SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1)',
                                         search)

            for tx in txs:
                tx = await Transaction.from_hex(tx['tx_hex'], check_signatures=False)
                for tx_output in tx.outputs:
                    if tx_output.address == address and tx_output.transaction_type is OutputType.STAKE:
                        pending_stake_transaction.append(tx)

            assert len(pending_stake_transaction) < 2
        return pending_stake_transaction

    async def get_pending_vote_as_delegate_transaction(self, address: str):
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        pending_vote_as_delegate_transaction = []
        async with self.pool.acquire() as connection:
            txs = await connection.fetch('SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1)',
                                         search)

            for tx in txs:
                tx = await Transaction.from_hex(tx['tx_hex'], check_signatures=False)
                if tx.transaction_type == TransactionType.VOTE_AS_DELEGATE \
                        and await tx.inputs[0].get_address() == address:
                    pending_vote_as_delegate_transaction.append(tx)
        return pending_vote_as_delegate_transaction

    async def get_address_stake(self, address: str, check_pending_txs: bool = False) -> Decimal:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        tx_inputs = await self.get_stake_outputs(address, check_pending_txs)
        stake = sum([tx_input.amount for tx_input in tx_inputs], Decimal(0))
        if check_pending_txs:
            async with self.pool.acquire() as connection:
                txs = await connection.fetch('SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1)',
                                             search)
            for tx in txs:
                tx = await Transaction.from_hex(tx['tx_hex'], check_signatures=False)
                for i, tx_output in enumerate(tx.outputs):
                    if tx_output.address in addresses and tx_output.is_stake is True:
                        stake += tx_output.amount
        return stake

    
    async def get_multiple_address_stakes(
        self, addresses: Set[str], check_pending_txs: bool = False
        ) -> Dict[str, Decimal]:
        """Batch process multiple address stakes in a single database query."""
        if not addresses:
            return {}

        # Prepare all points and searches in advance
        address_data = {
            addr: {
                'point': string_to_point(addr),
                'formats': [point_to_string(string_to_point(addr), fmt)
                            for fmt in list(AddressFormat)],
                'searches': ['%' + point_to_bytes(string_to_point(addr), fmt).hex() + '%'
                             for fmt in list(AddressFormat)]
            } for addr in addresses
        }

        # Flatten address formats for SQL query
        all_address_formats = [fmt
                               for addr_data in address_data.values()
                               for fmt in addr_data['formats']]

        async with self.pool.acquire() as connection:
            # Single query to get all unspent outputs
            if await connection.fetchrow(
                    'SELECT tx_hash, index FROM unspent_outputs WHERE address IS NULL'):
                await self.set_unspent_outputs_addresses()

            query = '''
                SELECT address, SUM(transactions.outputs_amounts[index + 1]) as total_amount 
                FROM unspent_outputs 
                INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash)
                WHERE address = ANY($1) 
                AND unspent_outputs.is_stake = TRUE
            '''

            if check_pending_txs:
                query += '''
                    AND CONCAT(unspent_outputs.tx_hash, unspent_outputs.index) != ALL (
                        SELECT CONCAT(pending_spent_outputs.tx_hash, pending_spent_outputs.index) 
                        FROM pending_spent_outputs
                    )
                '''

            query += ' GROUP BY address'

            # Execute single aggregated query
            results = await connection.fetch(query, all_address_formats, timeout=60)

            # Process results into a mapping
            stake_map = defaultdict(Decimal)
            for row in results:
                address = row['address']
                amount = Decimal(row['total_amount']) / SMALLEST
                # Find original address from formatted address
                original_addr = next(
                    addr for addr, data in address_data.items()
                    if address in data['formats']
                )
                stake_map[original_addr] += amount

            # Handle pending transactions if needed
            if check_pending_txs:
                all_searches = [search
                                for addr_data in address_data.values()
                                for search in addr_data['searches']]

                pending_txs = await connection.fetch(
                    'SELECT tx_hex FROM pending_transactions WHERE tx_hex LIKE ANY($1)',
                    all_searches
                )

                for tx_row in pending_txs:
                    tx = await Transaction.from_hex(tx_row['tx_hex'], check_signatures=False)
                    for i, tx_output in enumerate(tx.outputs):
                        if tx_output.is_stake:
                            # Find original address from output address
                            for original_addr, data in address_data.items():
                                if tx_output.address in data['formats']:
                                    stake_map[original_addr] += tx_output.amount

            return dict(stake_map)


    async def get_stake_outputs(self, address: str, check_pending_txs: bool = False) -> List[TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if await connection.fetchrow(
                    'SELECT tx_hash, index FROM unspent_outputs WHERE address IS NULL') is not None:
                await self.set_unspent_outputs_addresses()
            if not check_pending_txs:
                unspent_outputs = await connection.fetch(
                    '''SELECT unspent_outputs.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    unspent_outputs INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash) 
                    WHERE address = ANY($1) AND (unspent_outputs.is_stake = TRUE)''',
                    addresses)
            else:
                unspent_outputs = await connection.fetch(
                    '''SELECT unspent_outputs.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    unspent_outputs INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash) 
                    WHERE address = ANY($1) AND 
                    (unspent_outputs.is_stake = TRUE) AND 
                    CONCAT(unspent_outputs.tx_hash, unspent_outputs.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs) ''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in unspent_outputs]

    async def get_inode_registration_outputs(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                inode_registration_output = await connection.fetch(
                    '''SELECT inode_registration_output.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    inode_registration_output INNER JOIN transactions ON (transactions.tx_hash = 
                    inode_registration_output.tx_hash) WHERE address = ANY($1)''',
                    addresses)
            else:
                inode_registration_output = await connection.fetch(
                    '''SELECT inode_registration_output.tx_hash, index, transactions.outputs_amounts[index + 1] AS 
                    amount FROM inode_registration_output INNER JOIN transactions ON (transactions.tx_hash = 
                    inode_registration_output.tx_hash) WHERE address = ANY($1) AND CONCAT(
                    inode_registration_output.tx_hash, inode_registration_output.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs) ''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in inode_registration_output]

    async def get_genesis_block(self):
        async with self.pool.acquire() as connection:
            genesis_block = await connection.fetch(
                '''SELECT content FROM blocks where id=1''')
            return genesis_block[0]['content'] if genesis_block else None

    async def get_all_registered_inode(self, check_pending_txs: bool = False):
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                registered_inode_address = await connection.fetch(
                    '''SELECT inode_registration_output.address, blocks.timestamp 
                    FROM inode_registration_output
                    INNER JOIN transactions 
                    ON inode_registration_output.tx_hash = transactions.tx_hash
                    INNER JOIN blocks 
                    ON transactions.block_hash = blocks.hash''')
            else:
                registered_inode_address = await connection.fetch(
                    '''SELECT inode_registration_output.address, blocks.timestamp 
                    FROM inode_registration_output
                    INNER JOIN transactions 
                    ON inode_registration_output.tx_hash = transactions.tx_hash
                    INNER JOIN blocks 
                    ON transactions.block_hash = blocks.hash WHERE 
                    CONCAT(inode_registration_output.tx_hash, inode_registration_output.index) != ALL (SELECT 
                    CONCAT(pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    timeout=60)
        return [(address['address'], address['timestamp']) for address in registered_inode_address]

    async def get_active_inodes(self, check_pending_txs: bool = False):
        upow.helpers.getting_active_inodes = True
        inode_with_vote = await self.get_all_registered_inode_with_vote(check_pending_txs)
        total_power = sum(item["power"] for item in inode_with_vote)
        for item in inode_with_vote:
            item["emission"] = (item["power"] / total_power) * 100 if total_power > 0 else item["power"]
            item["emission"] = round_up_decimal(item["emission"], round_up_length='0.01')
            time_difference = datetime.utcnow() - item["registered_at"]
            item["is_active"] = item["emission"] >= 1 or time_difference <= timedelta(hours=48)
        active_inodes = [item for item in inode_with_vote if item["is_active"] is True]
        upow.helpers.getting_active_inodes = False
        return active_inodes

    async def get_inode_vote_ratio_by_address(self, address: str, check_pending_txs: bool = False):
        async with self.pool.acquire() as connection:
            point = string_to_point(address)
            search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                      list(AddressFormat)]
            addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
            addresses.reverse()
            search.reverse()
            if not check_pending_txs:
                inode_ballot = await connection.fetch(
                    '''SELECT transactions.outputs_amounts[index + 1] AS vote, 
                    transactions.inputs_addresses[index + 1] AS validator FROM inodes_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash) 
                    WHERE inodes_ballot.address = ANY($1) ''', addresses)
            else:
                inode_ballot = await connection.fetch(
                    '''SELECT transactions.outputs_amounts[index + 1] AS vote, 
                    transactions.inputs_addresses[index + 1] AS validator FROM inodes_ballot 
                    INNER JOIN transactions ON (transactions.tx_hash = inodes_ballot.tx_hash)
                    WHERE inodes_ballot.address = ANY($1) 
                    AND CONCAT(inodes_ballot.tx_hash, inodes_ballot.index) != ALL (SELECT CONCAT(
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses)
            votes = [((Decimal(vote['vote']) / SMALLEST), vote['validator']) for vote in
                     inode_ballot]  # Calc will be (vote*stake) / 10
            vote_stake_ratio = [(vote * await self.get_validators_stake(validator)) / 10 for vote, validator in votes]
            sum_vote_stake_ratio = sum(vote_stake_ratio, Decimal(0))
            sum_vote_stake_ratio = round_up_decimal(sum_vote_stake_ratio)
            return sum_vote_stake_ratio

    async def get_all_registered_inode_with_vote(self, check_pending_txs: bool = False):
        inode_addresses = await self.get_all_registered_inode(check_pending_txs)
        result_list = [{"wallet": address, "power": await self.get_inode_vote_ratio_by_address(address,
                                                                                               check_pending_txs),
                        "registered_at": timestamp}
                       for address, timestamp in inode_addresses]
        return result_list

    async def is_inode_registered(self, address: str, check_pending_txs: bool = False) -> bool:
        inode_reg = await self.get_inode_registration_outputs(address, check_pending_txs)
        if len(inode_reg) > 0:
            return True
        else:
            return False

    async def get_inode_count(self, check_pending_txs: bool = False):
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                inode_count = await connection.fetch('SELECT COUNT(*) FROM your_table_name')
            else:
                inode_count = await connection.fetch(
                    '''SELECT COUNT(*) FROM inode_registration_output WHERE CONCAT(
                    inode_registration_output.tx_hash, inode_registration_output.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''')
            return inode_count

    async def get_validator_registration_outputs(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                validator_registration_output = await connection.fetch(
                    '''SELECT validator_registration_output.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    validator_registration_output INNER JOIN transactions ON (transactions.tx_hash = 
                    validator_registration_output.tx_hash) WHERE address = ANY($1)''',
                    addresses)
            else:
                validator_registration_output = await connection.fetch(
                    '''SELECT validator_registration_output.tx_hash, index, transactions.outputs_amounts[index + 1] AS 
                    amount FROM validator_registration_output INNER JOIN transactions ON (transactions.tx_hash = 
                    validator_registration_output.tx_hash) WHERE address = ANY($1) AND CONCAT(
                    validator_registration_output.tx_hash, validator_registration_output.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in validator_registration_output]

    async def is_validator_registered(self, address: str, check_pending_txs: bool = False) -> bool:
        inode_reg = await self.get_validator_registration_outputs(address, check_pending_txs)
        if len(inode_reg) > 0:
            return True
        else:
            return False

    async def get_validators_voting_power(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                validators_voting_power = await connection.fetch(
                    '''SELECT validators_voting_power.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    validators_voting_power INNER JOIN transactions ON (transactions.tx_hash = 
                    validators_voting_power.tx_hash) WHERE address = ANY($1)''',
                    addresses)
            else:
                validators_voting_power = await connection.fetch(
                    '''SELECT validators_voting_power.tx_hash, index, transactions.outputs_amounts[index + 1] AS 
                    amount FROM validators_voting_power INNER JOIN transactions ON (transactions.tx_hash = 
                    validators_voting_power.tx_hash) WHERE address = ANY($1) AND CONCAT(
                    validators_voting_power.tx_hash, validators_voting_power.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in validators_voting_power]

    async def get_validators_spent_votes(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                validators_spent_votes = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, index, transactions.outputs_amounts[index + 1]
                    AS amount FROM transactions INNER JOIN inodes_ballot ON (transactions.tx_hash = 
                    inodes_ballot.tx_hash) WHERE transactions.inputs_addresses[index+1] = ANY($1)''',
                    addresses)
            else:
                validators_spent_votes = await connection.fetch(
                    '''SELECT inodes_ballot.tx_hash, index, transactions.outputs_amounts[index + 1]
                    AS amount FROM transactions INNER JOIN inodes_ballot ON (transactions.tx_hash = 
                    inodes_ballot.tx_hash) WHERE transactions.inputs_addresses[index+1] = ANY($1) AND CONCAT(
                    inodes_ballot.tx_hash, inodes_ballot.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in validators_spent_votes]

    async def get_delegates_voting_power(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                delegates_voting_power = await connection.fetch(
                    '''SELECT delegates_voting_power.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM 
                    delegates_voting_power INNER JOIN transactions ON (transactions.tx_hash = 
                    delegates_voting_power.tx_hash) WHERE address = ANY($1)''',
                    addresses)
            else:
                delegates_voting_power = await connection.fetch(
                    '''SELECT delegates_voting_power.tx_hash, index, transactions.outputs_amounts[index + 1] AS 
                    amount FROM delegates_voting_power INNER JOIN transactions ON (transactions.tx_hash = 
                    delegates_voting_power.tx_hash) WHERE address = ANY($1) AND CONCAT(
                    delegates_voting_power.tx_hash, delegates_voting_power.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in delegates_voting_power]

    async def get_delegates_spent_votes(self, address: str, check_pending_txs: bool = False) -> List[
        TransactionInput]:
        point = string_to_point(address)
        search = ['%' + point_to_bytes(string_to_point(address), address_format).hex() + '%' for address_format in
                  list(AddressFormat)]
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        addresses.reverse()
        search.reverse()
        async with self.pool.acquire() as connection:
            if not check_pending_txs:
                delegates_spent_votes = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, index, transactions.outputs_amounts[index + 1]
                    AS amount FROM transactions INNER JOIN validators_ballot ON (transactions.tx_hash = 
                    validators_ballot.tx_hash) WHERE transactions.inputs_addresses[index+1] = ANY($1)''',
                    addresses)
            else:
                delegates_spent_votes = await connection.fetch(
                    '''SELECT validators_ballot.tx_hash, index, transactions.outputs_amounts[index + 1]
                    AS amount FROM transactions INNER JOIN validators_ballot ON (transactions.tx_hash = 
                    validators_ballot.tx_hash) WHERE transactions.inputs_addresses[index+1] = ANY($1) AND CONCAT(
                    validators_ballot.tx_hash, validators_ballot.index) != ALL (SELECT CONCAT( 
                    pending_spent_outputs.tx_hash, pending_spent_outputs.index) FROM pending_spent_outputs)''',
                    addresses, timeout=60)
        return [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                tx_hash, index, amount in delegates_spent_votes]

    async def get_delegates_all_power(self, address: str, check_pending_txs: bool = False):
        delegates_unspent_votes = await self.get_delegates_voting_power(address, check_pending_txs)
        delegates_spent_votes = await self.get_delegates_spent_votes(address, check_pending_txs)
        delegates_unspent_votes.extend(delegates_spent_votes)
        return delegates_unspent_votes

    async def get_address_spendable_outputs_delta(self, address: str, block_no: int) -> Tuple[
        List[TransactionInput], List[TransactionInput]]:
        point = string_to_point(address)
        addresses = [point_to_string(point, address_format) for address_format in list(AddressFormat)]
        async with self.pool.acquire() as connection:
            unspent_outputs = await connection.fetch(
                'SELECT unspent_outputs.tx_hash, index, transactions.outputs_amounts[index + 1] AS amount FROM unspent_outputs INNER JOIN transactions ON (transactions.tx_hash = unspent_outputs.tx_hash) INNER JOIN blocks ON (blocks.hash = transactions.block_hash) WHERE unspent_outputs.address = ANY($1) AND blocks.id >= $2',
                addresses, block_no)
            spending_txs = await connection.fetch(
                'SELECT tx_hex, blocks.id AS block_no FROM transactions INNER JOIN blocks ON (transactions.block_hash = blocks.hash) WHERE $1 = ANY(inputs_addresses) AND blocks.id >= $2 LIMIT $2',
                address, block_no)
        unspent_outputs = [TransactionInput(tx_hash, index, amount=Decimal(amount) / SMALLEST, public_key=point) for
                           tx_hash, index, amount in unspent_outputs]
        spending_txs = [await Transaction.from_hex(tx['tx_hex'], False) for tx in spending_txs]
        spent_outputs = sum([tx.inputs for tx in spending_txs], [])
        return unspent_outputs, spent_outputs

    async def get_nice_transaction(self, tx_hash: str, address: str = None):
        async with self.pool.acquire() as connection:
            is_confirm: bool = True
            res = await connection.fetchrow(
                '''SELECT tx_hex, tx_hash, block_hash, inputs_addresses, blocks.id AS block_no, blocks.timestamp
                FROM transactions INNER JOIN blocks  ON (transactions.block_hash = blocks.hash) WHERE tx_hash = $1''',
                tx_hash)
            if res is None:
                res = await connection.fetchrow(
                    'SELECT tx_hex, tx_hash, inputs_addresses FROM pending_transactions WHERE tx_hash = $1', tx_hash)
                is_confirm = False
        if res is None:
            return None
        tx = await Transaction.from_hex(res['tx_hex'], False)
        if isinstance(tx, CoinbaseTransaction):
            transaction = {'is_coinbase': True, 'hash': res['tx_hash'], 'block_hash': res.get('block_hash'),
                           'block_no': res.get('block_no'),
                           'datetime': res.get('timestamp'),
                           }
        else:
            delta = None
            if address is not None:
                public_key = string_to_point(address)
                delta = 0
                for i, tx_input in enumerate(tx.inputs):
                    if string_to_point(res['inputs_addresses'][i]) == public_key:
                        print('getting related output for delta')
                        delta -= await tx_input.get_amount()
                for tx_output in tx.outputs:
                    if tx_output.public_key == public_key:
                        delta += tx_output.amount
            transaction = {'is_coinbase': False, 'hash': res['tx_hash'], 'block_hash': res.get('block_hash'),
                           'block_no': res.get('block_no'),
                           'datetime': res.get('timestamp'),
                           'message': tx.message.hex() if tx.message is not None else None,
                           'transaction_type': tx.transaction_type.name,
                           'is_confirm': is_confirm,
                           'inputs': [],
                           'delta': delta, 'fees': await tx.get_fees()}
            for i, input in enumerate(tx.inputs):
                transaction['inputs'].append({
                    'index': input.index,
                    'tx_hash': input.tx_hash,
                    'address': res['inputs_addresses'][i],
                    'amount': await input.get_amount()
                })
        transaction['outputs'] = [{'address': output.address, 'amount': output.amount,
                                   'type': output.transaction_type.name} for output in tx.outputs]
        return transaction
