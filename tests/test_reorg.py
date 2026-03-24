"""
Tests for upow/reorg.py :: perform_reorg()

Uses only the Python standard library (unittest.IsolatedAsyncioTestCase).

Prerequisites
-------------
1.  A PostgreSQL database called ``upow_test`` must exist:

        createdb upow_test
        psql upow_test < schema.sql

2.  Run:

        python -m unittest tests/test_reorg.py -v

"""

import os
import secrets
import unittest
from decimal import Decimal

from fastecdsa import keys

from upow.constants import CURVE, SMALLEST
from upow.database import Database, emission_details
from upow.helpers import OutputType, TransactionType, point_to_string, sha256
from upow.manager import Manager
from upow.reorg import perform_reorg
from upow.upow_transactions import (
    CoinbaseTransaction,
    Transaction,
    TransactionInput,
    TransactionOutput,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = dict(
    user=os.environ.get("UPOW_TEST_USER", "upow"),
    password=os.environ.get("UPOW_TEST_PASSWORD", ""),
    database=os.environ.get("UPOW_TEST_DB", "upow_test"),
    host=os.environ.get("UPOW_TEST_HOST", "127.0.0.1"),
)

_WIPE_SQL = """
TRUNCATE
    pending_spent_outputs,
    pending_transactions,
    inodes_ballot,
    validators_ballot,
    delegates_voting_power,
    validators_voting_power,
    validator_registration_output,
    inode_registration_output,
    unspent_outputs,
    transactions,
    blocks
RESTART IDENTITY CASCADE
"""


# ---------------------------------------------------------------------------
# Wallet helper
# ---------------------------------------------------------------------------


class Wallet:
    """Minimal EC keypair with a compressed address string."""

    def __init__(self):
        self.private_key = keys.gen_private_key(CURVE)
        self.public_key = keys.get_public_key(self.private_key, CURVE)
        self.address: str = point_to_string(self.public_key)


# ---------------------------------------------------------------------------
# Low-level DB helpers
# ---------------------------------------------------------------------------


def _rand_hash() -> str:
    """Return a random 64-hex-char string (looks like SHA-256)."""
    return secrets.token_hex(32)


async def _insert_block(db: Database, block_id: int, block_hash: str, miner_address: str) -> None:
    """Insert a block row directly, bypassing PoW validation."""
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO blocks (id, hash, content, address, random, difficulty, reward, timestamp)
            VALUES ($1, $2, 'test_content', $3, 0, 1.0, 50, NOW())
            """,
            block_id, block_hash, miner_address,
        )


async def _insert_tx_row(
    db: Database,
    block_hash: str,
    tx_hex: str,
    inputs_addresses: list,
    outputs_addresses: list,
    outputs_amounts: list,    # integers already multiplied by SMALLEST
    fees: Decimal = Decimal("0"),
) -> str:
    """Insert a transaction row directly. Returns the computed tx_hash."""
    tx_hash = sha256(tx_hex)
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO transactions
              (block_hash, tx_hash, tx_hex, inputs_addresses,
               outputs_addresses, outputs_amounts, fees)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            block_hash, tx_hash, tx_hex,
            inputs_addresses, outputs_addresses, outputs_amounts, fees,
        )
    return tx_hash


async def _count(db: Database, table: str, tx_hash: str) -> int:
    """Count rows in a governance or UTXO table by tx_hash."""
    async with db.pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT COUNT(*) FROM {table} WHERE tx_hash = $1", tx_hash
        )


async def _pending_count(db: Database) -> int:
    """Count rows in pending_transactions."""
    async with db.pool.acquire() as conn:
        return await conn.fetchval("SELECT COUNT(*) FROM pending_transactions")


# ---------------------------------------------------------------------------
# Transaction builders
# ---------------------------------------------------------------------------


def _make_coinbase(block_hash: str, wallet: Wallet, amount: Decimal = Decimal("50")) -> CoinbaseTransaction:
    return CoinbaseTransaction(block_hash, wallet.address, amount)


def _build_and_sign_tx(
    sender: Wallet,
    source_tx_hash: str,
    source_index: int,
    source_amount: Decimal,
    recipient_address: str,
    output_amount: Decimal,
    output_type: OutputType = OutputType.REGULAR,
    message: bytes = None,
    fee: Decimal = Decimal("0"),
) -> Transaction:
    """
    Build a minimal signed transaction.
    Pass ``message=str(tx_type.value).encode()`` for governance txs.
    ``source_amount`` is stored on the input so get_fees() works without a
    DB round-trip during construction.
    """
    tx_input = TransactionInput(
        source_tx_hash, source_index,
        private_key=sender.private_key,
        public_key=sender.public_key,
        amount=source_amount,
    )
    tx_output = TransactionOutput(recipient_address, output_amount, output_type)
    tx = Transaction([tx_input], [tx_output], message=message)
    tx.fees = fee
    tx.sign([sender.private_key])
    return tx


# ---------------------------------------------------------------------------
# Base test case — handles DB setup/teardown for every test
# ---------------------------------------------------------------------------


class ReorgTestCase(unittest.IsolatedAsyncioTestCase):
    """
    Each test method gets a clean database state:
      - asyncSetUp  wipes all tables and connects
      - asyncTearDown  wipes again and closes the pool
    """

    async def asyncSetUp(self):
        self.db = await Database.create(**DB_CONFIG, ignore=True)
        async with self.db.pool.acquire() as conn:
            await conn.execute(_WIPE_SQL)
        Manager.difficulty = None

    async def asyncTearDown(self):
        async with self.db.pool.acquire() as conn:
            await conn.execute(_WIPE_SQL)
        await self.db.pool.close()
        Database.instance = None

    # ── helpers ─────────────────────────────────────────────────────────────

    async def _delete_utxo(self, tx_hash: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM unspent_outputs WHERE tx_hash = $1", tx_hash
            )

    async def _delete_governance_row(self, table: str, tx_hash: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                f"DELETE FROM {table} WHERE tx_hash = $1", tx_hash
            )

    # ── Tests ────────────────────────────────────────────────────────────────

    async def test_01_nothing_to_reorg_returns_zero(self):
        """perform_reorg returns 0 when no blocks exist at block_no."""
        removed = await perform_reorg(1)
        self.assertEqual(removed, 0)

    # ─────────────────────────────────────────────────────────────────────────
    # Regular UTXO restoration
    # ─────────────────────────────────────────────────────────────────────────

    async def test_02_regular_utxo_restored(self):
        """
        Block 1 (pre-fork):  coinbase → miner (UTXO created).
        Block 2 (orphaned):  spend → recipient (UTXO consumed).
        After reorg(2):      UTXO is back in unspent_outputs.
        """
        miner = Wallet()
        recipient = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        coinbase = _make_coinbase(blk1_hash, miner, Decimal("50"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )
        await self.db.add_unspent_outputs([(cb_hash, 0)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        spend_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("50"), recipient.address, Decimal("50")
        )
        await _insert_tx_row(
            self.db, blk2_hash, spend_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[recipient.address],
            outputs_amounts=[50 * SMALLEST],
        )
        # Simulate block-2 processing: UTXO consumed
        await self._delete_utxo(cb_hash)

        self.assertEqual(await _count(self.db, "unspent_outputs", cb_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "unspent_outputs", cb_hash), 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Governance table restoration — one test per governance tx type
    # ─────────────────────────────────────────────────────────────────────────

    async def test_03_inode_registration_output_restored(self):
        """
        Block 1: inode_reg_tx creates inode_registration_output row.
        Block 2: INODE_DE_REGISTRATION tx consumes it.
        After reorg(2): inode_registration_output row is restored.
        """
        miner = Wallet()
        inode = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        coinbase = _make_coinbase(blk1_hash, miner, Decimal("1000"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[1000 * SMALLEST],
        )
        inode_reg_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("1000"),
            inode.address, Decimal("1000"),
            output_type=OutputType.INODE_REGISTRATION,
        )
        inode_reg_hash = await _insert_tx_row(
            self.db, blk1_hash, inode_reg_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[inode.address],
            outputs_amounts=[1000 * SMALLEST],
        )
        await self.db.add_inode_registration_outputs([(inode_reg_hash, 0, inode.address)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        dereg_tx = _build_and_sign_tx(
            inode, inode_reg_hash, 0, Decimal("1000"),
            miner.address, Decimal("1000"),
            message=str(TransactionType.INODE_DE_REGISTRATION.value).encode(),
        )
        await _insert_tx_row(
            self.db, blk2_hash, dereg_tx.hex(),
            inputs_addresses=[inode.address],
            outputs_addresses=[miner.address],
            outputs_amounts=[1000 * SMALLEST],
        )
        # Governance row consumed
        await self._delete_governance_row("inode_registration_output", inode_reg_hash)

        self.assertEqual(await _count(self.db, "inode_registration_output", inode_reg_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "inode_registration_output", inode_reg_hash), 1)

    async def test_04_validators_voting_power_restored(self):
        """
        Block 1: tx creates a validators_voting_power row.
        Block 2: VOTE_AS_VALIDATOR tx consumes it.
        After reorg(2): validators_voting_power row is restored.
        """
        miner = Wallet()
        validator = Wallet()
        inode = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        coinbase = _make_coinbase(blk1_hash, miner, Decimal("10"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[10 * SMALLEST],
        )
        val_reg_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("10"),
            validator.address, Decimal("10"),
            output_type=OutputType.VALIDATOR_VOTING_POWER,
        )
        val_reg_hash = await _insert_tx_row(
            self.db, blk1_hash, val_reg_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[validator.address],
            outputs_amounts=[10 * SMALLEST],
        )
        await self.db.add_validator_voting_power([(val_reg_hash, 0, validator.address)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        vote_tx = _build_and_sign_tx(
            validator, val_reg_hash, 0, Decimal("10"),
            inode.address, Decimal("1"),
            output_type=OutputType.VOTE_AS_VALIDATOR,
            message=str(TransactionType.VOTE_AS_VALIDATOR.value).encode(),
        )
        await _insert_tx_row(
            self.db, blk2_hash, vote_tx.hex(),
            inputs_addresses=[validator.address],
            outputs_addresses=[inode.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self._delete_governance_row("validators_voting_power", val_reg_hash)

        self.assertEqual(await _count(self.db, "validators_voting_power", val_reg_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "validators_voting_power", val_reg_hash), 1)

    async def test_05_delegates_voting_power_restored(self):
        """
        Block 1: tx creates a delegates_voting_power row.
        Block 2: VOTE_AS_DELEGATE tx consumes it.
        After reorg(2): delegates_voting_power row is restored.
        """
        miner = Wallet()
        delegate = Wallet()
        validator = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        coinbase = _make_coinbase(blk1_hash, miner, Decimal("10"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[10 * SMALLEST],
        )
        del_power_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("10"),
            delegate.address, Decimal("10"),
            output_type=OutputType.DELEGATE_VOTING_POWER,
        )
        del_power_hash = await _insert_tx_row(
            self.db, blk1_hash, del_power_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[delegate.address],
            outputs_amounts=[10 * SMALLEST],
        )
        await self.db.add_delegates_voting_power([(del_power_hash, 0, delegate.address)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        vote_tx = _build_and_sign_tx(
            delegate, del_power_hash, 0, Decimal("10"),
            validator.address, Decimal("1"),
            output_type=OutputType.VOTE_AS_DELEGATE,
            message=str(TransactionType.VOTE_AS_DELEGATE.value).encode(),
        )
        await _insert_tx_row(
            self.db, blk2_hash, vote_tx.hex(),
            inputs_addresses=[delegate.address],
            outputs_addresses=[validator.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self._delete_governance_row("delegates_voting_power", del_power_hash)

        self.assertEqual(await _count(self.db, "delegates_voting_power", del_power_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "delegates_voting_power", del_power_hash), 1)

    async def test_06_inodes_ballot_restored(self):
        """
        Block 1: VOTE_AS_VALIDATOR output creates an inodes_ballot row.
        Block 2: REVOKE_AS_VALIDATOR tx consumes it.
        After reorg(2): inodes_ballot row is restored.
        """
        miner = Wallet()
        inode = Wallet()
        revoker = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        coinbase = _make_coinbase(blk1_hash, miner, Decimal("1"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[1 * SMALLEST],
        )
        # Vote tx: output of type VOTE_AS_VALIDATOR → feeds inodes_ballot
        # output address = the inode being voted for
        vote_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("1"),
            inode.address, Decimal("1"),
            output_type=OutputType.VOTE_AS_VALIDATOR,
        )
        vote_hash = await _insert_tx_row(
            self.db, blk1_hash, vote_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[inode.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self.db.add_vote_to_inode_ballots([(vote_hash, 0, inode.address)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        revoke_tx = _build_and_sign_tx(
            revoker, vote_hash, 0, Decimal("1"),
            miner.address, Decimal("1"),
            message=str(TransactionType.REVOKE_AS_VALIDATOR.value).encode(),
        )
        await _insert_tx_row(
            self.db, blk2_hash, revoke_tx.hex(),
            inputs_addresses=[revoker.address],
            outputs_addresses=[miner.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self._delete_governance_row("inodes_ballot", vote_hash)

        self.assertEqual(await _count(self.db, "inodes_ballot", vote_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "inodes_ballot", vote_hash), 1)

    async def test_07_validators_ballot_restored(self):
        """
        Block 1: VOTE_AS_DELEGATE output creates a validators_ballot row.
        Block 2: REVOKE_AS_DELEGATE tx consumes it.
        After reorg(2): validators_ballot row is restored.
        """
        miner = Wallet()
        validator = Wallet()
        revoker = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        coinbase = _make_coinbase(blk1_hash, miner, Decimal("1"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[1 * SMALLEST],
        )
        # Vote tx: output of type VOTE_AS_DELEGATE → feeds validators_ballot
        # output address = the validator being voted for
        vote_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("1"),
            validator.address, Decimal("1"),
            output_type=OutputType.VOTE_AS_DELEGATE,
        )
        vote_hash = await _insert_tx_row(
            self.db, blk1_hash, vote_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[validator.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self.db.add_vote_to_validators_ballot([(vote_hash, 0, validator.address)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        revoke_tx = _build_and_sign_tx(
            revoker, vote_hash, 0, Decimal("1"),
            miner.address, Decimal("1"),
            message=str(TransactionType.REVOKE_AS_DELEGATE.value).encode(),
        )
        await _insert_tx_row(
            self.db, blk2_hash, revoke_tx.hex(),
            inputs_addresses=[revoker.address],
            outputs_addresses=[miner.address],
            outputs_amounts=[1 * SMALLEST],
        )
        await self._delete_governance_row("validators_ballot", vote_hash)

        self.assertEqual(await _count(self.db, "validators_ballot", vote_hash), 0)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        self.assertEqual(await _count(self.db, "validators_ballot", vote_hash), 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Mempool re-queuing
    # ─────────────────────────────────────────────────────────────────────────

    async def test_08_orphaned_tx_requeued_to_mempool(self):
        """
        A regular spend tx from an orphaned block is re-queued to the mempool
        after reorg and survives clear_pending_transactions() because its UTXO
        was also restored.
        """
        miner = Wallet()
        recipient = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        coinbase = _make_coinbase(blk1_hash, miner, Decimal("50"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )
        await self.db.add_unspent_outputs([(cb_hash, 0)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        # fee=1 so get_fees() returns a non-None Decimal (required by DB NOT NULL)
        spend_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("50"),
            recipient.address, Decimal("49"),
            fee=Decimal("1"),
        )
        await _insert_tx_row(
            self.db, blk2_hash, spend_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[recipient.address],
            outputs_amounts=[49 * SMALLEST],
            fees=Decimal("1"),
        )
        await self._delete_utxo(cb_hash)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        # Spend tx is now in the mempool
        self.assertEqual(await _pending_count(self.db), 1)
        # And the UTXO that it spends was restored
        self.assertEqual(await _count(self.db, "unspent_outputs", cb_hash), 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Cross-chain orphaned tx — must NOT enter mempool
    # ─────────────────────────────────────────────────────────────────────────

    async def test_09_chained_orphan_not_requeued(self):
        """
        tx_a (orphaned, pre-fork input) → eligible for mempool.
        tx_b (orphaned, spends tx_a output) → NOT eligible, since its input
        references another orphaned tx.
        """
        miner = Wallet()
        intermediate = Wallet()
        recipient = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        coinbase = _make_coinbase(blk1_hash, miner, Decimal("50"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )
        await self.db.add_unspent_outputs([(cb_hash, 0)])

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)

        # tx_a: pre-fork input → eligible
        tx_a = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("50"), intermediate.address, Decimal("50")
        )
        tx_a_hash = await _insert_tx_row(
            self.db, blk2_hash, tx_a.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[intermediate.address],
            outputs_amounts=[50 * SMALLEST],
        )

        # tx_b: spends orphaned tx_a → NOT eligible
        tx_b = _build_and_sign_tx(
            intermediate, tx_a_hash, 0, Decimal("50"), recipient.address, Decimal("50")
        )
        await _insert_tx_row(
            self.db, blk2_hash, tx_b.hex(),
            inputs_addresses=[intermediate.address],
            outputs_addresses=[recipient.address],
            outputs_amounts=[50 * SMALLEST],
        )
        await self._delete_utxo(cb_hash)

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)
        # Only tx_a in mempool — tx_b excluded
        self.assertEqual(await _pending_count(self.db), 1)
        self.assertEqual(await _count(self.db, "unspent_outputs", cb_hash), 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Multi-block reorg
    # ─────────────────────────────────────────────────────────────────────────

    async def test_10_multi_block_reorg(self):
        """
        Unwinding 3 orphaned blocks removes them all and restores the UTXO
        that was consumed in one of those blocks.
        """
        miner = Wallet()
        recipient = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        cb = _make_coinbase(blk1_hash, miner, Decimal("50"))
        cb_hash = await _insert_tx_row(
            self.db, blk1_hash, cb.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )
        await self.db.add_unspent_outputs([(cb_hash, 0)])

        # Block 2 (orphaned): spends coinbase
        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        spend_tx = _build_and_sign_tx(
            miner, cb_hash, 0, Decimal("50"), recipient.address, Decimal("50")
        )
        await _insert_tx_row(
            self.db, blk2_hash, spend_tx.hex(),
            inputs_addresses=[miner.address],
            outputs_addresses=[recipient.address],
            outputs_amounts=[50 * SMALLEST],
        )

        # Block 3 (orphaned): coinbase only (no pre-fork inputs to restore)
        blk3_hash = _rand_hash()
        await _insert_block(self.db, 3, blk3_hash, miner.address)
        cb3 = _make_coinbase(blk3_hash, miner, Decimal("25"))
        await _insert_tx_row(
            self.db, blk3_hash, cb3.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[25 * SMALLEST],
        )

        # Block 4 (orphaned): coinbase only
        blk4_hash = _rand_hash()
        await _insert_block(self.db, 4, blk4_hash, miner.address)
        cb4 = _make_coinbase(blk4_hash, miner, Decimal("25"))
        await _insert_tx_row(
            self.db, blk4_hash, cb4.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[25 * SMALLEST],
        )

        await self._delete_utxo(cb_hash)

        removed = await perform_reorg(2)  # unwinds blocks 2, 3, 4

        self.assertEqual(removed, 3)
        self.assertEqual(await _count(self.db, "unspent_outputs", cb_hash), 1)

        # Chain tip is block 1
        async with self.db.pool.acquire() as conn:
            tip = await conn.fetchval("SELECT MAX(id) FROM blocks")
        self.assertEqual(tip, 1)

    # ─────────────────────────────────────────────────────────────────────────
    # Difficulty cache and emission_details
    # ─────────────────────────────────────────────────────────────────────────

    async def test_11_difficulty_cache_invalidated(self):
        """perform_reorg() must set Manager.difficulty = None."""
        miner = Wallet()
        Manager.difficulty = ("stale", "cached", "value")

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        coinbase = _make_coinbase(blk1_hash, miner)
        await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )

        await perform_reorg(1)

        self.assertIsNone(Manager.difficulty)

    async def test_12_emission_details_cleaned(self):
        """
        emission_details pickledb entries for orphaned block IDs are
        removed during reorg.
        """
        miner = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)
        coinbase = _make_coinbase(blk1_hash, miner)
        await _insert_tx_row(
            self.db, blk1_hash, coinbase.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[50 * SMALLEST],
        )

        # Plant a fake emission entry for block 1
        emission_details.set("1", {"fake": "data"})
        self.assertIsNot(emission_details.get("1"), False)

        await perform_reorg(1)

        self.assertIs(emission_details.get("1"), False)

    # ─────────────────────────────────────────────────────────────────────────
    # Coinbase-only orphaned block
    # ─────────────────────────────────────────────────────────────────────────

    async def test_13_coinbase_only_orphaned_block(self):
        """
        An orphaned block that contains only a coinbase transaction
        completes without error (no inputs to restore).
        """
        miner = Wallet()

        blk1_hash = _rand_hash()
        await _insert_block(self.db, 1, blk1_hash, miner.address)

        blk2_hash = _rand_hash()
        await _insert_block(self.db, 2, blk2_hash, miner.address)
        coinbase2 = _make_coinbase(blk2_hash, miner, Decimal("25"))
        await _insert_tx_row(
            self.db, blk2_hash, coinbase2.hex(),
            inputs_addresses=[],
            outputs_addresses=[miner.address],
            outputs_amounts=[25 * SMALLEST],
        )

        removed = await perform_reorg(2)

        self.assertEqual(removed, 1)

        async with self.db.pool.acquire() as conn:
            remaining_blocks = await conn.fetchval("SELECT COUNT(*) FROM blocks WHERE id >= 2")
        self.assertEqual(remaining_blocks, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
