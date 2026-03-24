"""
Block reorg module for the UPOW blockchain.

When a fork is detected during sync, the node must unwind the orphaned chain
back to the common ancestor and restore ALL state that was altered by those blocks:

  Regular UTXOs:
    - unspent_outputs          (spent by every non-coinbase tx type)

  Governance outputs (spent by specific tx types):
    - inode_registration_output   (spent by INODE_DE_REGISTRATION)
    - validators_voting_power     (spent by VOTE_AS_VALIDATOR)
    - delegates_voting_power      (spent by VOTE_AS_DELEGATE)
    - inodes_ballot               (spent by REVOKE_AS_VALIDATOR)
    - validators_ballot           (spent by REVOKE_AS_DELEGATE)

  Mempool:
    - Orphaned non-coinbase transactions whose inputs all reference pre-fork
      (surviving) transactions are re-queued for potential inclusion.

  Caches / side-effects:
    - emission_details pickledb entries for orphaned blocks are removed.
    - Manager.difficulty is invalidated so the next block recalculates it.
    - clear_pending_transactions() purges any mempool entries that are now
      invalid in the restored chain state.

Public API
----------
    from upow.reorg import perform_reorg

    removed = await perform_reorg(block_no)   # returns count of removed blocks
"""

from asyncpg import UniqueViolationError

from .database import Database, emission_details
from .helpers import TransactionType, sha256
from .my_logger import CustomLogger
from .upow_transactions import Transaction, CoinbaseTransaction

logger = CustomLogger(__name__).get_logger()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _enrich_with_addresses(db: Database, inputs: list) -> list:
    if not inputs:
        return []

    unique_hashes = list({tx_hash for tx_hash, _ in inputs})
    async with db.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT tx_hash, outputs_addresses "
            "FROM transactions WHERE tx_hash = ANY($1)",
            unique_hashes,
        )

    addr_map: dict[str, list] = {
        row["tx_hash"]: (row["outputs_addresses"] or []) for row in rows
    }

    enriched = []
    for tx_hash, index in inputs:
        addresses = addr_map.get(tx_hash, [])
        if addresses and index < len(addresses):
            enriched.append((tx_hash, index, addresses[index]))
        else:
            logger.warning(
                f"Reorg: address not found for output ({tx_hash[:16]}…, {index}); "
                "storing with NULL address"
            )
            # Always use a 3-tuple so the batch INSERT path stays uniform.
            enriched.append((tx_hash, index, None))

    return enriched


# ---------------------------------------------------------------------------
# Core reorg function
# ---------------------------------------------------------------------------

async def perform_reorg(block_no: int) -> int:
    
    # Import here to avoid circular imports at module load time.
    from .manager import Manager, clear_pending_transactions

    db = Database.instance

    orphaned_blocks = await db.get_blocks(block_no, 500)
    if not orphaned_blocks:
        logger.info("Reorg: nothing to remove (block_no=%d)", block_no)
        return 0

    first_id = orphaned_blocks[0]["block"]["id"]
    last_id  = orphaned_blocks[-1]["block"]["id"]
    logger.info(
        "Reorg: unwinding %d block(s) [%d..%d]",
        len(orphaned_blocks), first_id, last_id,
    )


    all_txs: list[tuple[str, object]] = []
    orphaned_tx_hashes: set[str] = set()

    for block_info in orphaned_blocks:
        for tx_hex in block_info.get("transactions", []):
            tx = await Transaction.from_hex(tx_hex, check_signatures=False)
            all_txs.append((tx_hex, tx))
            orphaned_tx_hashes.add(sha256(tx_hex))


    regular_to_restore:          list[tuple] = []
    inode_reg_to_restore:        list[tuple] = []
    validator_power_to_restore:  list[tuple] = []
    delegate_power_to_restore:   list[tuple] = []
    inode_ballot_to_restore:     list[tuple] = []
    validator_ballot_to_restore: list[tuple] = []

    # Transactions eligible for mempool re-queuing:
    # non-coinbase txs whose ALL inputs reference pre-fork transactions.
    mempool_candidates: list[tuple[str, object]] = []  # (tx_hex, tx)

    for tx_hex, tx in all_txs:
        if isinstance(tx, CoinbaseTransaction):
            continue

        pre_fork_inputs = [
            (inp.tx_hash, inp.index)
            for inp in tx.inputs
            if inp.tx_hash not in orphaned_tx_hashes
        ]
        all_inputs_pre_fork = len(pre_fork_inputs) == len(tx.inputs)

        tx_type = tx.transaction_type

        if tx_type == TransactionType.INODE_DE_REGISTRATION:
            inode_reg_to_restore.extend(pre_fork_inputs)
        elif tx_type == TransactionType.VOTE_AS_VALIDATOR:
            validator_power_to_restore.extend(pre_fork_inputs)
        elif tx_type == TransactionType.VOTE_AS_DELEGATE:
            delegate_power_to_restore.extend(pre_fork_inputs)
        elif tx_type == TransactionType.REVOKE_AS_VALIDATOR:
            inode_ballot_to_restore.extend(pre_fork_inputs)
        elif tx_type == TransactionType.REVOKE_AS_DELEGATE:
            validator_ballot_to_restore.extend(pre_fork_inputs)
        else:
            regular_to_restore.extend(pre_fork_inputs)

        # Only re-queue transactions whose every input can be satisfied by
        # the restored chain state (avoids FK violations in pending_spent_outputs).
        if all_inputs_pre_fork:
            mempool_candidates.append((tx_hex, tx))


    inode_reg_enriched       = await _enrich_with_addresses(db, inode_reg_to_restore)
    validator_power_enriched = await _enrich_with_addresses(db, validator_power_to_restore)
    delegate_power_enriched  = await _enrich_with_addresses(db, delegate_power_to_restore)
    inode_ballot_enriched    = await _enrich_with_addresses(db, inode_ballot_to_restore)
    validator_ballot_enriched= await _enrich_with_addresses(db, validator_ballot_to_restore)


    async with db.pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM blocks WHERE id >= $1", block_no, timeout=600
            )
            logger.info("Reorg: deleted blocks with id >= %d", block_no)

            if regular_to_restore:
                await conn.executemany(
                    "INSERT INTO unspent_outputs (tx_hash, index) VALUES ($1, $2)",
                    regular_to_restore,
                )
                logger.info(
                    "Reorg: restored %d regular UTXO(s)", len(regular_to_restore)
                )

            if inode_reg_enriched:
                await conn.executemany(
                    "INSERT INTO inode_registration_output (tx_hash, index, address)"
                    " VALUES ($1, $2, $3)",
                    inode_reg_enriched,
                )
                logger.info(
                    "Reorg: restored %d inode_registration_output(s)",
                    len(inode_reg_enriched),
                )

            if validator_power_enriched:
                await conn.executemany(
                    "INSERT INTO validators_voting_power (tx_hash, index, address)"
                    " VALUES ($1, $2, $3)",
                    validator_power_enriched,
                )
                logger.info(
                    "Reorg: restored %d validators_voting_power row(s)",
                    len(validator_power_enriched),
                )

            if delegate_power_enriched:
                await conn.executemany(
                    "INSERT INTO delegates_voting_power (tx_hash, index, address)"
                    " VALUES ($1, $2, $3)",
                    delegate_power_enriched,
                )
                logger.info(
                    "Reorg: restored %d delegates_voting_power row(s)",
                    len(delegate_power_enriched),
                )

            if inode_ballot_enriched:
                await conn.executemany(
                    "INSERT INTO inodes_ballot (tx_hash, index, address)"
                    " VALUES ($1, $2, $3)",
                    inode_ballot_enriched,
                )
                logger.info(
                    "Reorg: restored %d inodes_ballot row(s)",
                    len(inode_ballot_enriched),
                )

            if validator_ballot_enriched:
                await conn.executemany(
                    "INSERT INTO validators_ballot (tx_hash, index, address)"
                    " VALUES ($1, $2, $3)",
                    validator_ballot_enriched,
                )
                logger.info(
                    "Reorg: restored %d validators_ballot row(s)",
                    len(validator_ballot_enriched),
                )

    mempool_added = 0
    for tx_hex, _ in mempool_candidates:
        try:
            tx = await Transaction.from_hex(tx_hex, check_signatures=True)
            await tx.get_fees()  # populate fees before DB insert
            added = await db.add_pending_transaction(tx, verify=False)
            if added:
                mempool_added += 1
        except UniqueViolationError:
            pass  # already in the mempool
        except Exception as exc:
            logger.debug(
                "Reorg: skipped re-queuing tx %s: %s", sha256(tx_hex)[:16], exc
            )

    logger.info(
        "Reorg: re-queued %d / %d orphaned tx(s) to mempool",
        mempool_added, len(mempool_candidates),
    )


    for block_info in orphaned_blocks:
        block_id_str = str(block_info["block"]["id"])
        try:
            if emission_details.get(block_id_str) is not False:
                emission_details.rem(block_id_str)
        except Exception:
            pass  # non-fatal; pickledb entries are informational only


    Manager.difficulty = None

    await clear_pending_transactions()

    logger.info(
        "Reorg: complete — %d block(s) removed, chain tip is now block %d",
        len(orphaned_blocks), block_no - 1,
    )
    return len(orphaned_blocks)
