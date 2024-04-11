from decimal import Decimal

from fastecdsa import keys

from upow.constants import CURVE, MAX_INODES
from upow.database import Database
from upow.helpers import point_to_string, TransactionType, OutputType
from upow.upow_transactions import Transaction, TransactionOutput, TransactionInput


async def create_transaction(
    private_key,
    receiving_address,
    amount,
    message: bytes = None,
    send_back_address=None,
):
    database: Database = await Database.get()
    amount = Decimal(amount)
    inputs = []
    sender_address = point_to_string(keys.get_public_key(private_key, CURVE))
    if send_back_address is None:
        send_back_address = sender_address

    inputs.extend(
        await database.get_spendable_outputs(sender_address, check_pending_txs=True)
    )

    if not inputs:
        raise Exception("No spendable outputs")

    if sum(input.amount for input in inputs) < amount:
        raise Exception(f"Error: You don't have enough funds")

    transaction_inputs = []

    for tx_input in sorted(inputs, key=lambda item: item.amount):
        if tx_input.amount >= amount:
            transaction_inputs.append(tx_input)
            break
    for tx_input in sorted(inputs, key=lambda item: item.amount, reverse=True):
        if sum(input.amount for input in transaction_inputs) >= amount:
            break
        transaction_inputs.append(tx_input)

    transaction_amount = sum(input.amount for input in transaction_inputs)

    transaction = Transaction(
        transaction_inputs,
        [TransactionOutput(receiving_address, amount=amount)],
        message,
    )
    if transaction_amount > amount:
        transaction.outputs.append(
            TransactionOutput(send_back_address, transaction_amount - amount)
        )

    transaction.sign([private_key])

    return transaction


async def create_transaction_to_send_multiple_wallet(
    private_key,
    receiving_addresses,
    amounts,
    message: bytes = None,
    send_back_address=None,
):
    if len(receiving_addresses) != len(amounts):
        raise Exception("Receiving addresses length is different from amounts length")
    database: Database = await Database.get()
    amounts = [Decimal(amount) for amount in amounts]
    total_amount = sum(amounts)
    total_amount = Decimal(total_amount)
    inputs = []
    sender_address = point_to_string(keys.get_public_key(private_key, CURVE))
    if send_back_address is None:
        send_back_address = sender_address

    inputs.extend(
        await database.get_spendable_outputs(sender_address, check_pending_txs=True)
    )

    if not inputs:
        raise Exception("No spendable outputs")

    total_input_amount = sum(input.amount for input in inputs)

    if total_input_amount < total_amount:
        raise Exception(f"Error: You don't have enough funds")

    transaction_inputs = []
    transaction_outputs = []

    # Select inputs to cover the total amount
    input_amount = Decimal(0)
    for tx_input in sorted(inputs, key=lambda item: item.amount, reverse=True):
        transaction_inputs.append(tx_input)
        input_amount += tx_input.amount
        if input_amount >= total_amount:
            break

    # Create outputs for each receiving address
    for receiving_address, amount in zip(receiving_addresses, amounts):
        transaction_outputs.append(
            TransactionOutput(receiving_address, amount=Decimal(amount))
        )

    # If there's change, add an output back to the sender
    change_amount = input_amount - total_amount
    if change_amount > 0:
        transaction_outputs.append(
            TransactionOutput(send_back_address, amount=change_amount)
        )

    transaction = Transaction(transaction_inputs, transaction_outputs, message)
    transaction.sign([private_key])

    return transaction


async def create_stake_transaction(private_key, amount, send_back_address=None):
    database: Database = await Database.get()
    amount = Decimal(amount)
    inputs = []

    sender_address = point_to_string(keys.get_public_key(private_key, CURVE))
    if send_back_address is None:
        send_back_address = sender_address

    inputs.extend(
        await database.get_spendable_outputs(sender_address, check_pending_txs=True)
    )
    if not inputs:
        raise Exception("No spendable outputs")

    if sum(input.amount for input in inputs) < amount:
        raise Exception(f"Error: You don't have enough funds")

    stake_inputs = await database.get_stake_outputs(
        sender_address
    )
    if stake_inputs:
        raise Exception("Already staked")

    pending_stake_tx = await database.get_pending_stake_transaction(sender_address)
    if pending_stake_tx:
        raise Exception("Already staked. Transaction is in pending")

    transaction_inputs = []

    for tx_input in sorted(inputs, key=lambda item: item.amount):
        if tx_input.amount >= amount:
            transaction_inputs.append(tx_input)
            break
    for tx_input in sorted(inputs, key=lambda item: item.amount, reverse=True):
        if sum(input.amount for input in transaction_inputs) >= amount:
            break
        transaction_inputs.append(tx_input)

    transaction_amount = sum(input.amount for input in transaction_inputs)

    transaction = Transaction(
        transaction_inputs,
        [
            TransactionOutput(
                sender_address, amount=amount, transaction_type=OutputType.STAKE
            )
        ],
    )

    if transaction_amount > amount:
        transaction.outputs.append(
            TransactionOutput(send_back_address, transaction_amount - amount)
        )

    if not await database.get_delegates_all_power(
        sender_address, check_pending_txs=True
    ):
        voting_power = Decimal(10)
        transaction.outputs.append(
            TransactionOutput(
                sender_address,
                voting_power,
                transaction_type=OutputType.DELEGATE_VOTING_POWER,
            )
        )

    transaction.sign([private_key])

    return transaction


async def create_unstake_transaction(private_key):
    database: Database = await Database.get()
    sender_address = point_to_string(keys.get_public_key(private_key, CURVE))
    stake_inputs = await database.get_stake_outputs(
        sender_address, check_pending_txs=True
    )
    if not stake_inputs:
        raise Exception(f"Error: There is nothing staked")
    # transaction_inputs = [stake_inputs[0]]
    amount = stake_inputs[0].amount

    if await database.get_delegates_spent_votes(sender_address, check_pending_txs=True):
        raise Exception("Kindly release the votes.")

    pending_vote_tx = await Database.instance.get_pending_vote_as_delegate_transaction(address=sender_address)
    if pending_vote_tx:
        raise Exception('Kindly release the votes. Vote transaction is in pending')

    transaction = Transaction(
        [stake_inputs[0]],
        [
            TransactionOutput(
                sender_address, amount=amount, transaction_type=OutputType.UN_STAKE
            )
        ],
    )
    transaction.sign([private_key])
    return transaction


async def create_inode_registration_transaction(private_key):
    database: Database = await Database.get()
    amount = Decimal(1000)
    inputs = []
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    inputs.extend(await database.get_spendable_outputs(address, check_pending_txs=True))

    if not inputs:
        raise Exception("No spendable outputs")

    if sum(input.amount for input in inputs) < amount:
        raise Exception(f"Error: You don't have enough funds")

    stake_inputs = await database.get_stake_outputs(address, check_pending_txs=True)
    if not stake_inputs:
        raise Exception(f"You are not a delegate. Become a delegate by staking.")

    is_inode_registered = await database.is_inode_registered(
        address, check_pending_txs=True
    )
    if is_inode_registered:
        raise Exception(f"This address is already registered as inode.")

    is_validator_registered = await database.is_validator_registered(
        address, check_pending_txs=True
    )
    if is_validator_registered:
        raise Exception(
            f"This address is registered as validator and a validator cannot be an inode."
        )

    inode_addresses = await database.get_active_inodes(check_pending_txs=True)
    if len(inode_addresses) >= MAX_INODES:
        raise Exception(f"{MAX_INODES} inodes are already registered.")

    transaction_inputs = []

    for tx_input in sorted(inputs, key=lambda item: item.amount):
        if tx_input.amount >= amount:
            transaction_inputs.append(tx_input)
            break
    for tx_input in sorted(inputs, key=lambda item: item.amount, reverse=True):
        if sum(input.amount for input in transaction_inputs) >= amount:
            break
        transaction_inputs.append(tx_input)

    transaction_amount = sum(input.amount for input in transaction_inputs)

    transaction = Transaction(
        transaction_inputs,
        [
            TransactionOutput(
                address, amount=amount, transaction_type=OutputType.INODE_REGISTRATION
            )
        ],
    )
    if transaction_amount > amount:
        transaction.outputs.append(
            TransactionOutput(address, transaction_amount - amount)
        )

    transaction.sign([private_key])
    return transaction


async def create_inode_de_registration_transaction(private_key):
    database: Database = await Database.get()
    inputs = []
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    inputs.extend(
        await database.get_inode_registration_outputs(address, check_pending_txs=True)
    )
    if not inputs:
        raise Exception("This address is not registered as an inode.")
    active_inode_addresses = await database.get_active_inodes(check_pending_txs=True)
    is_inode_active = any(
        entry.get("wallet") == address for entry in active_inode_addresses
    )
    if is_inode_active:
        raise Exception("This address is an active inode. Cannot de-register.")
    # if await database.get_votes_output(address):
    #     raise Exception('Cannot deregister as an inode if you have votes')
    amount = inputs[0].amount
    message = string_to_bytes(str(TransactionType.INODE_DE_REGISTRATION.value))
    transaction = Transaction(
        inputs, [TransactionOutput(address, amount=amount)], message
    )
    transaction.sign([private_key])
    return transaction


async def create_validator_registration_transaction(private_key):
    database: Database = await Database.get()
    amount = Decimal(100)
    inputs = []
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    inputs.extend(await database.get_spendable_outputs(address, check_pending_txs=True))

    if not inputs:
        raise Exception("No spendable outputs")

    if sum(input.amount for input in inputs) < amount:
        raise Exception(f"Error: You don't have enough funds")

    stake_inputs = await database.get_stake_outputs(address, check_pending_txs=True)
    if not stake_inputs:
        raise Exception(f"You are not a delegate. Become a delegate by staking.")

    is_validator_registered = await database.is_validator_registered(
        address, check_pending_txs=True
    )
    if is_validator_registered:
        raise Exception(f"This address is already registered as validator.")

    is_inode_registered = await database.is_inode_registered(
        address, check_pending_txs=True
    )
    if is_inode_registered:
        raise Exception(
            f"This address is registered as inode and an inode cannot be a validator."
        )

    transaction_inputs = select_transaction_input(inputs, amount)

    transaction_amount = sum(input.amount for input in transaction_inputs)

    message = string_to_bytes(str(TransactionType.VALIDATOR_REGISTRATION.value))
    transaction = Transaction(
        transaction_inputs,
        [
            TransactionOutput(
                address,
                amount=amount,
                transaction_type=OutputType.VALIDATOR_REGISTRATION,
            )
        ],
        message,
    )

    voting_power = Decimal(10)
    transaction.outputs.append(
        TransactionOutput(
            address, voting_power, transaction_type=OutputType.VALIDATOR_VOTING_POWER
        )
    )

    if transaction_amount > amount:
        transaction.outputs.append(
            TransactionOutput(address, transaction_amount - amount)
        )

    transaction.sign([private_key])
    return transaction


async def create_voting_transaction(private_key, vote_range, vote_receiving_address):
    try:
        vote_range = int(vote_range)
    except:
        raise Exception("Invalid voting range")
    if vote_range > 10:
        raise Exception("Voting should be in range of 10")
    if vote_range <= 0:
        raise Exception("Invalid voting range")

    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    is_inode_registered = await database.is_inode_registered(
        address, check_pending_txs=True
    )
    if is_inode_registered:
        raise Exception(f"This address is registered as inode. Cannot vote.")

    is_validator_registered = await database.is_validator_registered(
        address, check_pending_txs=True
    )
    if is_validator_registered:
        return await vote_as_validator(private_key, vote_range, vote_receiving_address)
    elif await database.get_stake_outputs(address, check_pending_txs=True):
        return await vote_as_delegate(private_key, vote_range, vote_receiving_address)
    else:
        raise Exception("Not eligible to vote")


async def vote_as_validator(private_key, vote_range, vote_receiving_address):
    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    vote_range = Decimal(vote_range)
    inputs = []
    inputs.extend(
        await database.get_validators_voting_power(address, check_pending_txs=True)
    )
    if not inputs:
        raise Exception("No voting outputs")

    if sum(input.amount for input in inputs) < vote_range:
        raise Exception(
            f"Error: You don't have enough voting power left. Kindly revoke some voting power."
        )

    if not await database.is_inode_registered(
        vote_receiving_address, check_pending_txs=True
    ):
        raise Exception("Vote recipient is not registered as an inode.")

    transaction_inputs = select_transaction_input(inputs, vote_range)

    transaction_vote_range = sum(input.amount for input in transaction_inputs)

    message = string_to_bytes(str(TransactionType.VOTE_AS_VALIDATOR.value))
    transaction = Transaction(
        transaction_inputs,
        [
            TransactionOutput(
                vote_receiving_address,
                amount=vote_range,
                transaction_type=OutputType.VOTE_AS_VALIDATOR,
            )
        ],
        message,
    )
    if transaction_vote_range > vote_range:
        transaction.outputs.append(
            TransactionOutput(
                address,
                transaction_vote_range - vote_range,
                transaction_type=OutputType.VALIDATOR_VOTING_POWER,
            )
        )

    transaction.sign([private_key])
    return transaction


async def vote_as_delegate(private_key, vote_range, vote_receiving_address):
    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))

    vote_range = Decimal(vote_range)
    inputs = []
    inputs.extend(
        await database.get_delegates_voting_power(address, check_pending_txs=True)
    )
    if not inputs:
        raise Exception("No voting outputs")

    if sum(input.amount for input in inputs) < vote_range:
        raise Exception(
            f"Error: You don't have enough voting power left. Kindly release some voting power."
        )

    if not await database.is_validator_registered(
        vote_receiving_address, check_pending_txs=True
    ):
        raise Exception("Vote recipient is not registered as a validator.")

    transaction_inputs = select_transaction_input(inputs, vote_range)

    transaction_vote_range = sum(input.amount for input in transaction_inputs)

    message = string_to_bytes(str(TransactionType.VOTE_AS_DELEGATE.value))
    transaction = Transaction(
        transaction_inputs,
        [
            TransactionOutput(
                vote_receiving_address,
                amount=vote_range,
                transaction_type=OutputType.VOTE_AS_DELEGATE,
            )
        ],
        message,
    )
    if transaction_vote_range > vote_range:
        transaction.outputs.append(
            TransactionOutput(
                address,
                transaction_vote_range - vote_range,
                transaction_type=OutputType.DELEGATE_VOTING_POWER,
            )
        )

    transaction.sign([private_key])
    return transaction


async def create_revoke_transaction(private_key, revoke_from_address):
    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    is_validator_registered = await database.is_validator_registered(
        address, check_pending_txs=True
    )
    if is_validator_registered:
        return await revoke_vote_as_validator(private_key, revoke_from_address)
    elif await database.get_stake_outputs(address, check_pending_txs=True):
        return await revoke_vote_as_delegate(private_key, revoke_from_address)
    else:
        raise Exception("Not eligible to revoke")


async def revoke_vote_as_validator(private_key, inode_address):
    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    inode_ballot_inputs = await database.get_inode_ballot_input_by_address(
        address, inode_address, check_pending_txs=True
    )
    if not inode_ballot_inputs:
        raise Exception("You have not voted.")

    is_revoke_valid = [
        await database.is_revoke_valid(inode_ballot_input.tx_hash)
        for inode_ballot_input in inode_ballot_inputs
    ]
    if not any(is_revoke_valid):
        raise Exception("You can revoke after 48 hrs of voting")

    message = string_to_bytes(str(TransactionType.REVOKE_AS_VALIDATOR.value))
    sum_of_votes = sum(
        inode_ballot_input.amount for inode_ballot_input in inode_ballot_inputs
    )
    transaction = Transaction(
        inode_ballot_inputs,
        [
            TransactionOutput(
                address,
                amount=sum_of_votes,
                transaction_type=OutputType.VALIDATOR_VOTING_POWER,
            )
        ],
        message,
    )
    transaction.sign([private_key])
    return transaction


async def revoke_vote_as_delegate(private_key, validator_address):
    database: Database = await Database.get()
    address = point_to_string(keys.get_public_key(private_key, CURVE))
    validator_ballot_inputs = await database.get_validator_ballot_input_by_address(
        address, validator_address, check_pending_txs=True
    )
    if not validator_ballot_inputs:
        raise Exception("You have not voted.")
    is_revoke_valid = [
        await database.is_revoke_valid(validator_ballot_input.tx_hash)
        for validator_ballot_input in validator_ballot_inputs
    ]
    if not any(is_revoke_valid):
        raise Exception("You can revoke after 48 hrs of voting")

    message = string_to_bytes(str(TransactionType.REVOKE_AS_DELEGATE.value))
    sum_of_votes = sum(
        validator_ballot_input.amount
        for validator_ballot_input in validator_ballot_inputs
    )
    transaction = Transaction(
        validator_ballot_inputs,
        [
            TransactionOutput(
                address,
                amount=sum_of_votes,
                transaction_type=OutputType.DELEGATE_VOTING_POWER,
            )
        ],
        message,
    )
    transaction.sign([private_key])
    return transaction


def select_transaction_input(inputs, amount):
    transaction_inputs = []
    for tx_input in sorted(inputs, key=lambda item: item.amount):
        if tx_input.amount >= amount:
            transaction_inputs.append(tx_input)
            break
    for tx_input in sorted(inputs, key=lambda item: item.amount, reverse=True):
        if sum(input.amount for input in transaction_inputs) >= amount:
            break
        transaction_inputs.append(tx_input)
    return transaction_inputs


def string_to_bytes(string: str) -> bytes:
    if string is None:
        return None
    try:
        return bytes.fromhex(string)
    except ValueError:
        return string.encode("utf-8")
