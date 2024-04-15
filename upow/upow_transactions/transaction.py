import logging
from decimal import Decimal
from io import BytesIO
from typing import List

from fastecdsa import keys
from icecream import ic

import upow.helpers
from . import TransactionInput, TransactionOutput
from .coinbase_transaction import CoinbaseTransaction
from ..constants import ENDIAN, SMALLEST, CURVE, MAX_INODES
from ..helpers import point_to_string, bytes_to_string, sha256, TransactionType, get_transaction_type_from_message, \
    OutputType, InputType

print = ic


class Transaction:
    def __init__(self, inputs: List[TransactionInput], outputs: List[TransactionOutput], message: bytes = None,
                 version: int = None):
        if len(inputs) >= 256:
            raise Exception(f'You can spend max 255 inputs in a single transactions, not {len(inputs)}')
        if len(outputs) >= 256:
            raise Exception(f'You can have max 255 outputs in a single transactions, not {len(outputs)}')
        self.inputs = inputs
        self.outputs = outputs
        self.message = message
        self.transaction_type = get_transaction_type_from_message(message)
        if version is None:
            if all(len(tx_output.address_bytes) == 64 for tx_output in outputs):
                version = 1
            elif all(len(tx_output.address_bytes) == 33 for tx_output in outputs):
                version = 3
            else:
                raise NotImplementedError()
        if version > 3:
            raise NotImplementedError()
        self.version = version
        self._hex: str = None
        self.fees: Decimal = None
        self.tx_hash: str = None

    def hex(self, full: bool = True):
        inputs, outputs = self.inputs, self.outputs
        hex_inputs = ''.join(tx_input.tobytes().hex() for tx_input in inputs)
        hex_outputs = ''.join(tx_output.tobytes().hex() for tx_output in outputs)

        version = self.version

        self._hex = ''.join([
            version.to_bytes(1, ENDIAN).hex(),
            len(inputs).to_bytes(1, ENDIAN).hex(),
            hex_inputs,
            (len(outputs)).to_bytes(1, ENDIAN).hex(),
            hex_outputs
        ])

        if not full and (version <= 2 or self.message is None):
            return self._hex

        if self.message is not None:
            if version <= 2:
                self._hex += bytes([1, len(self.message)]).hex()
            else:
                self._hex += bytes([1]).hex()
                self._hex += (len(self.message)).to_bytes(2, ENDIAN).hex()
            self._hex += self.message.hex()
            if not full:
                return self._hex
        else:
            self._hex += (0).to_bytes(1, ENDIAN).hex()

        signatures = []
        for tx_input in inputs:
            signed = tx_input.get_signature()
            if signed not in signatures:
                signatures.append(signed)
                self._hex += signed

        return self._hex

    def hash(self):
        if self.tx_hash is None:
            self.tx_hash = sha256(self.hex())
        return self.tx_hash

    def _verify_double_spend_same_transaction(self):
        used_inputs = []
        for tx_input in self.inputs:
            input_hash = f"{tx_input.tx_hash}{tx_input.index}"
            if input_hash in used_inputs:
                return False
            used_inputs.append(input_hash)
        return True

    async def verify_double_spend(self):
        from upow.database import Database
        check_inputs = [(tx_input.tx_hash, tx_input.index) for tx_input in self.inputs]
        if self.transaction_type == TransactionType.INODE_DE_REGISTRATION:
            inode_outputs = await Database.instance.get_inode_outputs(check_inputs)
            return set(check_inputs) == set(inode_outputs)

        elif self.transaction_type == TransactionType.VOTE_AS_VALIDATOR:
            voting_power = await Database.instance.get_validator_voting_power_outputs(check_inputs)
            return set(check_inputs) == set(voting_power)

        elif self.transaction_type == TransactionType.VOTE_AS_DELEGATE:
            voting_power = await Database.instance.get_delegates_voting_power_outputs(check_inputs)
            return set(check_inputs) == set(voting_power)

        elif self.transaction_type == TransactionType.REVOKE_AS_VALIDATOR:
            get_inodes_ballot = await Database.instance.get_inodes_ballot_outputs(check_inputs)
            return set(check_inputs) == set(get_inodes_ballot)

        elif self.transaction_type == TransactionType.REVOKE_AS_DELEGATE:
            get_validator_ballot = await Database.instance.get_validators_ballot_outputs(check_inputs)
            return set(check_inputs) == set(get_validator_ballot)

        else:
            unspent_outputs = await Database.instance.get_unspent_outputs(check_inputs)
            return set(check_inputs) == set(unspent_outputs)

    async def verify_double_spend_pending(self):
        from upow.database import Database
        check_inputs = [(tx_input.tx_hash, tx_input.index) for tx_input in self.inputs]
        spent_outputs = await Database.instance.get_pending_spent_outputs(check_inputs)
        is_true = spent_outputs == []
        if not is_true:
            logging.error(f'Double spending in pending {spent_outputs}')
        return is_true

    async def _fill_transaction_inputs(self, txs=None) -> None:
        from upow.database import Database
        check_inputs = [tx_input.tx_hash for tx_input in self.inputs if
                        tx_input.transaction is None and tx_input.transaction_info is None]
        if not check_inputs:
            return
        if txs is None:
            txs = await Database.instance.get_transactions_info(check_inputs)
        for tx_input in self.inputs:
            tx_hash = tx_input.tx_hash
            if tx_hash in txs:
                tx_input.transaction_info = txs[tx_hash]

    async def _check_signature(self):
        tx_hex = self.hex(False)
        checked_signatures = []
        for tx_input in self.inputs:
            if tx_input.signed is None:
                print('not signed')
                return False
            await tx_input.get_public_key()
            signature = (tx_input.public_key, tx_input.signed)
            if signature in checked_signatures:
                continue
            if not await tx_input.verify(tx_hex):
                print('signature not valid')
                return False
            checked_signatures.append(signature)
        return True

    async def _check_voter_revoke_signature(self):
        tx_hex = self.hex(False)
        checked_signatures = []
        for tx_input in self.inputs:
            if tx_input.signed is None:
                print('not signed')
                return False
            await tx_input.get_voter_public_key()
            signature = (tx_input.public_key, tx_input.signed)
            if signature in checked_signatures:
                continue
            if not await tx_input.verify_revoke_tx(tx_hex):
                print('voter signature not valid')
                return False
            checked_signatures.append(signature)
        return True

    def _verify_outputs(self):
        return self.outputs and all(tx_output.verify() for tx_output in self.outputs)

    async def verify(self, check_double_spend: bool = True) -> bool:
        if check_double_spend and not self._verify_double_spend_same_transaction():
            print('double spend inside same transaction')
            return False

        if check_double_spend and not await self.verify_double_spend():
            print('double spend')
            return False

        await self._fill_transaction_inputs()

        if not await self.verify_stake_transaction():
            return False

        if not await self.verify_un_stake_transaction():
            return False

        if not await self.verify_validator_transaction():
            return False

        if not await self.verify_revoke_as_validator():
            return False

        if not await self.verify_revoke_as_delegate():
            return False

        if not await self.verify_inode_de_register_transaction():
            return False

        if not await self.verify_inode_register_transaction():
            return False

        if not await self.verify_vote_as_validator_transaction():
            return False

        if not await self.verify_vote_as_delegate_transaction():
            return False

        if self.transaction_type in (TransactionType.REVOKE_AS_VALIDATOR, TransactionType.REVOKE_AS_DELEGATE):
            if not await self._check_voter_revoke_signature():
                return False
        else:
            if not await self._check_signature():
                return False

        if not self._verify_outputs():
            print('invalid outputs')
            return False

        if await self.get_fees() < 0:
            print('We are not the Federal Reserve')
            return False

        return True

    async def verify_inode_de_register_transaction(self):
        if self.transaction_type == TransactionType.INODE_DE_REGISTRATION:
            from upow.database import Database
            address = await self.inputs[0].get_address()
            inputs = await Database.instance.get_inode_registration_outputs(address)
            if not inputs:
                print('This address is not registered as an inode.')
                return False

            active_inode_addresses = await Database.instance.get_active_inodes()
            is_inode_active = any(entry.get("wallet") == address for entry in active_inode_addresses)
            if is_inode_active:
                print('This address is an active inode. Cannot de-register.')
                return False
        return True

    async def verify_vote_as_validator_transaction(self):
        if self.transaction_type == TransactionType.VOTE_AS_VALIDATOR:
            vote_range = sum(tx_output.amount for tx_output in self.outputs
                             if tx_output.transaction_type == OutputType.VOTE_AS_VALIDATOR)
            if vote_range > 10:
                print('Voting should be in range of 10')
                return False

            if vote_range <= 0:
                print('Invalid voting range')
                return False

            from upow.database import Database
            address = await self.inputs[0].get_address()
            is_inode_registered = await Database.instance.is_inode_registered(address, check_pending_txs=True)
            if is_inode_registered:
                print(f"This address is registered as inode. Cannot vote.")
                return False

            is_validator_registered = await Database.instance.is_validator_registered(address, check_pending_txs=True)
            if not is_validator_registered:
                print(f"This address is not registered as validator. Cannot vote.")
                return False

            vote_receiving_address = ''
            for tx_output in self.outputs:
                if tx_output.transaction_type is OutputType.VOTE_AS_VALIDATOR:
                    vote_receiving_address = tx_output.address

            if not await Database.instance.is_inode_registered(vote_receiving_address, check_pending_txs=True):
                print('Vote recipient is not registered as an inode.')
                return False
        return True

    async def verify_vote_as_delegate_transaction(self):
        if self.transaction_type == TransactionType.VOTE_AS_DELEGATE:
            vote_range = sum(tx_output.amount for tx_output in self.outputs
                             if tx_output.transaction_type == OutputType.VOTE_AS_DELEGATE)
            if vote_range > 10:
                print('Voting should be in range of 10')
                return False

            if vote_range <= 0:
                print('Invalid voting range')
                return False

            from upow.database import Database
            address = await self.inputs[0].get_address()
            is_inode_registered = await Database.instance.is_inode_registered(address, check_pending_txs=True)
            if is_inode_registered:
                print(f"This address is registered as inode. Cannot vote.")
                return False

            is_delegate = await Database.instance.get_stake_outputs(address)
            if not is_delegate:
                print(f"This address is not staked anything. Cannot vote.")
                return False

            vote_receiving_address = ''
            for tx_output in self.outputs:
                if tx_output.transaction_type is OutputType.VOTE_AS_DELEGATE:
                    vote_receiving_address = tx_output.address

            if not await Database.instance.is_validator_registered(vote_receiving_address, check_pending_txs=True):
                print('Vote recipient is not registered as a validator.')
                return False
        return True

    async def verify_inode_register_transaction(self):
        if any(tx_output.transaction_type == OutputType.INODE_REGISTRATION for tx_output in self.outputs):
            from upow.database import Database
            address = await self.inputs[0].get_address()

            inode_registration_amount = sum(tx_output.amount for tx_output in self.outputs
                                            if tx_output.transaction_type == OutputType.INODE_REGISTRATION)

            if inode_registration_amount != 1000:
                print('Inode registration amount is in correct')
                return False

            stake_inputs = await Database.instance.get_stake_outputs(address)
            if not stake_inputs:
                print(f"You are not a delegate. Become a delegate by staking.")
                return False

            is_inode_registered = await Database.instance.is_inode_registered(address, check_pending_txs=True)
            if is_inode_registered:
                print(f"This address is already registered as inode.")
                return False

            is_validator_registered = await Database.instance.is_validator_registered(address, check_pending_txs=True)
            if is_validator_registered:
                print(f"This address is registered as validator and a validator cannot be an inode.")
                return False

            inode_addresses = await Database.instance.get_active_inodes(check_pending_txs=True)
            if len(inode_addresses) >= MAX_INODES:
                print(f"{MAX_INODES} inodes are already registered.")
                return False

            active_inode_addresses = await Database.instance.get_active_inodes()
            is_inode_active = any(entry.get("wallet") == address for entry in active_inode_addresses)
            if is_inode_active:
                print('This address is an active inode. Cannot de-register.')
                return False
        return True

    async def verify_validator_transaction(self):
        if self.transaction_type == TransactionType.VALIDATOR_REGISTRATION:
            from upow.database import Database
            address = await self.inputs[0].get_address()
            is_delegate = await Database.instance.get_stake_outputs(address)
            if not is_delegate:
                print("You are not a delegate. Become a delegate by staking.")
                return False

            if await Database.instance.is_validator_registered(address, check_pending_txs=True):
                print('validator already registered')
                return False

            if await Database.instance.is_inode_registered(address, check_pending_txs=True):
                print('Already registered as an inode')
                return False

            validator_reg_amount = sum(tx_output.amount for tx_output in self.outputs if
                                      tx_output.transaction_type == OutputType.VALIDATOR_REGISTRATION)
            if validator_reg_amount != 100:
                print('validator reg amount is not correct')
                return False

            validator_voting_power = [tx_output for tx_output in self.outputs if
                                      tx_output.transaction_type == OutputType.VALIDATOR_VOTING_POWER]
            if len(validator_voting_power) != 1:
                print('Validator voting power input bug')
                return False

            if validator_voting_power[0].amount != 10:
                print('Validator voting power bug')
                return False
        return True

    async def verify_revoke_as_validator(self):
        if self.transaction_type == TransactionType.REVOKE_AS_VALIDATOR:
            from upow.database import Database
            address = await self.inputs[0].get_voter_address()
            is_validator_registered = await Database.instance.is_validator_registered(address, check_pending_txs=True)
            if not is_validator_registered:
                print("This address is not registered as validator.")
                return False
            is_delegate = await Database.instance.get_stake_outputs(address)
            if not is_delegate:
                print('This address is not registered as delegate. Cannot revoke')
                return False

            is_revoke_valid = [await Database.instance.is_revoke_valid(inode_ballot_input.tx_hash)
                               for inode_ballot_input in self.inputs]
            if not any(is_revoke_valid):
                print('You can revoke after 48 hrs of voting')
                return False
        return True

    async def verify_revoke_as_delegate(self):
        if self.transaction_type == TransactionType.REVOKE_AS_DELEGATE:
            from upow.database import Database
            address = await self.inputs[0].get_voter_address()
            is_delegate = await Database.instance.get_stake_outputs(address)
            if not is_delegate:
                print('This address is not registered as delegate. Cannot revoke')
                return False

            is_revoke_valid = [await Database.instance.is_revoke_valid(validator_ballot_input.tx_hash)
                               for validator_ballot_input in self.inputs]
            if not any(is_revoke_valid):
                print('You can revoke after 48 hrs of voting')
                return False
        return True

    async def verify_stake_transaction(self):
        if any(tx_output.transaction_type == OutputType.STAKE for tx_output in self.outputs):
            from upow.database import Database
            address = await self.inputs[0].get_address()
            stake_inputs = await Database.instance.get_stake_outputs(address)
            if stake_inputs and not upow.helpers.is_blockchain_syncing:
                logging.error('Already staked')
                return False

            pending_stake_tx = await Database.instance.get_pending_stake_transaction(address)
            pending_stake_tx = [tx for tx in pending_stake_tx if tx.tx_hash != self.tx_hash]
            if pending_stake_tx:
                logging.error('Already staked. Transaction is in pending')
                return False

            tx_delegate_power = sum(tx_output.amount
                                    for tx_output in self.outputs
                                    if tx_output.transaction_type == OutputType.DELEGATE_VOTING_POWER)
            if tx_delegate_power > 0:
                if tx_delegate_power != 10:
                    logging.error('Delegate voting power bug')
                    return False

                if await Database.instance.get_delegates_all_power(address, check_pending_txs=True):
                    logging.error('Delegate already have voting power')
                    return False
            else:
                if not await Database.instance.get_delegates_all_power(address, check_pending_txs=True):
                    logging.error('Delegate doesnt have voting power')
                    return False

        return True

    async def verify_un_stake_transaction(self):
        if any(tx_output.transaction_type == OutputType.UN_STAKE for tx_output in self.outputs):
            from upow.database import Database
            address = await self.inputs[0].get_address()
            if await Database.instance.get_delegates_spent_votes(address) \
                    and self.hash() not in ["8befeb253bc6eddd8501f5b27a02b195f5c06a51ccf788213cbedafe7cc49c53"]: # ignoring the revoke_as_delegate and unstake in same block
                print('Kindly release the votes.')
                return False
            pending_vote_tx = await Database.instance.get_pending_vote_as_delegate_transaction(address=address)
            if pending_vote_tx:
                print('Kindly release the votes. Vote transaction is in pending')
                return False
        return True

    async def verify_pending(self):
        return await self.verify() and await self.verify_double_spend_pending()

    def sign(self, private_keys: list = []):
        for private_key in private_keys:
            for input in self.inputs:
                if input.private_key is None and (input.public_key or input.transaction):
                    public_key = keys.get_public_key(private_key, CURVE)
                    input_public_key = input.public_key or input.transaction.outputs[input.index].public_key
                    if public_key == input_public_key:
                        input.private_key = private_key
        for input in self.inputs:
            if input.private_key is not None:
                input.sign(self.hex(False))
        return self

    async def get_fees(self):
        input_amount = 0
        output_amount = 0
        if self.transaction_type == TransactionType.REGULAR:
            for tx_input in self.inputs:
                input_amount += await tx_input.get_amount()
            output_amount = sum(tx_output.amount for tx_output in self.outputs
                                if tx_output.transaction_type
                                not in (OutputType.VALIDATOR_VOTING_POWER, OutputType.DELEGATE_VOTING_POWER))
        #
        # if self.transaction_type in (TransactionType.REVOKE_AS_DELEGATE, TransactionType.REVOKE_AS_VALIDATOR):
        #     output_amount = sum(tx_output.amount for tx_output in self.outputs)
        # else:
        #     output_amount = sum(tx_output.amount for tx_output in self.outputs
        #                         if tx_output.transaction_type
        #                         not in (OutputType.VALIDATOR_VOTING_POWER, OutputType.DELEGATE_VOTING_POWER))

        self.fees = input_amount - output_amount
        assert (self.fees * SMALLEST) % 1 == 0.0
        return self.fees

    @staticmethod
    async def from_hex(hexstring: str, check_signatures: bool = True):
        tx_bytes = BytesIO(bytes.fromhex(hexstring))
        version = int.from_bytes(tx_bytes.read(1), ENDIAN)
        if version > 3:
            raise NotImplementedError()

        inputs_count = int.from_bytes(tx_bytes.read(1), ENDIAN)

        inputs = []

        for i in range(0, inputs_count):
            tx_hex = tx_bytes.read(32).hex()
            tx_index = int.from_bytes(tx_bytes.read(1), ENDIAN)
            input_type = int.from_bytes(tx_bytes.read(1), ENDIAN)
            inputs.append(TransactionInput(tx_hex, index=tx_index, input_type=InputType(input_type)))

        outputs_count = int.from_bytes(tx_bytes.read(1), ENDIAN)

        outputs = []

        for i in range(0, outputs_count):
            pubkey = tx_bytes.read(64 if version == 1 else 33)
            amount_length = int.from_bytes(tx_bytes.read(1), ENDIAN)
            amount = int.from_bytes(tx_bytes.read(amount_length), ENDIAN) / Decimal(SMALLEST)
            transaction_type = int.from_bytes(tx_bytes.read(1), ENDIAN)
            outputs.append(TransactionOutput(bytes_to_string(pubkey), amount, OutputType(transaction_type)))

        specifier = int.from_bytes(tx_bytes.read(1), ENDIAN)
        if specifier == 36:
            # assert len(inputs) == 1 and len(outputs) == 1
            assert len(inputs) == 1
            coinbase_transaction = CoinbaseTransaction(inputs[0].tx_hash, outputs[0].address, outputs[0].amount)
            if len(outputs) > 1:
                coinbase_transaction.outputs.extend(outputs[1:])
            return coinbase_transaction
        else:
            if specifier == 1:
                message_length = int.from_bytes(tx_bytes.read(1 if version <= 2 else 2), ENDIAN)
                message = tx_bytes.read(message_length)
            else:
                message = None
                assert specifier == 0

            signatures = []

            while True:
                signed = (int.from_bytes(tx_bytes.read(32), ENDIAN), int.from_bytes(tx_bytes.read(32), ENDIAN))
                if signed[0] == 0:
                    break
                signatures.append(signed)

            if len(signatures) == 1:
                for tx_input in inputs:
                    tx_input.signed = signatures[0]
            elif len(inputs) == len(signatures):
                for i, tx_input in enumerate(inputs):
                    tx_input.signed = signatures[i]
            else:
                if not check_signatures:
                    return Transaction(inputs, outputs, message, version)
                index = {}
                for tx_input in inputs:
                    public_key = point_to_string(await tx_input.get_public_key())
                    if public_key not in index.keys():
                        index[public_key] = []
                    index[public_key].append(tx_input)

                for i, signed in enumerate(signatures):
                    for tx_input in index[list(index.keys())[i]]:
                        tx_input.signed = signed

            return Transaction(inputs, outputs, message, version)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.hex() == other.hex()
        else:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)
