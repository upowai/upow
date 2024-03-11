from decimal import Decimal

from ..constants import ENDIAN, SMALLEST, CURVE
from ..helpers import byte_length, string_to_point, string_to_bytes, OutputType


class TransactionOutput:
    def __init__(self, address: str, amount: Decimal, transaction_type: OutputType = OutputType.REGULAR):
        from fastecdsa.point import Point
        if isinstance(address, Point):
            raise Exception('TransactionOutput does not accept Point anymore. Pass the address string instead')
        self.address = address
        self.address_bytes = string_to_bytes(address)
        self.public_key = string_to_point(address)
        assert (amount * SMALLEST) % 1 == 0.0, 'too many decimal digits'
        self.amount = amount
        self.transaction_type = transaction_type
        self.is_stake = transaction_type == OutputType.STAKE

    def tobytes(self):
        amount = int(self.amount * SMALLEST)
        count = byte_length(amount)
        return self.address_bytes + count.to_bytes(1, ENDIAN) + amount.to_bytes(count, ENDIAN) + self.transaction_type.to_bytes(1, ENDIAN)

    def verify(self):
        return self.amount > 0 and CURVE.is_point_on_curve((self.public_key.x, self.public_key.y))

    @property
    def as_dict(self):
        res = vars(self).copy()
        if 'public_key' in res: del res['public_key']
        return res
