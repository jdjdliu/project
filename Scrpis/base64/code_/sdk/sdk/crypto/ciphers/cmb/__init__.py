from ..base import Cipher
from .cmb_sm2.CMBSMFunction import CMBSM2SignWithSM3ASN1, CMBSM2VerifyWithSM3ASN1
from .cmb_sm2.SMCryptException import SMCryptException

__all__ = [
    "CMBSM2WithSM3ASN1",
]


class CMBSM2WithSM3ASN1(Cipher):
    NAME = "SM3WithSM2"

    def encrypt(self: "Cipher", message: str) -> bytes:
        pass

    def decrypt(self: "Cipher", message: str) -> bytes:
        pass

    def sign(self: "Cipher", message: str) -> bytes:
        signature: bytes = CMBSM2SignWithSM3ASN1(
            privkey=bytes.fromhex(self.private_key),
            msg=message.encode(),
        )
        return signature

    def verify(self: "Cipher", signature: bytes, message: str) -> bool:
        try:
            CMBSM2VerifyWithSM3ASN1(pubkey=bytes.fromhex(self.public_key), msg=message.encode(), signature=signature)  # type: ignore
            return True
        except SMCryptException:
            return False
