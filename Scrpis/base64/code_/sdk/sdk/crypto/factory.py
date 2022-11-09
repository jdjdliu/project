from typing import Type

from .ciphers import CIPHERS
from .ciphers.base import Cipher


def get_cipher(cipher_name: str) -> Type[Cipher]:
    return CIPHERS[cipher_name]
