from abc import ABC, abstractmethod


class Cipher(ABC):
    NAME: str = ""

    def __init__(
        self: "Cipher",
        private_key: str = "",
        public_key: str = "",
        secret: str = "",
        iv: str = "",
    ) -> None:
        self.private_key = private_key
        self.public_key = public_key
        self.secret = secret
        self.iv = iv

    @abstractmethod
    def encrypt(self: "Cipher", message: str) -> bytes:
        pass

    @abstractmethod
    def decrypt(self: "Cipher", message: str) -> bytes:
        pass

    @abstractmethod
    def sign(self: "Cipher", message: str) -> bytes:
        pass

    @abstractmethod
    def verify(self: "Cipher", signature: bytes, message: str) -> bool:
        pass
