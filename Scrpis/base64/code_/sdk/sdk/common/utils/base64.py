import binascii
import re
from typing import Any, Optional, Union

bytes_types = (bytes, bytearray)
_urlsafe_encode_translation = bytes.maketrans(b"+/", b"-_")
_urlsafe_decode_translation = bytes.maketrans(b"-_", b"+/")


def _bytes_from_decode_data(s: Any) -> bytes:
    if isinstance(s, str):
        try:
            return s.encode("ascii")
        except UnicodeEncodeError:
            raise ValueError("string argument should contain only ASCII characters")
    if isinstance(s, bytes_types):
        return s
    try:
        return memoryview(s).tobytes()
    except TypeError:
        raise TypeError("argument should be a bytes-like object or ASCII string, not %r" % s.__class__.__name__) from None


def b64decode(s: Union[str, bytes], altchars: Optional[bytes] = None, validate: bool = False) -> bytes:
    s_bytes = _bytes_from_decode_data(s)
    s_bytes = s_bytes + b"=" * (len(s_bytes) % 4)

    if altchars is not None:
        _altchars = _bytes_from_decode_data(altchars)
        assert len(_altchars) == 2, repr(_altchars)
        s_bytes = s_bytes.translate(bytes.maketrans(_altchars, b"+/"))

    if validate and not re.fullmatch(b"[A-Za-z0-9+/]*={0,2}", s_bytes):
        raise binascii.Error("Non-base64 digit found")

    return binascii.a2b_base64(s_bytes)


def b64encode(s: bytes, altchars: Optional[bytes] = None) -> bytes:
    encoded = binascii.b2a_base64(s, newline=False)
    if altchars is not None:
        assert len(altchars) == 2, repr(altchars)
        encoded = encoded.translate(bytes.maketrans(b"+/", altchars))

    return encoded


def urlsafe_b64decode(s: Union[str, bytes], altchars: Optional[bytes] = None, validate: bool = False) -> bytes:
    s_bytes = _bytes_from_decode_data(s)
    s_bytes = s_bytes.translate(_urlsafe_decode_translation)
    return b64decode(s_bytes)


def urlsafe_b64encode(s: bytes) -> bytes:
    return b64encode(s).translate(_urlsafe_encode_translation)
