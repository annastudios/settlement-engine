import re

# Country code, 2 check digits, then 11-30 letters/digits (15-34 chars total).
_IBAN_SHAPE = re.compile(r"^[A-Z]{2}[0-9]{2}[A-Z0-9]{11,30}$")


def normalize_iban(raw: str) -> str:
    """Return the IBAN upper-cased without spaces, or raise ValueError.

    Checks the shape and the ISO 13616 mod-97 checksum, which catches most
    typos, such as one wrong digit or two swapped digits.
    """
    iban = raw.replace(" ", "").upper()
    if not _IBAN_SHAPE.match(iban):
        raise ValueError("not a valid IBAN format")
    # Move the first 4 chars to the end, turn letters into numbers (A=10 ...
    # Z=35), and the whole thing must leave remainder 1 when divided by 97.
    rearranged = iban[4:] + iban[:4]
    as_digits = "".join(str(int(char, 36)) for char in rearranged)
    if int(as_digits) % 97 != 1:
        raise ValueError("IBAN checksum is wrong")
    return iban
