import pytest

from settlement.iban import normalize_iban


@pytest.mark.parametrize(
    "iban",
    [
        "GB82WEST12345698765432",
        "GB33BUKB20201555555555",
        "DE89370400440532013000",
        "FR1420041010050500013M02606",
        "NL91ABNA0417164300",
    ],
)
def test_valid_ibans(iban):
    assert normalize_iban(iban) == iban


def test_spaces_and_lower_case_are_normalised():
    assert normalize_iban("gb82 west 1234 5698 7654 32") == "GB82WEST12345698765432"


@pytest.mark.parametrize(
    "iban, reason",
    [
        ("GB82WEST12345698765433", "checksum"),  # last digit changed
        ("GB82WEST12345698765423", "checksum"),  # two digits swapped
        ("GB82", "format"),  # too short
        ("1282WEST12345698765432", "format"),  # no country code
        ("GB82WEST1234569876543!", "format"),  # bad character
    ],
)
def test_invalid_ibans(iban, reason):
    with pytest.raises(ValueError, match=reason):
        normalize_iban(iban)
