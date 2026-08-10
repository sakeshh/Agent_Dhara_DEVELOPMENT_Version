from __future__ import annotations
import re

SAMPLING_THRESHOLD = 10_000_000

DEFAULT_SAMPLE_SIZE = 100_000

HEAVY_OPERATION_THRESHOLD = 10_000_000

MAX_REL_ROW_INDEXES = 200

PLACEHOLDERS = {
    "", " ", "-", "--", "---", "n/a", "na", "none", "null", "nil",
    "unknown", "not available", "missing", "undefined", "not applicable",
    "tbd", "tba", "n.a.", "n.a", "#n/a", "#null!", "#value!", "#ref!",
    "#div/0!", "error", "nan", "inf", "-inf", "0000-00-00", "1900-01-01",
    "9999-12-31", "00", "000", "0000", "?", "??", "???", "!",
    "temp", "test", "dummy", "placeholder", "na.", "na,", "not set",
    "unknown unknown", "n.d.", "nd", "not known",
}

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]*\.[^@\s]+$")

PHONE_RE = re.compile(r"^[+()\-\.\s0-9]{7,}$")

HTML_TAG_RE = re.compile(r"<[a-zA-Z][^>]*>|</[a-zA-Z]+>")

PUNCTUATION_ONLY_RE = re.compile(r"^[\W_]+$")

LEADING_ZERO_RE = re.compile(r"^0[0-9]+$")

MULTI_SPACE_RE = re.compile(r"  +")  # two or more consecutive spaces

_UUID_RE = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
)

_URL_RE = re.compile(r'^https?://', re.IGNORECASE)

_IP4_RE = re.compile(r'^\d{1,3}(?:\.\d{1,3}){3}$')

_IP6_RE = re.compile(r'^[0-9a-fA-F:]{7,39}$')

_BOOL_VALS = frozenset({"true","false","yes","no","y","n","1","0","t","f","on","off"})

_PHONE_NAME_HINTS = frozenset({
    "phone","mobile","contact","tel","cell","fax",
    "whatsapp","landline","ph_no","phno","phone_no",
    "telephone","phn","mob","cellphone","handphone"
})

SENTINEL_NUMBERS = {
    -999, -9999, -99999, -999999, -9999999,
    999, 9999, 99999, 999999, 9999999,
    -1, -99, -100, -1000,
    0.0, -0.0,
    1111, 1234, 12345, 123456, 1234567,
    9876, 98765, 9876543,
    11111, 22222, 33333, 44444, 55555, 66666, 77777, 88888,
}

