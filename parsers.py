import unicodedata
import re
from datetime import datetime, timezone


def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return text.lower().strip()


def map_payment_method(raw: str):
    if not raw:
        return None
    text = normalize_text(raw)

    if "efectivo" in text:
        return 2
    if "tarjeta presente" in text:
        return 3
    if "tarjeta (en" in text or "tarjeta en linea" in text or "tarjeta en línea" in text:
        return 1
    if "gratis" in text:
        return 5
    return None


def parse_currency(value) -> float:
    if value is None:
        return 0.0
    s = str(value).strip()
    if s == "":
        return 0.0
    s = s.replace("\xa0", " ")

    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()

    s = re.sub(r"[^0-9,\.\-]", "", s)
    s = s.replace(" ", "")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace('.', '')
            s = s.replace(',', '.')
        else:
            s = s.replace(',', '')
    elif "," in s and "." not in s:
        if s.count(',') > 1:
            s = s.replace(',', '')
        else:
            s = s.replace(',', '.')
    elif "." in s and "," not in s:
        if s.count('.') > 1:
            s = s.replace('.', '')

    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if m:
        num_str = m.group(0)
    else:
        s2 = re.sub(r"[^0-9\.\-]", "", s)
        if s2 in ("", ".", "-"):
            return 0.0
        num_str = s2

    try:
        val = float(num_str)
    except:
        return 0.0

    if is_negative:
        val = -abs(val)
    return val


def to_float(value):
    return float(parse_currency(value))


def to_int(value):
    v = parse_currency(value)
    try:
        return int(v)
    except:
        return 0


def parse_datetime(value: str):
    if not value:
        return None
    v = str(value).strip()
    if "T" in v or "+" in v or "Z" in v:
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass

    formats = [
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for f in formats:
        try:
            dt = datetime.strptime(v, f)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            continue

    return None
