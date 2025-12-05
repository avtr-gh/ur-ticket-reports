import os
import csv
import boto3
import requests
import unicodedata
import re
from io import StringIO
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

AWS_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

URTICKET_API = os.getenv("URTICKET_API")
URTICKET_TOKEN = os.getenv("URTICKET_TOKEN")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

# Mapear métodos de pago
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

# Parsear cantidades con letras (MXN) o comas
def parse_currency(value) -> float:
    if value is None:
        return 0.0

    s = str(value).strip()
    if s == "":
        return 0.0

    s = s.replace("\xa0", " ")

    # Negativos con paréntesis
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1].strip()

    # Quitar texto y símbolos monetarios
    s = re.sub(r"[A-Za-z\$€£¥]", "", s)
    s = s.replace(" ", "")

    # remover separadores de miles
    s = s.replace(",", "")

    # Buscar número válido
    m = re.search(r"-?\d+(\.\d+)?", s)
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


# AWS / SUPABASE 
s3 = boto3.client("s3", region_name=AWS_REGION)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Bucket S3
def fetch_latest_csv():
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=AWS_BUCKET)

    csv_files = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                csv_files.append(obj)

    if not csv_files:
        return None

    latest = max(csv_files, key=lambda x: x["LastModified"])
    key = latest["Key"]

    obj = s3.get_object(Bucket=AWS_BUCKET, Key=key)
    content = obj["Body"].read().decode("utf-8")
    return content


# API URTICKET
def fetch_ticket_types_from_api(event_id: int):
    url = f"{URTICKET_API}{event_id}"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {URTICKET_TOKEN}",
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error API UrTicket: {response.text}")

    data = response.json()
    return data.get("saleItems", [])


# Main
def process_and_sync():
    csv_content = fetch_latest_csv()
    if not csv_content:
        return {"success": False, "message": "No CSV found"}

    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)

    if not rows:
        return {"success": False, "message": "CSV vacío"}

    # Detectar múltiples eventos en CSV
    events_dict = {}

    for row in rows:
        event_id = to_int(row.get("event_id"))
        if event_id == 0:
            continue
        if event_id not in events_dict:
            events_dict[event_id] = []
        events_dict[event_id].append(row)

    # Procesar cada evento
    for event_id, event_rows in events_dict.items():

        first = event_rows[0]
        event_name = first.get("event_name")
        start_datetime = first.get("start_datetime")

        # 1) UPSERT EVENTO
        supabase.table("events").upsert({
            "id": event_id,
            "name": event_name,
            "start_datetime": start_datetime
        }).execute()

        # 2) UPSERT TICKET TYPES DESDE API
        sale_items = fetch_ticket_types_from_api(event_id)

        for item in sale_items:
            total_stock = item.get("totalStock") or 0

            supabase.table("ticket_type").upsert({
                "id": item["itemId"],
                "event_id": event_id,
                "ticket_name": item.get("name"),
                "total_stock": int(total_stock)
            }).execute()

        # 3) INSERTAR VENTAS
        for row in event_rows:

            ticket_type_id = to_int(row.get("ticket_type_id"))
            if ticket_type_id == 0:
                continue

            qty = to_int(row.get("total_tickets"))
            payment_method_id = map_payment_method(row.get("payment_method"))

            #Cantidades
            price_gross = parse_currency(row.get("price_gross"))
            refund_online = parse_currency(row.get("refund_online"))
            refund_offline = parse_currency(row.get("refund_offline"))
            refund_total = refund_online + refund_offline
            fee = parse_currency(row.get("fee"))
            discount = parse_currency(row.get("discount"))
            price_net = parse_currency(row.get("price_net"))

            payment_gateway_raw = row.get("payment_gateway")
            payment_gateway = payment_gateway_raw.strip() if payment_gateway_raw else None
            if payment_gateway == "":
                payment_gateway = None

            supabase.table("event_sales").insert({
                "event_id": event_id,
                "ticket_type_id": ticket_type_id,
                "payment_method_id": payment_method_id,
                "qty": qty,
                "price_gross": price_gross,
                "refund": refund_total,
                "fee": fee,
                "discount": discount,
                "price_net": price_net,
                "payment_gateway": payment_gateway
            }).execute()

    return {"success": True, "processed_events": list(events_dict.keys())}


if __name__ == "__main__":
    print(process_and_sync())
