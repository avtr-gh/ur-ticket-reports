import csv
from io import StringIO
from datetime import datetime, timezone

from clients import supabase, fetch_latest_csv
from parsers import to_int, parse_currency, map_payment_method, parse_datetime
from urticket import fetch_ticket_types_from_api


def sale_needs_update(existing_row: dict, new_obj: dict) -> bool:
    def n(v):
        return v if v is not None else 0

    if n(existing_row.get("qty")) != n(new_obj.get("qty")):
        return True
    if float(n(existing_row.get("price_gross"))) != float(n(new_obj.get("price_gross"))):
        return True
    if float(n(existing_row.get("price_net"))) != float(n(new_obj.get("price_net"))):
        return True
    if float(n(existing_row.get("refund"))) != float(n(new_obj.get("refund"))):
        return True
    if float(n(existing_row.get("fee"))) != float(n(new_obj.get("fee"))):
        return True
    if float(n(existing_row.get("discount"))) != float(n(new_obj.get("discount"))):
        return True

    eg = existing_row.get("payment_gateway")
    ng = new_obj.get("payment_gateway")
    if (eg or "") != (ng or ""):
        return True

    return False


def process_event_new(event_id: int, event_rows: list):
    if not event_rows:
        return {"success": False, "message": "No rows for event"}

    first = event_rows[0]
    event_name = first.get("event_name")
    start_datetime_raw = first.get("start_datetime")
    start_datetime = parse_datetime(start_datetime_raw)
    if start_datetime is None and start_datetime_raw:
        print(f"Warning: couldn't parse start_datetime '{start_datetime_raw}' for event {event_id}; storing NULL")

    end_datetime_raw = first.get("end_datetime")
    end_datetime = None
    if end_datetime_raw:
        try:
            end_datetime = str(datetime.strptime(end_datetime_raw[:10], "%Y-%m-%d").date())
        except Exception:
            print(f"Warning: couldn't parse end_datetime '{end_datetime_raw}' for event {event_id}; storing NULL")

    supabase.table("events").upsert({
        "id": event_id,
        "name": event_name,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime
    }).execute()

    try:
        sale_items = fetch_ticket_types_from_api(event_id)
    except Exception as e:
        sale_items = []
        print(f"Warning: failed to fetch sale items for event {event_id}: {e}")

    for item in sale_items:
        total_stock = item.get("totalStock") or 0
        supabase.table("ticket_type").upsert({
            "id": item["itemId"],
            "event_id": event_id,
            "ticket_name": item.get("name"),
            "total_stock": int(total_stock)
        }).execute()

    inserted = 0
    for row in event_rows:
        ticket_type_id = to_int(row.get("ticket_type_id"))
        if ticket_type_id == 0:
            continue

        qty = to_int(row.get("total_tickets") or row.get("qty"))
        payment_method_id = map_payment_method(row.get("payment_method")) or 6

        price_gross = parse_currency(row.get("price_gross"))
        refund_total = parse_currency(row.get("refund_online")) + parse_currency(row.get("refund_offline"))
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
            "price_net": price_net,
            "refund": refund_total,
            "fee": fee,
            "discount": discount,
            "payment_gateway": payment_gateway
        }).execute()
        inserted += 1

    return {"success": True, "event_id": event_id, "inserted_sales": inserted}


def sync_existing_event(event_id: int, event_rows: list):
    resp_tt = supabase.table("ticket_type").select("*").eq("event_id", event_id).execute()
    existing_tt = {}
    if resp_tt.data:
        for t in resp_tt.data:
            existing_tt[t["id"]] = t

    try:
        api_items = fetch_ticket_types_from_api(event_id)
    except Exception as e:
        api_items = []
        print(f"Warning: failed to fetch sale items for event {event_id}: {e}")

    api_map = {it["itemId"]: it for it in api_items}

    for item_id, item in api_map.items():
        total_stock = int(item.get("totalStock") or 0)
        if item_id not in existing_tt:
            supabase.table("ticket_type").insert({
                "id": item_id,
                "event_id": event_id,
                "ticket_name": item.get("name"),
                "total_stock": total_stock
            }).execute()
        else:
            if existing_tt[item_id].get("total_stock") != total_stock:
                supabase.table("ticket_type").update({
                    "total_stock": total_stock
                }).eq("id", item_id).execute()

    resp_sales = supabase.table("event_sales").select("*").eq("event_id", event_id).execute()
    existing_sales = {}
    if resp_sales.data:
        for s in resp_sales.data:
            key = (s.get("ticket_type_id"), s.get("payment_method_id"))
            existing_sales[key] = s

    inserts = 0
    updates = 0
    for row in event_rows:
        ticket_type_id = to_int(row.get("ticket_type_id"))
        if ticket_type_id == 0:
            continue

        payment_method_id = map_payment_method(row.get("payment_method")) or 6

        qty = to_int(row.get("total_tickets"))
        price_gross = parse_currency(row.get("price_gross"))
        refund_total = parse_currency(row.get("refund_online")) + parse_currency(row.get("refund_offline"))
        fee = parse_currency(row.get("fee"))
        discount = parse_currency(row.get("discount"))
        price_net = parse_currency(row.get("price_net"))

        payment_gateway_raw = row.get("payment_gateway")
        payment_gateway = payment_gateway_raw.strip() if payment_gateway_raw else None
        if payment_gateway == "":
            payment_gateway = None

        key = (ticket_type_id, payment_method_id)
        new_obj = {
            "event_id": event_id,
            "ticket_type_id": ticket_type_id,
            "payment_method_id": payment_method_id,
            "qty": qty,
            "price_gross": price_gross,
            "price_net": price_net,
            "refund": refund_total,
            "fee": fee,
            "discount": discount,
            "payment_gateway": payment_gateway
        }

        if key not in existing_sales:
            supabase.table("event_sales").insert(new_obj).execute()
            inserts += 1
        else:
            existing_row = existing_sales[key]
            if sale_needs_update(existing_row, new_obj):
                supabase.table("event_sales").update(new_obj).eq("id", existing_row["id"]).execute()
                updates += 1

    return {"event_id": event_id, "inserted": inserts, "updated": updates}


def sync_events_and_sales():
    csv_content = fetch_latest_csv()
    if not csv_content:
        return {"success": False, "message": "No CSV found"}

    reader = csv.DictReader(StringIO(csv_content))
    rows = list(reader)
    if not rows:
        return {"success": False, "message": "CSV vacío"}

    events_map = {}
    for row in rows:
        eid = to_int(row.get("event_id"))
        if eid == 0:
            continue
        events_map.setdefault(eid, []).append(row)

    today = datetime.now(timezone.utc).date().isoformat()
    resp = supabase.table("events").select("id, end_datetime").gte("end_datetime", today).execute()
    existing_event_ids = set()
    if resp.data:
        for e in resp.data:
            existing_event_ids.add(e["id"])

    results = {"processed": []}

    for event_id, ev_rows in events_map.items():
        if event_id not in existing_event_ids:
            r = process_event_new(event_id, ev_rows)
            results["processed"].append({"event_id": event_id, "action": "inserted", "detail": r})
        else:
            r = sync_existing_event(event_id, ev_rows)
            results["processed"].append({"event_id": event_id, "action": "synced", "detail": r})

    return {"success": True, "results": results}


def process_and_sync():
    return sync_events_and_sales()
