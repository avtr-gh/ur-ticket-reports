import requests
from clients import URTICKET_API, URTICKET_TOKEN


def fetch_ticket_types_from_api(event_id: int):
    url = f"{URTICKET_API}{event_id}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {URTICKET_TOKEN}",
    }
    response = requests.get(url, headers=headers, timeout=15)
    if response.status_code != 200:
        raise Exception(f"Error API UrTicket ({response.status_code}): {response.text}")
    data = response.json()
    return data.get("saleItems", [])
