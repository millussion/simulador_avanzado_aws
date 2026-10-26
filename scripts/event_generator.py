import json
import random
import uuid
from datetime import datetime, timedelta

TOTAL_EVENTS = 50000
DUPLICATE_RATE = 0.03
LATE_RATE = 0.05
INVALID_RATE = 0.01

EVENT_TYPES = ["created", "paid", "cancelled", "refunded"]
EVENT_WEIGHTS = [0.45, 0.40, 0.10, 0.05]

COUNTRIES = ["US", "MX", "BR", "CO", "AR", "CL"]
CURRENCY_MAP = {
    "US": "USD",
    "MX": "MXN",
    "BR": "BRL",
    "CO": "COP",
    "AR": "ARS",
    "CL": "CLP"
}

BASE_PRICES = {
    "USD": (10, 500),
    "MXN": (200, 10000),
    "BRL": (50, 2500),
    "COP": (40000, 2000000),
    "ARS": (8000, 400000),
    "CLP": (9000, 450000)
}

SKU_CATALOG = [f"SKU-{i:05d}" for i in range(1, 501)]

def generate_events(target_date_str="2026-10-25", output_path="/opt/airflow/events_20261025.jsonl"):
    random.seed(42)  # Semilla fija para reproducibilidad determinista
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    events = []

    print(f"Generando {TOTAL_EVENTS} eventos sintéticos para la fecha {target_date_str}...")

    for i in range(TOTAL_EVENTS):
        event_id = str(uuid.uuid4())
        country = random.choice(COUNTRIES)
        currency = CURRENCY_MAP[country]
        order_id = f"ORD-{random.randint(100000, 999999)}"
        customer_id = f"CUST-{random.randint(1000, 9999)}" if random.random() > 0.1 else None
        event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

        is_late = random.random() < LATE_RATE
        if is_late:
            delta_days = random.randint(2, 5)
            event_dt = target_date - timedelta(days=delta_days, seconds=random.randint(0, 86400))
        else:
            event_dt = target_date + timedelta(seconds=random.randint(0, 86400 - 1))

        price_range = BASE_PRICES[currency]
        amount = round(random.uniform(price_range[0], price_range[1]), 2)

        # Inyectar anomalía para prueba de cuarentena
        if random.random() < INVALID_RATE:
            amount = -50.0

        num_items = random.randint(1, 4)
        skus = random.sample(SKU_CATALOG, k=num_items)

        event = {
            "event_id": event_id,
            "order_id": order_id,
            "event_type": event_type,
            "event_time": event_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "country": country,
            "currency": currency,
            "payload": {
                "customer_id": customer_id,
                "amount": amount,
                "sku_list": skus
            }
        }
        events.append(event)

        # Inyectar duplicado con mismo event_id
        if random.random() < DUPLICATE_RATE:
            dup_event = dict(event)
            events.append(dup_event)

    with open(output_path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")

    print(f"Archivo generado con éxito en {output_path} ({len(events)} líneas totales incluyendo duplicados).")

if __name__ == "__main__":
    generate_events()