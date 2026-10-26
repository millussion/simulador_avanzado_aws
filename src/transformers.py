import hashlib
import json
from datetime import datetime
from typing import Dict, Any, Tuple, List

def compute_record_hash(*args) -> str:
    """Calcula un hash SHA-256 determinista concatenando los argumentos."""
    raw_str = "|".join(str(arg).strip() if arg is not None else "" for arg in args)
    return hashlib.sha256(raw_str.encode("utf-8")).hexdigest()

def validate_historical_record(record: Dict[str, Any]) -> Tuple[bool, str]:
    """Valida reglas de calidad para registros historicos."""
    try:
        price = float(record.get("Price", 0) or 0)
        if price <= 0:
            return False, "NON_POSITIVE_UNIT_PRICE"
    except (ValueError, TypeError):
        return False, "MALFORMED_PRICE"

    if not record.get("Invoice") or not str(record.get("Invoice")).strip():
        return False, "MISSING_INVOICE"

    return True, ""

def process_daily_events_batch(events: List[Dict[str, Any]], batch_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, int]]:
    """Procesa un lote diario deduplicando en memoria y segregando anomalias a cuarentena."""
    seen_ids = set()
    valid_events = []
    quarantined_records = []
    
    metrics = {
        "rows_read": len(events),
        "rows_valid": 0,
        "rows_rejected": 0,
        "rows_duplicate": 0,
        "rows_late": 0
    }

    target_date = datetime.strptime(batch_date, "%Y-%m-%d").date()

    for item in events:
        event_id = str(item.get("event_id", "")).strip()
        
        # Deduplicacion por event_id
        if event_id in seen_ids:
            metrics["rows_duplicate"] += 1
            continue
        seen_ids.add(event_id)

        # Regla de calidad: Monto no positivo
        try:
            amount = float(item.get("amount", 0) or 0)
            if amount <= 0:
                metrics["rows_rejected"] += 1
                quarantined_records.append({
                    "event_id": event_id,
                    "rejection_reason": "NON_POSITIVE_AMOUNT",
                    "raw_payload": json.dumps(item)
                })
                continue
        except (ValueError, TypeError):
            metrics["rows_rejected"] += 1
            quarantined_records.append({
                "event_id": event_id,
                "rejection_reason": "MALFORMED_AMOUNT",
                "raw_payload": json.dumps(item)
            })
            continue

        # Regla de llegada tardia (>24h respecto a la fecha del lote)
        is_late = False
        event_time_str = item.get("event_time", "")
        if event_time_str:
            try:
                clean_time = event_time_str.replace("Z", "+00:00")
                event_dt = datetime.fromisoformat(clean_time)
                if (target_date - event_dt.date()).days >= 1:
                    is_late = True
                    metrics["rows_late"] += 1
            except Exception:
                pass

        record_hash = compute_record_hash(event_id, item.get("order_id", ""))
        
        valid_record = dict(item)
        valid_record["is_late"] = is_late
        valid_record["record_hash"] = record_hash
        valid_record["amount"] = amount
        valid_events.append(valid_record)

    metrics["rows_valid"] = len(valid_events)
    return valid_events, quarantined_records, metrics

# Alias para compatibilidad con la importacion en dag_daily_orders_pipeline.py
process_daily_events = process_daily_events_batch
