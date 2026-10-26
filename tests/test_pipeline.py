import pytest
from datetime import datetime
from src.transformers import (
    compute_record_hash,
    process_daily_events_batch,
    validate_historical_record,
)

# -----------------------------------------------------------------------------
# 1. Pruebas de Hash e Idempotencia
# -----------------------------------------------------------------------------
def test_compute_record_hash_consistency():
    """El hash debe ser estrictamente determinista ante los mismos valores."""
    record_a = {"event_id": "EVT-1001", "order_id": "ORD-500", "amount": 150.50}
    record_b = {"event_id": "EVT-1001", "order_id": "ORD-500", "amount": 150.50}
    
    hash_a = compute_record_hash(record_a["event_id"], record_a["order_id"])
    hash_b = compute_record_hash(record_b["event_id"], record_b["order_id"])
    
    assert hash_a == hash_b
    assert len(hash_a) == 64  # SHA-256 hex digest

def test_compute_record_hash_different_inputs():
    """Registros con distinto event_id deben producir hashes distintos."""
    hash_1 = compute_record_hash("EVT-001", "ORD-100")
    hash_2 = compute_record_hash("EVT-002", "ORD-100")
    assert hash_1 != hash_2

# -----------------------------------------------------------------------------
# 2. Pruebas de Deduplicación y Detección de Eventos Tardíos (Batch Diario)
# -----------------------------------------------------------------------------
def test_process_daily_events_deduplication():
    """Los eventos duplicados deben contabilizarse y no incluirse en valid_events."""
    batch_date = "2026-10-25"
    events = [
        {"event_id": "E1", "order_id": "O1", "event_type": "paid", "event_time": "2026-10-25T10:00:00Z", "country": "Colombia", "currency": "COP", "amount": 50000},
        {"event_id": "E1", "order_id": "O1", "event_type": "paid", "event_time": "2026-10-25T10:00:00Z", "country": "Colombia", "currency": "COP", "amount": 50000}, # Duplicado
        {"event_id": "E2", "order_id": "O2", "event_type": "paid", "event_time": "2026-10-25T11:00:00Z", "country": "Colombia", "currency": "COP", "amount": 75000},
    ]

    valid, quarantined, metrics = process_daily_events_batch(events, batch_date=batch_date)
    
    assert metrics["rows_read"] == 3
    assert metrics["rows_duplicate"] == 1
    assert len(valid) == 2
    assert {e["event_id"] for e in valid} == {"E1", "E2"}

def test_process_daily_events_late_arrivals():
    """Eventos ocurridos >24h antes de la fecha del lote deben marcarse como is_late=True."""
    batch_date = "2026-10-25"
    events = [
        {"event_id": "E_ON_TIME", "order_id": "O1", "event_type": "paid", "event_time": "2026-10-25T08:00:00Z", "country": "Colombia", "currency": "COP", "amount": 1000},
        {"event_id": "E_LATE", "order_id": "O2", "event_type": "paid", "event_time": "2026-10-23T08:00:00Z", "country": "Colombia", "currency": "COP", "amount": 2000},
    ]

    valid, quarantined, metrics = process_daily_events_batch(events, batch_date=batch_date)
    
    assert metrics["rows_late"] == 1
    late_event = next(e for e in valid if e["event_id"] == "E_LATE")
    assert late_event["is_late"] is True

# -----------------------------------------------------------------------------
# 3. Pruebas de Reglas de Cuarentena (Precios No Positivos y Datos Inválidos)
# -----------------------------------------------------------------------------
def test_quarantine_negative_or_zero_price_daily():
    """Montos <= 0 deben aislarse en cuarentena con el motivo registrado."""
    batch_date = "2026-10-25"
    events = [
        {"event_id": "E_ZERO", "order_id": "O1", "event_type": "paid", "event_time": "2026-10-25T10:00:00Z", "country": "Colombia", "currency": "COP", "amount": 0.0},
        {"event_id": "E_NEG", "order_id": "O2", "event_type": "paid", "event_time": "2026-10-25T11:00:00Z", "country": "Colombia", "currency": "COP", "amount": -150.0},
    ]

    valid, quarantined, metrics = process_daily_events_batch(events, batch_date=batch_date)
    
    assert len(valid) == 0
    assert len(quarantined) == 2
    assert metrics["rows_rejected"] == 2
    for q in quarantined:
        assert "NON_POSITIVE_AMOUNT" in q["rejection_reason"]

def test_quarantine_historical_record_invalid_price():
    """Registros del dataset histórico con Price <= 0 deben ser marcados como inválidos."""
    invalid_record = {
        "Invoice": "536365",
        "StockCode": "85123A",
        "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
        "Quantity": "6",
        "InvoiceDate": "2010-12-01 08:26:00",
        "Price": "0.0",
        "Customer ID": "17850",
        "Country": "United Kingdom"
    }

    is_valid, reason = validate_historical_record(invalid_record)
    assert is_valid is False
    assert reason == "NON_POSITIVE_UNIT_PRICE"
