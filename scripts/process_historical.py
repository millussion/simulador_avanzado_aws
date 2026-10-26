import os
import csv
import hashlib
from datetime import datetime
import psycopg2

def compute_hash(*values):
    serialized = "|".join([str(v).strip() for v in values])
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

def process_historical_csv(csv_path: str, db_url: str):
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    
    batch_id = "HISTORICAL_INIT_LOAD"
    now_utc = datetime.utcnow()
    
    rates = {"GBP": 1.30, "USD": 1.0, "EUR": 1.08}  # Tasas para normalizar retail original
    
    rows_read = 0
    rows_valid = 0
    rows_rejected = 0
    rows_duplicate = 0
    
    seen_hashes = set()
    
    print(f"Iniciando procesamiento de {csv_path}...")
    
    with open(csv_path, mode="r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows_read += 1
            
            # Mapear nombres habituales del dataset Online Retail
            invoice = (row.get("Invoice") or row.get("InvoiceNo") or "").strip()
            stock_code = (row.get("StockCode") or "").strip().upper()
            description = (row.get("Description") or "").strip()
            quantity_str = (row.get("Quantity") or "0").strip()
            invoice_date_str = (row.get("InvoiceDate") or "").strip()
            price_str = (row.get("Price") or row.get("UnitPrice") or "0").strip()
            customer_id = (row.get("Customer ID") or row.get("CustomerID") or "").strip()
            country = (row.get("Country") or "UNKNOWN").strip().upper()
            
            # Validación de SKU obligatorio
            if not stock_code:
                rows_rejected += 1
                cur.execute("""
                    INSERT INTO analytical.quarantine_records 
                    (source_name, record_identifier, rejection_reason, raw_payload, batch_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, ("kaggle_online_retail", invoice or "UNKNOWN", "MISSING_SKU", str(row), batch_id))
                continue
                
            try:
                quantity = int(quantity_str)
                price = float(price_str)
            except ValueError:
                rows_rejected += 1
                cur.execute("""
                    INSERT INTO analytical.quarantine_records 
                    (source_name, record_identifier, rejection_reason, raw_payload, batch_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, ("kaggle_online_retail", invoice or "UNKNOWN", "INVALID_NUMERIC_VALUES", str(row), batch_id))
                continue
                
            # Parseo de fecha
            try:
                # Formato típico: "2010-12-01 08:26:00" o "12/01/2010 08:26"
                if "-" in invoice_date_str:
                    event_time = datetime.strptime(invoice_date_str, "%Y-%m-%d %H:%M:%S")
                else:
                    event_time = datetime.strptime(invoice_date_str, "%m/%d/%Y %H:%M")
            except Exception:
                rows_rejected += 1
                cur.execute("""
                    INSERT INTO analytical.quarantine_records 
                    (source_name, record_identifier, rejection_reason, raw_payload, batch_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, ("kaggle_online_retail", invoice or "UNKNOWN", "INVALID_INVOICE_DATE", str(row), batch_id))
                continue

            # Reglas de negocio: devoluciones vs ventas y precio <= 0 en ventas
            is_return = invoice.startswith("C") or quantity < 0
            
            if not is_return and price <= 0:
                rows_rejected += 1
                cur.execute("""
                    INSERT INTO analytical.quarantine_records 
                    (source_name, record_identifier, rejection_reason, raw_payload, batch_id)
                    VALUES (%s, %s, %s, %s, %s);
                """, ("kaggle_online_retail", invoice, "NON_POSITIVE_PRICE_ON_SALE", str(row), batch_id))
                continue

            # Tratamiento de Customer ID ausente
            final_cust_id = customer_id if customer_id else "ANONYMOUS"
            
            # Hash determinista de deduplicación
            rec_hash = compute_hash(invoice, stock_code, event_time.isoformat(), quantity, price)
            if rec_hash in seen_hashes:
                rows_duplicate += 1
                continue
            seen_hashes.add(rec_hash)

            gross_amount = round(abs(quantity) * price, 2)
            rate_usd = rates.get("GBP", 1.30)
            gross_amount_usd = round(gross_amount * rate_usd, 2)

            # Inserción en dimensión de productos (si no existe)
            cur.execute("""
                INSERT INTO analytical.dim_product (sku, product_name, category, is_active, catalog_updated_at)
                VALUES (%s, %s, 'General Retail', TRUE, %s)
                ON CONFLICT (sku) DO NOTHING;
            """, (stock_code, description, now_utc))

            # Inserción en tabla de hechos de líneas de pedido
            cur.execute("""
                INSERT INTO analytical.fact_order_lines 
                (invoice_no, order_id, sku, quantity, unit_price, gross_amount, gross_amount_usd, is_return, event_time, country, currency, batch_id, record_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (record_hash) DO NOTHING;
            """, (invoice, invoice, stock_code, quantity, price, gross_amount, gross_amount_usd, is_return, event_time, country, "GBP", batch_id, rec_hash))

            # Registrar en fact_order_events para consolidar analítica
            event_type = "refunded" if is_return else "paid"
            cur.execute("""
                INSERT INTO analytical.fact_order_events
                (event_id, order_id, event_type, event_time, country, currency, amount, amount_usd, is_late, batch_id, record_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING;
            """, (f"EVT_{rec_hash[:16]}", invoice, event_type, event_time, country, "GBP", gross_amount, gross_amount_usd, False, batch_id, rec_hash))

            rows_valid += 1
            if rows_read % 10000 == 0:
                conn.commit()
                print(f"Procesados {rows_read} registros...")

    # Registrar auditoría final
    cur.execute("""
        INSERT INTO analytical.etl_batch_audit 
        (batch_id, batch_date, source_name, rows_read, rows_valid, rows_rejected, rows_duplicate, rows_late, rows_published, status)
        VALUES (%s, CURRENT_DATE, %s, %s, %s, %s, %s, 0, %s, 'COMPLETED')
        ON CONFLICT (batch_id) DO NOTHING;
    """, (batch_id, "kaggle_online_retail", rows_read, rows_valid, rows_rejected, rows_duplicate, rows_valid))

    conn.commit()
    cur.close()
    conn.close()
    print(f"Carga histórica completa: {rows_valid} filas válidas, {rows_rejected} en cuarentena, {rows_duplicate} duplicados.")

if __name__ == "__main__":
    db = os.getenv("ANALYTICS_DB_URL", "postgresql://analytics_user:analytics_pass@postgres-analytics:5432/analytics_db")
    path = "/opt/airflow/data/historical/online_retail.csv"
    process_historical_csv(path, db)