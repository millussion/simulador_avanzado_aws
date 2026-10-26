CREATE SCHEMA IF NOT EXISTS analytical;

CREATE TABLE IF NOT EXISTS analytical.dim_product (
    product_key SERIAL PRIMARY KEY,
    sku VARCHAR(64) NOT NULL UNIQUE,
    product_name VARCHAR(255),
    category VARCHAR(100),
    is_active BOOLEAN,
    catalog_updated_at TIMESTAMP WITH TIME ZONE,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytical.dim_order (
    order_id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64),
    country VARCHAR(10),
    current_status VARCHAR(32),
    created_at TIMESTAMP WITH TIME ZONE,
    paid_at TIMESTAMP WITH TIME ZONE,
    cancelled_at TIMESTAMP WITH TIME ZONE,
    refunded_at TIMESTAMP WITH TIME ZONE,
    is_late_processed BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytical.fact_order_events (
    event_surrogate_key SERIAL PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    order_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    country VARCHAR(10) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    amount NUMERIC(12, 2),
    amount_usd NUMERIC(12, 2),
    is_late BOOLEAN DEFAULT FALSE,
    batch_id VARCHAR(64) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    record_hash VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS analytical.quarantine_records (
    quarantine_id SERIAL PRIMARY KEY,
    source_name VARCHAR(64) NOT NULL,
    record_identifier VARCHAR(128),
    rejection_reason VARCHAR(255) NOT NULL,
    raw_payload TEXT,
    batch_id VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytical.etl_batch_audit (
    batch_id VARCHAR(64) PRIMARY KEY,
    batch_date DATE NOT NULL,
    source_name VARCHAR(64) NOT NULL,
    rows_read INTEGER DEFAULT 0,
    rows_valid INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    rows_duplicate INTEGER DEFAULT 0,
    rows_late INTEGER DEFAULT 0,
    rows_published INTEGER DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);