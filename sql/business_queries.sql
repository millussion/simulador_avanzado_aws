-- ============================================================================
-- 1. Ventas e ingresos netos consolidados por país (Histórico + Eventos Diarios)
-- ============================================================================
WITH historical_sales AS (
    SELECT 
        country,
        COUNT(DISTINCT invoice_no) AS total_orders,
        SUM(quantity) AS total_units_sold,
        ROUND(SUM(gross_amount_usd)::numeric, 2) AS total_gross_usd,
        ROUND(SUM(CASE WHEN is_return THEN gross_amount_usd ELSE 0 END)::numeric, 2) AS total_returns_usd,
        ROUND(SUM(gross_amount_usd)::numeric, 2) AS total_net_usd
    FROM analytical.fact_order_lines
    GROUP BY country
),
daily_sales AS (
    SELECT 
        country,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(*) AS total_units_sold,
        ROUND(SUM(amount_usd)::numeric, 2) AS total_gross_usd,
        ROUND(SUM(CASE WHEN event_type = 'refunded' THEN amount_usd ELSE 0 END)::numeric, 2) AS total_returns_usd,
        ROUND(SUM(CASE WHEN event_type = 'refunded' THEN -amount_usd ELSE amount_usd END)::numeric, 2) AS total_net_usd
    FROM analytical.fact_order_events
    WHERE event_type IN ('paid', 'refunded', 'completed')
    GROUP BY country
)
SELECT 
    COALESCE(h.country, d.country) AS country,
    COALESCE(h.total_orders, 0) + COALESCE(d.total_orders, 0) AS total_orders,
    COALESCE(h.total_net_usd, 0) + COALESCE(d.total_net_usd, 0) AS net_revenue_usd
FROM historical_sales h
FULL OUTER JOIN daily_sales d ON h.country = d.country
ORDER BY net_revenue_usd DESC
LIMIT 10;

-- ============================================================================
-- 2. Tasa de devoluciones por país y por línea de producto
-- ============================================================================
SELECT 
    country,
    COUNT(*) AS total_line_items,
    COUNT(CASE WHEN is_return THEN 1 END) AS returned_items,
    ROUND(
        (COUNT(CASE WHEN is_return THEN 1 END)::numeric / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS return_rate_percentage,
    ROUND(ABS(SUM(CASE WHEN is_return THEN gross_amount_usd ELSE 0 END))::numeric, 2) AS returned_amount_usd
FROM analytical.fact_order_lines
GROUP BY country
HAVING COUNT(*) > 1000
ORDER BY return_rate_percentage DESC
LIMIT 10;

-- ============================================================================
-- 3. Análisis de eventos tardíos (Late Arrivals > 24h) y su impacto financiero
-- ============================================================================
SELECT 
    is_late,
    COUNT(*) AS event_count,
    ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total,
    ROUND(SUM(amount_usd)::numeric, 2) AS total_amount_usd,
    ROUND(AVG(amount_usd)::numeric, 2) AS avg_ticket_usd
FROM analytical.fact_order_events
GROUP BY is_late;

-- ============================================================================
-- 4. Métricas de calidad y auditoría por lote de ingesta
-- ============================================================================
SELECT 
    b.batch_id,
    b.source_name,
    b.batch_date,
    b.rows_read,
    b.rows_valid,
    b.rows_rejected,
    b.rows_duplicate,
    b.rows_late,
    ROUND((b.rows_rejected::numeric / NULLIF(b.rows_read, 0)) * 100, 2) AS rejection_rate_pct,
    b.status
FROM analytical.etl_batch_audit b
ORDER BY b.batch_date DESC;

-- ============================================================================
-- 5. Causas principales de cuarentena (Data Quality Rules Violations)
-- ============================================================================
SELECT 
    source_name,
    rejection_reason,
    COUNT(*) AS quarantined_count,
    ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER (PARTITION BY source_name)) * 100, 2) AS pct_within_source
FROM analytical.quarantine_records
GROUP BY source_name, rejection_reason
ORDER BY source_name, quarantined_count DESC;

-- ============================================================================
-- 6. Rendimiento y precio promedio por categoría enriquecida desde la API Mock
-- ============================================================================
SELECT 
    p.category,
    COUNT(DISTINCT p.sku) AS distinct_products_in_catalog,
    ROUND(AVG(p.current_price)::numeric, 2) AS catalog_avg_price,
    COALESCE(SUM(l.quantity), 0) AS total_units_sold,
    COALESCE(ROUND(SUM(l.gross_amount_usd)::numeric, 2), 0) AS total_historical_sales_usd
FROM analytical.dim_product p
LEFT JOIN analytical.fact_order_lines l ON p.sku = l.stock_code
GROUP BY p.category
ORDER BY total_historical_sales_usd DESC;
