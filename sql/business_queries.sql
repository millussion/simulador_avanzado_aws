-- ============================================================================
-- 1. Ventas brutas, devoluciones y ventas netas por país y mes (Moneda Normalizada)
-- ============================================================================
WITH monthly_data AS (
    SELECT 
        country,
        DATE_TRUNC('month', event_time)::date AS sales_month,
        COUNT(DISTINCT COALESCE(invoice_no, order_id)) AS total_orders,
        SUM(quantity) AS total_units_sold,
        ROUND(SUM(gross_amount_usd)::numeric, 2) AS total_gross_usd,
        ROUND(SUM(CASE WHEN is_return THEN gross_amount_usd ELSE 0 END)::numeric, 2) AS total_returns_usd,
        ROUND(SUM(gross_amount_usd)::numeric, 2) AS total_net_usd
    FROM analytical.fact_order_lines
    GROUP BY country, DATE_TRUNC('month', event_time)
)
SELECT 
    sales_month,
    country,
    total_orders,
    total_gross_usd,
    total_returns_usd,
    total_net_usd
FROM monthly_data
ORDER BY sales_month DESC, total_net_usd DESC
LIMIT 15;

-- ============================================================================
-- 2. Categorías con mayores ingresos y proporción de devoluciones
-- ============================================================================
SELECT 
    COALESCE(p.category, 'Sin Categoría') AS category,
    COUNT(DISTINCT COALESCE(l.invoice_no, l.order_id)) AS total_orders,
    ROUND(SUM(l.gross_amount_usd)::numeric, 2) AS gross_revenue_usd,
    ROUND(ABS(SUM(CASE WHEN l.is_return THEN l.gross_amount_usd ELSE 0 END))::numeric, 2) AS returns_amount_usd,
    ROUND(
        (COUNT(CASE WHEN l.is_return THEN 1 END)::numeric / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS return_rate_pct
FROM analytical.fact_order_lines l
LEFT JOIN analytical.dim_product p ON l.sku = p.sku
GROUP BY p.category
ORDER BY gross_revenue_usd DESC;

-- ============================================================================
-- 3. Top 10 SKUs con mayor venta neta y Top 10 con mayor tasa de devolución
-- ============================================================================
-- Top 10 SKUs por Venta Neta
SELECT 
    l.sku,
    COALESCE(p.product_name, 'No registrado en catálogo') AS product_name,
    SUM(l.quantity) AS net_units_sold,
    ROUND(SUM(l.gross_amount_usd)::numeric, 2) AS net_revenue_usd
FROM analytical.fact_order_lines l
LEFT JOIN analytical.dim_product p ON l.sku = p.sku
GROUP BY l.sku, p.product_name
ORDER BY net_revenue_usd DESC
LIMIT 10;

-- Top 10 SKUs con más devoluciones (mínimo 50 movimientos)
SELECT 
    l.sku,
    COALESCE(p.product_name, 'No registrado en catálogo') AS product_name,
    COUNT(CASE WHEN l.is_return THEN 1 END) AS return_events_count,
    ROUND(
        (COUNT(CASE WHEN l.is_return THEN 1 END)::numeric / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS return_rate_pct,
    ROUND(ABS(SUM(CASE WHEN l.is_return THEN l.gross_amount_usd ELSE 0 END))::numeric, 2) AS returned_amount_usd
FROM analytical.fact_order_lines l
LEFT JOIN analytical.dim_product p ON l.sku = p.sku
GROUP BY l.sku, p.product_name
HAVING COUNT(*) >= 50
ORDER BY return_rate_pct DESC, returned_amount_usd DESC
LIMIT 10;

-- ============================================================================
-- 4. Tiempo promedio de creación a pago y proporción de pedidos cancelados
-- ============================================================================
WITH order_lifecycle AS (
    SELECT 
        order_id,
        MIN(CASE WHEN event_type = 'created' THEN event_time END) AS created_time,
        MIN(CASE WHEN event_type = 'paid' THEN event_time END) AS paid_time,
        MAX(CASE WHEN event_type = 'cancelled' THEN 1 ELSE 0 END) AS is_cancelled
    FROM analytical.fact_order_events
    GROUP BY order_id
)
SELECT 
    COUNT(*) AS total_tracked_orders,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (paid_time - created_time)) / 60)::numeric, 
        2
    ) AS avg_minutes_created_to_paid,
    SUM(is_cancelled) AS total_cancelled_orders,
    ROUND(
        (SUM(is_cancelled)::numeric / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS cancellation_rate_pct
FROM order_lifecycle;

-- ============================================================================
-- 5. Proporción de eventos tardíos (>24h) y estado de conciliación
-- ============================================================================
SELECT 
    is_late,
    COUNT(*) AS total_events,
    ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER ()) * 100, 2) AS percentage_of_total,
    ROUND(SUM(amount_usd)::numeric, 2) AS total_volume_usd
FROM analytical.fact_order_events
GROUP BY is_late;

-- ============================================================================
-- 6. Porcentaje de rechazos por fuente y evolución en los últimos 7 lotes
-- ============================================================================
SELECT 
    batch_id,
    source_name,
    batch_date,
    rows_read,
    rows_valid,
    rows_rejected,
    ROUND((rows_rejected::numeric / NULLIF(rows_read, 0)) * 100, 2) AS rejection_rate_pct,
    status
FROM analytical.etl_batch_audit
ORDER BY batch_date DESC
LIMIT 7;
