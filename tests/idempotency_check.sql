-- ============================================================================
-- VERIFICACIÓN DE IDEMPOTENCIA
-- Comprueba que no existan hashes duplicados en fact_order_events.
-- ============================================================================

SELECT
    'fact_order_events' AS tabla,
    COUNT(*) AS total_filas,
    COUNT(record_hash) AS hashes_no_nulos,
    COUNT(DISTINCT record_hash) AS total_hashes_unicos,
    COUNT(*) - COUNT(DISTINCT record_hash) AS posibles_duplicados_o_nulos,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT record_hash)
        THEN 'IDEMPOTENCIA EXITOSA: SIN DUPLICADOS NI HASHES NULOS'
        ELSE 'FALLO: REVISAR DUPLICADOS O HASHES NULOS'
    END AS estado_integridad
FROM analytical.fact_order_events;

-- Verifica si la base de datos tiene una restricción UNIQUE
-- asociada a record_hash.

SELECT
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
    ON tc.constraint_catalog = kcu.constraint_catalog
   AND tc.constraint_schema = kcu.constraint_schema
   AND tc.constraint_name = kcu.constraint_name
WHERE tc.table_schema = 'analytical'
  AND tc.table_name = 'fact_order_events'
  AND kcu.column_name = 'record_hash'
  AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY');