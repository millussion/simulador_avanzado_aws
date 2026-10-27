# Documento de Decisiones Técnicas — Mercado Nube Platform

Este documento detalla las justificaciones arquitectónicas, las alternativas evaluadas y las decisiones de diseño para el procesamiento y modelado analítico de Mercado Nube.

---

## 1. Justificación de Cómputo: Contenedores (Fargate / Docker Local) vs. AWS Glue vs. AWS Lambda

- **Decisión adoptada:** Orquestación basada en Apache Airflow ejecutando transformaciones modulares en tareas desacopladas, emulado localmente con Docker y transferible a AWS ECS/Fargate o Amazon MWAA.

- **Evaluación de AWS Glue:**
  - Descarte para el MVP: Glue Spark puede introducir latencia de inicio y costos de ejecución que no se justifican para lotes diarios de 50.000 a 100.000 eventos cuando Python nativo optimizado es suficiente.
  - La elección se basa en el volumen esperado, el tiempo de ejecución y el costo operativo. Los tiempos reales deben validarse mediante pruebas.

- **Evaluación de AWS Lambda:**
  - Descarte para la ingesta histórica de gran volumen: Lambda tiene un límite máximo de ejecución de 15 minutos por invocación.
  - Para más de 1.000.000 de registros históricos, se prefiere un procesamiento por lotes en contenedores, con particionamiento y checkpoints cuando corresponda.

- **Conclusión:** La ejecución en contenedores controlados ofrece portabilidad local para desarrollo y pruebas, y permite migrar a ECS/Fargate o Amazon MWAA según los requisitos operativos y de costos.

---

## 2. Granularidad y Estrategia del Modelo Dimensional

### `analytical.fact_order_lines`

- **Granularidad:** Una fila por artículo (SKU) vendido dentro de una factura (`invoice_no`).
- **Tratamiento de devoluciones:** Se preservan los montos negativos y se utiliza la bandera booleana `is_return = TRUE` cuando corresponda.
- Las métricas deben distinguir ventas brutas, devoluciones y ventas netas para evitar dobles conteos.

### `analytical.fact_order_events`

- **Granularidad:** Una fila por evento de cambio de estado, por ejemplo `created`, `paid`, `cancelled` o `refunded`.
- **Deduplicación:** Se utiliza una clave determinista SHA-256 (`record_hash`), calculada sobre los identificadores estables del evento.
- Las inserciones pueden utilizar `ON CONFLICT (record_hash) DO NOTHING`, siempre que exista una restricción UNIQUE sobre `record_hash`.

### `analytical.dim_product`

- **Estrategia:** Slowly Changing Dimension Type 1 (SCD1).
- Se utiliza UPSERT sobre la clave primaria `sku`.
- Se actualizan los atributos actuales, como precio y categoría, sin conservar versiones históricas de esos atributos.
- El histórico transaccional se conserva en las tablas de hechos.

---

## 3. Manejo de Calidad, Cuarentena y Datos Faltantes

### Segregación a cuarentena

Tabla propuesta: `analytical.quarantine_records`.

- Las filas con `price <= 0` o `amount <= 0` se desvían a cuarentena cuando incumplen las reglas de negocio.
- Los importes negativos válidos de devoluciones y cancelaciones deben tratarse según reglas explícitas, evitando rechazar operaciones legítimas.
- No se descartan registros silenciosamente.
- Se conserva el nombre de la fuente (`source_name`), la fecha de proceso, el motivo del rechazo y el payload original en formato JSONB.
- Motivos sugeridos:
  - `NON_POSITIVE_AMOUNT`
  - `MALFORMED_TIMESTAMP`
  - `INVALID_PRODUCT_ID`
  - `MISSING_REQUIRED_FIELD`

Los registros en cuarentena deben poder corregirse y reprocesarse posteriormente.

### Clientes faltantes

- Cuando `Customer ID` sea nulo, se preserva la venta para no alterar la conciliación financiera.
- Se puede utilizar `customer_id = NULL` o una clave sustituta como `GUEST_USER`, de acuerdo con el diseño del modelo.
- Si se utiliza `GUEST_USER`, debe existir una estrategia consistente para representar ese miembro desconocido en las dimensiones y evitar confundirlo con un cliente real.

---

## 4. Detección y Reprocesamiento de Eventos Tardíos (Late Arrivals)

### Regla de detección

Un evento se considera tardío cuando la fecha del lote es posterior a la fecha real del evento:

`(batch_date - event_time::date) >= 1 día`

La implementación debe calcular esta diferencia usando tipos de fecha compatibles con PostgreSQL.

### Estrategia de conciliación

- El pipeline diario ingesta los eventos tardíos sin detener todo el procesamiento.
- Se conserva `event_time` como fecha real de ocurrencia y `batch_date` como fecha de ingesta.
- Los cierres de mes y las conciliaciones recalculan los resultados usando `event_time`.
- Las ejecuciones deben ser idempotentes para evitar duplicados durante el reprocesamiento.

---

## 5. Estrategia de Migración a Amazon Redshift Serverless

Para migrar la base analítica local a AWS Redshift:

1. Exportar las capas `cleaned/` y `curated/` almacenadas en S3 a formato columnar Parquet.
2. Verificar que los archivos, tipos de datos, particiones y permisos sean compatibles con el esquema de destino.
3. Crear las tablas de destino en Redshift antes de ejecutar la carga.
4. Ejecutar la carga masiva con el comando nativo `COPY`.

Ejemplo ilustrativo:

```sql
COPY analytical.fact_order_events
FROM 's3://mercado-nube-data-lake-prod/curated/events/'
IAM_ROLE 'arn:aws:iam::123456789012:role/mercado-nube-redshift-role'
FORMAT AS PARQUET;
```

**Importante:** El ARN del rol es un ejemplo y debe sustituirse por el ARN real autorizado. La base de datos, el esquema, la tabla y los permisos deben existir y estar configurados previamente.

5. Evaluar las estrategias de distribución y ordenamiento según los patrones de consulta.

En Redshift pueden evaluarse `DISTKEY(order_id)` y `SORTKEY(event_time)`, pero la selección debe validarse según el volumen de datos, las uniones y las consultas más frecuentes. No deben copiarse automáticamente las estrategias de PostgreSQL.

---

## 6. Criterios de Idempotencia y Recuperación

La idempotencia significa que volver a ejecutar un mismo lote no debe generar registros duplicados ni alterar incorrectamente los resultados.

Criterios de validación:

- Restricciones de unicidad sobre las claves de negocio o identificadores de eventos.
- Operaciones de inserción idempotentes.
- Registro del estado de ejecución de cada lote.
- Identificación de errores y registros enviados a cuarentena.
- Capacidad de reejecutar un lote fallido sin duplicar datos.
- Validación de los conteos antes y después del reprocesamiento.

La consulta de `tests/idempotency_check.sql` proporciona una comprobación inicial de unicidad de los hashes de eventos. Para demostrar la idempotencia completa, también debe compararse el conteo antes y después de ejecutar dos veces el mismo lote.
