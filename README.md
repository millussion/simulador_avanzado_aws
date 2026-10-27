# Mercado Nube Platform
## Plataforma de Datos — Documentación Técnica y Guía de Entrega

## 1. Contexto de Negocio y Alcance

Mercado Nube es una plataforma de datos diseñada para consolidar y procesar información analítica de operaciones de comercio electrónico en Colombia, México y Perú.

Su objetivo es mejorar los cierres contables mensuales, garantizar la trazabilidad de las devoluciones y proporcionar un entorno reproducible tanto localmente como en AWS.

### Desafíos de negocio

- **Discrepancias contables:** diferencias entre los eventos de venta y los registros de facturación.
- **Trazabilidad de devoluciones:** seguimiento del ciclo de vida de los pedidos desde su creación hasta su devolución.
- **Flexibilidad operativa:** ejecución local y escalabilidad en la nube sin modificar la lógica principal del procesamiento.

### Objetivos técnicos

- Implementar una arquitectura de datos basada en el patrón Medallion.
- Automatizar los procesos mediante Apache Airflow.
- Aplicar reglas de calidad, deduplicación y cuarentena.
- Construir un modelo dimensional para consultas analíticas.
- Facilitar la migración desde PostgreSQL hacia Amazon Redshift.
- Controlar los costos de infraestructura mediante Terraform y procedimientos de limpieza.

---

## 2. Arquitectura Técnica

La plataforma utiliza una arquitectura híbrida basada en Apache Airflow, Docker, almacenamiento de objetos y un Data Warehouse relacional.

### Fuentes de datos

1. **Retail histórico (Kaggle):** archivos CSV procesados en lotes para construir los modelos iniciales.
2. **Stream diario (JSONL):** eventos operativos semiestructurados procesados mediante micro-lotes.
3. **Mock Catalog API:** servicio REST para enriquecer los metadatos del catálogo de productos e inventarios.

### Flujo de datos

```text
                    FUENTES DE DATOS
                           |
          +----------------+----------------+
          |                |                |
       Kaggle            JSONL          Catalog API
          |                |                |
          +----------------+----------------+
                           |
                    APACHE AIRFLOW
                           |
                     DATA LAKE (S3)
                           |
                  +------------------+
                  | Bronze (Raw)     |
                  +------------------+
                           |
                  +------------------+
                  | Silver (Cleaned) |
                  +------------------+
                           |
                  +------------------+
                  | Gold (Curated)   |
                  +------------------+
                           |
                 +---------+----------+
                 |                    |
             Quarantine           Data Warehouse
                                  /            \
                             PostgreSQL      Redshift
                              (Local)          (AWS)
```

### Capas del patrón Medallion

| Capa | Descripción |
|---|---|
| Bronze (Raw) | Almacena los datos originales sin modificar. |
| Silver (Cleaned) | Contiene datos normalizados, tipados, validados y deduplicados. |
| Gold (Curated) | Contiene datos transformados y modelos preparados para el análisis. |
| Quarantine | Almacena los registros rechazados junto con sus causas. |
| Data Warehouse | Mantiene las tablas de hechos y dimensiones para consultas analíticas. |

---

## 3. Modelo Dimensional

El modelo analítico está compuesto por las siguientes tablas principales.

### `fact_order_lines`

- **Granularidad:** una fila por artículo dentro de un pedido.
- Permite analizar cantidades, precios, descuentos y montos netos.
- Facilita la conciliación financiera y el análisis de ventas por producto y país.

### `fact_order_events`

- **Granularidad:** una fila por evento del ciclo de vida de un pedido.
- Registra estados como `CREATED`, `PAID`, `SHIPPED`, `DELIVERED`, `CANCELLED` y `RETURNED`.
- Permite auditar los cambios de estado y calcular tiempos de transición.

### `dim_product`

- Mantiene el catálogo unificado de productos.
- Utiliza una estrategia SCD Tipo 1 para conservar los atributos actuales del producto.

### Reglas de calidad e idempotencia

- Los registros con identificadores obligatorios faltantes o formatos inválidos se envían a cuarentena.
- Las devoluciones y los montos negativos se validan según las reglas de negocio para no rechazar operaciones legítimas.
- Se genera una clave determinista SHA-256 (`record_hash`) a partir de los campos identificadores del evento.
- Las reejecuciones deben evitar duplicados mediante claves únicas y operaciones de inserción idempotentes.
- Los eventos que llegan con más de 24 horas de retraso se identifican mediante `is_late_arrival = TRUE`.
- Los eventos tardíos se incorporan a sus periodos históricos correspondientes, manteniendo la fecha real del evento.

---

## 4. Requisitos Previos

Antes de ejecutar el proyecto, verifica que tengas instalados:

- Docker.
- Docker Compose.
- Git.
- Terraform, si vas a desplegar infraestructura en AWS.
- Credenciales AWS configuradas, si vas a utilizar servicios de nube.

Para comprobar las instalaciones:

```bash
docker --version
docker compose version
git --version
terraform --version
```

Terraform solo es necesario para la parte de infraestructura como código.

---

## 5. Instalación y Ejecución Local

### 5.1. Clonar el repositorio

```bash
git clone https://github.com/mercadonube/data-platform.git
cd data-platform
```

### 5.2. Iniciar los servicios

```bash
docker compose up -d --build
```

Verifica que los contenedores estén activos:

```bash
docker compose ps
```

### 5.3. Sincronizar el catálogo de productos

Ejecuta el siguiente comando si el servicio y el script están disponibles con estos nombres en tu configuración:

```bash
docker exec -it mercadonube-airflow-worker python scripts/sync_catalog.py \
  --api-url http://mock-api:8080/v1/products \
  --output-bucket mn-gold-local
```

### 5.4. Ejecutar los DAGs en Apache Airflow

Accede a la interfaz web:

`http://localhost:8080`

Las credenciales indicadas para el entorno de desarrollo son:

- Usuario: `admin`
- Contraseña: `admin`

En la interfaz de Airflow, activa y ejecuta los siguientes DAGs:

1. `dag_01_ingest_historical_kaggle`: carga inicial de datos históricos.
2. `dag_02_process_daily_stream`: ingesta incremental y validación de calidad.

Estas credenciales son únicamente valores de ejemplo para desarrollo. No deben utilizarse en producción.

### 5.5. Ejecutar pruebas automatizadas

```bash
docker exec -it mercadonube-airflow-worker pytest tests/ -v
```

Las pruebas deben validar las transformaciones, reglas de calidad, deduplicación e idempotencia.

**Nota:** los nombres de los contenedores, scripts, DAGs y buckets deben coincidir con los definidos en el repositorio y en `docker-compose.yml`.

---

## 6. Consultas Analíticas de Negocio

Las siguientes consultas ilustran los análisis principales de Mercado Nube. Deben ajustarse si los nombres o tipos de columnas del esquema real son diferentes.

### 6.1. Ventas netas por país

Calcula los pedidos y los ingresos brutos y netos por país, excluyendo pedidos cancelados y devueltos.

```sql
SELECT
    l.country_code,
    COUNT(DISTINCT l.order_id) AS total_orders,
    SUM(l.quantity * l.unit_price) AS gross_revenue,
    SUM(l.quantity * l.unit_price - l.discount_amount) AS net_revenue
FROM fact_order_lines AS l
WHERE l.current_status NOT IN ('CANCELLED', 'RETURNED')
GROUP BY l.country_code
ORDER BY net_revenue DESC;
```

### 6.2. Tasa de devoluciones por categoría

Identifica las categorías con mayor proporción de pedidos devueltos.

```sql
WITH order_returns AS (
    SELECT
        order_id,
        BOOL_OR(event_type = 'RETURNED') AS was_returned
    FROM fact_order_events
    GROUP BY order_id
)
SELECT
    p.category,
    COUNT(DISTINCT l.order_id) AS total_orders,
    COUNT(DISTINCT l.order_id) FILTER (
        WHERE r.was_returned
    ) AS returned_orders,
    ROUND(
        100.0 * COUNT(DISTINCT l.order_id) FILTER (
            WHERE r.was_returned
        ) / NULLIF(COUNT(DISTINCT l.order_id), 0),
        2
    ) AS return_rate_percentage
FROM fact_order_lines AS l
JOIN dim_product AS p
    ON l.product_id = p.product_id
LEFT JOIN order_returns AS r
    ON l.order_id = r.order_id
GROUP BY p.category
ORDER BY return_rate_percentage DESC;
```

### 6.3. Top 5 de productos más vendidos por país

```sql
WITH ranked_products AS (
    SELECT
        l.country_code,
        p.product_id,
        p.product_name,
        SUM(l.quantity) AS units_sold,
        SUM(l.quantity * l.unit_price) AS total_sales,
        ROW_NUMBER() OVER (
            PARTITION BY l.country_code
            ORDER BY SUM(l.quantity) DESC
        ) AS product_rank
    FROM fact_order_lines AS l
    JOIN dim_product AS p
        ON l.product_id = p.product_id
    GROUP BY
        l.country_code,
        p.product_id,
        p.product_name
)
SELECT
    country_code,
    product_id,
    product_name,
    units_sold,
    total_sales
FROM ranked_products
WHERE product_rank <= 5
ORDER BY country_code, product_rank;
```

### 6.4. Tiempo promedio del ciclo de vida del pedido

Calcula el tiempo promedio entre la creación y el pago, y entre la creación y la cancelación.

```sql
WITH order_timestamps AS (
    SELECT
        order_id,
        MAX(event_timestamp) FILTER (
            WHERE event_type = 'CREATED'
        ) AS created_at,
        MAX(event_timestamp) FILTER (
            WHERE event_type = 'PAID'
        ) AS paid_at,
        MAX(event_timestamp) FILTER (
            WHERE event_type = 'CANCELLED'
        ) AS cancelled_at
    FROM fact_order_events
    GROUP BY order_id
)
SELECT
    ROUND(
        AVG(EXTRACT(EPOCH FROM (paid_at - created_at)) / 60)::NUMERIC,
        2
    ) AS avg_minutes_to_pay,
    ROUND(
        AVG(EXTRACT(EPOCH FROM (cancelled_at - created_at)) / 3600)::NUMERIC,
        2
    ) AS avg_hours_to_cancel
FROM order_timestamps;
```

### 6.5. Análisis de eventos tardíos

```sql
SELECT
    DATE_TRUNC('month', event_timestamp) AS event_month,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (
        WHERE is_late_arrival = TRUE
    ) AS late_arrivals_count,
    ROUND(
        100.0 * COUNT(*) FILTER (
            WHERE is_late_arrival = TRUE
        ) / NULLIF(COUNT(*), 0),
        2
    ) AS late_arrival_ratio
FROM fact_order_events
GROUP BY DATE_TRUNC('month', event_timestamp)
ORDER BY event_month DESC;
```

### 6.6. Auditoría de lotes y registros rechazados

```sql
SELECT
    batch_id,
    source_pipeline,
    records_ingested,
    records_rejected,
    execution_date,
    status
FROM audit_batch_log
ORDER BY execution_date DESC;
```

Las consultas requieren que las tablas y columnas correspondientes existan en el Data Warehouse.

---

## 7. Infraestructura como Código: Terraform e IAM

La infraestructura de AWS se administra declarativamente mediante Terraform.

### 7.1. Variables principales

Ejemplo de configuración en `variables.tf`:

```hcl
variable "environment" {
  type        = string
  default     = "dev"
  description = "Ambiente de despliegue: dev, qa o prod"
}

variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "Región principal de AWS"
}
```

### 7.2. Buckets del Data Lake

Ejemplo ilustrativo de creación de buckets para las capas del Data Lake:

```hcl
resource "aws_s3_bucket" "data_lake" {
  for_each = toset(["raw", "cleaned", "curated", "quarantine"])

  bucket = "mercadonube-datalake-${each.key}-${var.environment}"
}
```

Los nombres de los buckets S3 deben ser únicos globalmente.

### 7.3. Política de ciclo de vida

Ejemplo de transición de datos históricos a Glacier después de 90 días:

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.data_lake["raw"].id

  rule {
    id     = "transition_raw_to_glacier"
    status = "Enabled"

    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
```

Antes de aplicar esta política, verifica los requisitos de retención, los costos de recuperación y el periodo mínimo de almacenamiento de la clase elegida.

### 7.4. Rol de ejecución para ECS/Fargate

Ejemplo básico de política de confianza:

```hcl
resource "aws_iam_role" "ecs_execution_role" {
  name = "mercadonube-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"

    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"

      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })
}
```

Este bloque define la confianza del rol, pero no concede por sí mismo todos los permisos necesarios para ejecutar tareas. Los permisos de ejecución y de acceso a S3 deben configurarse por separado siguiendo el principio de menor privilegio.

### 7.5. Comandos de Terraform

Desde la carpeta de infraestructura:

```bash
cd terraform
terraform init
terraform validate
terraform plan
```

Revisa el plan antes de desplegar. Solo después de confirmar los recursos, costos y permisos, ejecuta:

```bash
terraform apply
```

---

## 8. Estrategia de Migración a Amazon Redshift Serverless

La plataforma puede evolucionar desde PostgreSQL local hacia Amazon Redshift Serverless.

### Procedimiento

1. Exportar los datos procesados de las capas `cleaned` y `curated` a Amazon S3.
2. Utilizar archivos Parquet para cargas analíticas de gran volumen.
3. Crear previamente las tablas de destino en Redshift.
4. Configurar un rol IAM con acceso limitado al bucket de origen.
5. Ejecutar la carga masiva mediante `COPY`.
6. Validar los conteos y resultados de las consultas.
7. Evaluar las estrategias de distribución y ordenamiento según las consultas reales.

Ejemplo de carga:

```sql
COPY analytical.fact_order_events
FROM 's3://mercado-nube-data-lake-prod/curated/events/'
IAM_ROLE 'arn:aws:iam::123456789012:role/mercado-nube-redshift-role'
FORMAT AS PARQUET;
```

El ARN y el bucket son valores de ejemplo. Deben sustituirse por los recursos reales y autorizados. El esquema y la tabla de destino deben existir previamente.

---

## 9. Estimación de Costos AWS y Plan de Limpieza

### 9.1. Estimación para un entorno de pruebas

El objetivo del escenario de laboratorio es mantener el costo de validación técnica por debajo de 5 USD, sujeto a los supuestos de uso y a las tarifas de la región seleccionada.

| Servicio AWS | Configuración o uso estimado | Costo estimado |
|---|---|---:|
| Amazon S3 | 5 GB entre las capas Raw, Silver y Gold, más solicitudes | $0.15 USD/mes |
| AWS ECS/Fargate | Una tarea de 0.25 vCPU y 0.5 GB de RAM durante 1 hora al día | $0.85 USD/mes |
| Amazon Redshift Serverless | Consultas puntuales de prueba | $3.00 USD por escenario de prueba |
| Amazon ECR | Almacenamiento de dos imágenes Docker, con menos de 2 GB | $0.20 USD/mes |
| **Total objetivo** | **Supuestos del entorno de laboratorio** | **$4.20 USD** |

**Advertencia sobre costos:** las cifras son estimaciones de planificación y no una cotización vigente. El total depende de la región, el uso efectivo, las tarifas mínimas y los recursos auxiliares. El escenario no garantiza una factura inferior a 5 USD.

Antes de desplegar, revisa:

- Amazon VPC y posibles cargos de NAT Gateway.
- Amazon CloudWatch y la ingesta de logs.
- Amazon ECR y almacenamiento de imágenes.
- Amazon Redshift Serverless y el consumo real de capacidad.
- ECS/Fargate, almacenamiento y transferencia de datos.
- Amazon MWAA, si se utiliza, porque su entorno puede generar costos propios incluso cuando no se procesan lotes.

Configura alertas presupuestarias en AWS Budgets y revisa Cost Explorer para identificar cargos inesperados.

### 9.2. Limpieza de infraestructura AWS (Teardown)

Para eliminar los recursos gestionados por Terraform, ejecuta los siguientes comandos desde la raíz del proyecto.

**Paso 1. Revisar los recursos que se destruirán**

```bash
cd terraform/
terraform plan -destroy
```

**Paso 2. Eliminar los recursos**

```bash
terraform destroy
```

Después de revisar el plan, confirma la operación cuando Terraform lo solicite.

Para un entorno temporal que ya se ha verificado, también puedes utilizar:

```bash
terraform destroy -auto-approve
```

**Precauciones:**

- La destrucción puede eliminar datos y recursos de manera irreversible.
- Comprueba que estás en el entorno correcto antes de confirmar.
- Los recursos creados manualmente pueden quedar fuera del estado de Terraform.
- Los buckets con objetos, las bases de datos, los snapshots y las políticas de protección pueden requerir pasos adicionales.
- Revisa todas las regiones de AWS utilizadas y confirma que no queden recursos facturables.

### 9.3. Limpieza del entorno local Docker

Para detener y eliminar los contenedores, las redes y los volúmenes del proyecto:

```bash
docker compose down -v --remove-orphans
```

**Importante:** `-v` elimina los volúmenes asociados y puede borrar permanentemente los datos de PostgreSQL local.

Si necesitas conservar los datos, ejecuta:

```bash
docker compose down --remove-orphans
```

Para verificar el estado posterior:

```bash
docker compose ps -a
docker volume ls
```

### 9.4. Verificación final de costos

Después de la limpieza:

1. Comprueba el estado de los recursos en la consola de AWS.
2. Revisa S3, ECS, ECR, Redshift, CloudWatch y los recursos de red.
3. Comprueba AWS Billing y Cost Explorer.
4. Verifica que no existan recursos fuera de Terraform.
5. Recuerda que pueden aparecer cargos pendientes por consumo anterior o facturación con retraso.

---

## 10. Estructura Esperada del Proyecto

La estructura conceptual de la plataforma es la siguiente:

```text
data-platform/
├── dags/
│   ├── dag_01_ingest_historical_kaggle.py
│   └── dag_02_process_daily_stream.py
├── scripts/
│   └── sync_catalog.py
├── tests/
│   ├── idempotency_check.sql
│   └── ...
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── ...
├── docker-compose.yml
├── DECISIONS.md
├── README.md
└── business_queries.sql
```

Esta estructura es una referencia documental. Los archivos y nombres definitivos deben coincidir con los existentes en el repositorio.

---

## 11. Criterios de Validación y Entrega

Antes de considerar finalizada la entrega, verifica los siguientes puntos:

- [ ] Los servicios locales se inician mediante Docker Compose.
- [ ] Airflow permite ejecutar los DAGs de ingesta y procesamiento.
- [ ] Los datos históricos y los eventos diarios se procesan correctamente.
- [ ] Las reglas de calidad envían los registros inválidos a cuarentena.
- [ ] Las reejecuciones no duplican eventos.
- [ ] Los eventos tardíos se identifican y procesan correctamente.
- [ ] Las tablas de hechos y dimensiones permiten ejecutar las consultas analíticas.
- [ ] Las pruebas automatizadas finalizan correctamente.
- [ ] La infraestructura de Terraform pasa las validaciones.
- [ ] Los costos de AWS se documentan con sus supuestos.
- [ ] El procedimiento de limpieza se encuentra documentado.
- [ ] No se incluyen credenciales, contraseñas reales ni claves de AWS en el repositorio.

---

## 12. Decisiones Técnicas

Las decisiones arquitectónicas detalladas, las reglas de granularidad, el tratamiento de eventos tardíos, la cuarentena y la estrategia de migración se documentan adicionalmente en `DECISIONS.md`.

La documentación de decisiones y las consultas analíticas deben mantenerse sincronizadas con la implementación real del proyecto.

---

## 13. Licencia y Uso

Proyecto de ingeniería de datos orientado al procesamiento analítico, la conciliación financiera y la trazabilidad de operaciones de comercio electrónico.

La licencia, los autores y las condiciones de distribución deben declararse según las condiciones reales del repositorio.

