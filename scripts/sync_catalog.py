import os
import time
import requests
import psycopg2
import psycopg2.extras

CATALOG_API_URL = os.getenv("CATALOG_API_URL", "http://catalog-api:8000")
DB_URL = os.getenv("ANALYTICS_DB_URL", "postgresql://analytics_user:analytics_pass@postgres-analytics:5432/analytics_db")

def fetch_page_with_retry(page: int, max_retries=5, backoff_factor=1.5):
    endpoint = f"{CATALOG_API_URL}/api/v1/products?page={page}"
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(endpoint, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500, 502, 503, 504]:
                wait_time = backoff_factor ** attempt
                print(f"[Pagina {page} | Intento {attempt}] HTTP {response.status_code}. Reintentando en {wait_time:.1f}s...")
                time.sleep(wait_time)
            else:
                response.raise_for_status()
        except requests.exceptions.RequestException as e:
            wait_time = backoff_factor ** attempt
            print(f"[Pagina {page} | Intento {attempt}] Error ({e}). Reintentando en {wait_time:.1f}s...")
            time.sleep(wait_time)
            
    raise RuntimeError(f"Fallo al consultar la pagina {page} tras {max_retries} intentos.")

def sync_catalog():
    print(f"Iniciando sincronizacion del catalogo desde {CATALOG_API_URL}/api/v1/products...")
    all_products = []
    current_page = 1
    
    while True:
        data = fetch_page_with_retry(current_page)
        items = data.get("items", [])
        total_items = data.get("total_items", len(items))
        
        if not items:
            break
            
        all_products.extend(items)
        print(f"Pagina {current_page} procesada: +{len(items)} items (Acumulado: {len(all_products)}/{total_items})")
        
        if len(all_products) >= total_items:
            break
            
        current_page += 1

    print(f"Total productos extraidos de la API: {len(all_products)}")
    if not all_products:
        print("Catalogo sin registros.")
        return

    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    try:
        upsert_sql = """
            INSERT INTO analytical.dim_product (sku, product_name, category, current_price, updated_at)
            VALUES %s
            ON CONFLICT (sku) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                current_price = EXCLUDED.current_price,
                updated_at = CURRENT_TIMESTAMP;
        """

        values = [
            (
                str(p.get("sku") or p.get("id")),
                str(p.get("title") or p.get("product_name") or p.get("name") or "Sin descripcion"),
                str(p.get("category") or "General"),
                float(p.get("current_price") or p.get("price") or 0.0),
                "NOW()"
            )
            for p in all_products
        ]

        psycopg2.extras.execute_values(cur, upsert_sql, values, page_size=1000)
        conn.commit()
        print(f"Sincronizacion completada con exito: {len(values)} registros upserted en analytical.dim_product.")

    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    sync_catalog()
