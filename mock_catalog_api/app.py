from fastapi import FastAPI, HTTPException, Query
from datetime import datetime

app = FastAPI(title="Catalog Mock Service")
CATEGORIES = ["Electronics", "Home", "Fashion", "Beauty", "Sports"]
TOTAL_PRODUCTS = 300
CALL_COUNTER = 0

@app.get("/api/v1/products")
def get_products(page: int = Query(1, ge=1), page_size: int = Query(50, le=100)):
    global CALL_COUNTER
    CALL_COUNTER += 1
    if CALL_COUNTER % 20 == 0:
        raise HTTPException(status_code=503, detail="Simulated Temporary Error")
    
    start = (page - 1) * page_size
    end = start + page_size
    items = [
        {
            "sku": f"SKU-{1000 + i}",
            "name": f"Product {1000 + i}",
            "category": CATEGORIES[i % len(CATEGORIES)],
            "is_active": True,
            "updated_at": datetime.utcnow().isoformat()
        }
        for i in range(start, min(end, TOTAL_PRODUCTS))
    ]
    return {"items": items, "page": page, "total_items": TOTAL_PRODUCTS}