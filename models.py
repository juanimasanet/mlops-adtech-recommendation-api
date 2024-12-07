from pydantic import BaseModel
from typing import List, Optional

# Modelo para la tabla `top_products_df`
class TopProduct(BaseModel): #FALTA MODIFICAR CON RESULTADO DAG
    product_id: int
    product_name: str
    advertiser: str
    score: float
    model: str  

# Modelo para la tabla `top_ctr_df`
class TopCTR(BaseModel):
    advertiser: str
    product_id: int
    clicks: int
    impressions: int
    ctr: float  # Click-through rate calculado
