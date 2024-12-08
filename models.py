from pydantic import BaseModel
from typing import List, Union

# Modelo para top_product_df
class TopProduct(BaseModel):
    advertiser_id: str  
    product_id: str    
    views: int         
    
# Modelo para top_ctr_df
class TopCTR(BaseModel):
    advertiser_id: str 
    product_id: str   
    clicks: float       
    impressions: float 
    ctr: float          

# Modelo genérico para devolver recomendaciones
class RecommendationResponse(BaseModel):
    advertiser_id: str
    model: str
    recommendations: Union[List[TopProduct], List[TopCTR]]
