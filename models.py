from pydantic import BaseModel
from typing import List

class Recommendation(BaseModel):
    product_id: int
    product_name: str
    score: float

class Stats(BaseModel):
    advertiser_count: int
    top_advertisers: List[str]
    model_agreement: float
