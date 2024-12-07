from fastapi import FastAPI
from app.routers import recommendations, stats, history

app = FastAPI()

app.include_router(recommendations.router)
app.include_router(stats.router)
app.include_router(history.router)

@app.get("/")
def root():
    return {"message": "AdTech Recommendation API"}
