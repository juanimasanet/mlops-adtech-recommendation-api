from fastapi import APIRouter, HTTPException
from app.database import get_db_connection

router = APIRouter()

@router.get("/history/{adv}")
def get_history(adv: str):
    """
    Devuelve el historial de recomendaciones de los últimos 7 días para un advertiser.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Consulta para obtener las recomendaciones de los últimos 7 días
    query = """
        SELECT recommendation_date, product_id, views
        FROM top_product_df
        WHERE advertiser_id = %s AND recommendation_date >= NOW() - INTERVAL '7 days'
    """
    cursor.execute(query, (adv,))
    results = cursor.fetchall()
    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail="No history found for the given advertiser.")

    # Formatear resultados en una estructura JSON
    history = [
        {
            "recommendation_date": row["recommendation_date"].strftime("%Y-%m-%d"),
            "product_id": row["product_id"],
            "views": row["views"]
        } for row in results
    ]

    return {"advertiser": adv, "history": history}
