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
        SELECT date, product_id, views
        FROM top_products_df
        WHERE advertiser_id = %s AND date >= CURRENT_DATE - INTERVAL '7 days'
    """
    cursor.execute(query, (adv,))
    results = cursor.fetchall()
    conn.close()

    if not results:
        raise HTTPException(status_code=404, detail="No history found for the given advertiser.")

    # Formatear resultados en una estructura JSON
    history = [
        {
            "date": row[0].strftime("%Y-%m-%d"),  
            "product_id": row[1],
            "views": row[2]
        } for row in results
    ]

    return {"advertiser": adv, "history": history}
