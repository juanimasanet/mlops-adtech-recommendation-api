from fastapi import APIRouter, HTTPException
from app.database import get_db_connection
from psycopg2.extras import DictCursor

router = APIRouter()

@router.get("/history/{advertiser_id}")
def get_history(advertiser_id: str):
    """
    Devuelve el historial de productos más vistos para un advertiser específico.
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=DictCursor)  # Usar DictCursor para acceder a los resultados como diccionarios

    # Consulta SQL
    query = """
        SELECT date, product_id, views
        FROM top_products_df
        WHERE advertiser_id = %s
        ORDER BY date DESC;
    """
    cursor.execute(query, (advertiser_id,))
    results = cursor.fetchall()

    # Si no hay resultados, devolver un error 404
    if not results:
        conn.close()
        raise HTTPException(status_code=404, detail="No se encontró historial para este advertiser.")

    # Formatear los resultados
    history = []
    for row in results:
        try:
            history.append({
                "date": row["date"].strftime("%Y-%m-%d"),  # Asegurarse de que sea un objeto datetime
                "product_id": row["product_id"],
                "views": row["views"]
            })
        except AttributeError:
            conn.close()
            raise HTTPException(status_code=500, detail="Error al procesar los datos del historial.")

    conn.close()
    return {"advertiser_id": advertiser_id, "history": history}
