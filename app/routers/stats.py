from fastapi import APIRouter, HTTPException
from app.database import get_db_connection
import psycopg2.extras  # Para usar DictCursor

router = APIRouter()

@router.get("/stats/")
def get_stats():
    """
    Devuelve estadísticas generales sobre las recomendaciones:
    - Cantidad de advertisers
    - Advertisers que más varían sus recomendaciones por día
    - Estadísticas de coincidencia entre ambos modelos
    """
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)  # Usar DictCursor para obtener resultados como diccionarios

    try:
        # Cantidad de advertisers
        cursor.execute("SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers FROM top_products_df")
        advertiser_result = cursor.fetchone()
        if advertiser_result is None:
            raise HTTPException(status_code=404, detail="No data found for advertiser count")
        advertiser_count = advertiser_result["total_advertisers"]

        # Advertisers que más varían sus recomendaciones por día
        cursor.execute("""
            SELECT advertiser_id, COUNT(DISTINCT date) AS variation_count
            FROM top_products_df
            GROUP BY advertiser_id
            ORDER BY variation_count DESC
            LIMIT 1
        """)
        top_advertiser_result = cursor.fetchone()
        if top_advertiser_result is None:
            raise HTTPException(status_code=404, detail="No data found for top advertiser")
        top_advertiser = {
            "advertiser_id": top_advertiser_result["advertiser_id"],
            "variation_count": top_advertiser_result["variation_count"]
        }

        # Estadísticas de coincidencia entre ambos modelos
        cursor.execute("""
            SELECT COUNT(*) AS common_recommendations
            FROM top_products_df tp
            JOIN top_ctr_df tc
            ON tp.advertiser_id = tc.advertiser_id 
               AND tp.product_id = tc.product_id
               AND tp.date = tc.date
        """)
        model_agreement_result = cursor.fetchone()
        if model_agreement_result is None:
            raise HTTPException(status_code=404, detail="No data found for model agreement")
        model_agreement = model_agreement_result["common_recommendations"]

    finally:
        conn.close()

    # Formatear resultados en una estructura JSON
    return {
        "advertiser_count": advertiser_count,
        "top_advertiser": top_advertiser,
        "model_agreement": model_agreement
    }
