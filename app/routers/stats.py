from fastapi import APIRouter
from app.database import get_db_connection

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
    cursor = conn.cursor()

    # Cantidad de advertisers
    cursor.execute("SELECT COUNT(DISTINCT advertiser_id) AS total_advertisers FROM top_product_df")
    advertiser_count = cursor.fetchone()["total_advertisers"]

    # Advertisers que más varían sus recomendaciones por día
    cursor.execute("""
        SELECT advertiser_id, COUNT(DISTINCT recommendation_date) AS variation_count
        FROM top_product_df
        GROUP BY advertiser_id
        ORDER BY variation_count DESC
        LIMIT 1
    """)
    top_advertiser = cursor.fetchone()

    # Estadísticas de coincidencia entre ambos modelos
    cursor.execute("""
        SELECT COUNT(*) AS common_recommendations
        FROM top_product_df tp
        JOIN top_ctr_df tc
        ON tp.advertiser_id = tc.advertiser_id AND tp.product_id = tc.product_id
    """)
    model_agreement = cursor.fetchone()["common_recommendations"]

    conn.close()

    # Formatear resultados en una estructura JSON
    return {
        "advertiser_count": advertiser_count,
        "top_advertiser": {
            "advertiser_id": top_advertiser["advertiser_id"],
            "variation_count": top_advertiser["variation_count"]
        },
        "model_agreement": model_agreement
    }
