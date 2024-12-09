from fastapi import APIRouter, HTTPException
from app.database import get_db_connection
from app.models import TopProduct, TopCTR, RecommendationResponse

router = APIRouter()

@router.get("/recommendations/{adv}/{model}", response_model=RecommendationResponse)
def get_recommendations(adv: str, model: str):
    """
    Devuelve las recomendaciones para un advertiser y un modelo específico.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    if model == "top_product":
        # Query para top_product_df
        query = """
            SELECT advertiser_id, product_id, views
            FROM top_products_df
            WHERE advertiser_id = %s AND date = CURRENT_DATE
        """
        cursor.execute(query, (adv,))
        results = cursor.fetchall()

        # Formatear resultados
        recommendations = [
            TopProduct(
                advertiser_id=row["advertiser_id"],
                product_id=row["product_id"],
                views=row["views"]
            )
            for row in results
        ]

    elif model == "top_ctr":
        # Query para top_ctr_df
        query = """
            SELECT advertiser_id, product_id, clicks, impressions, ctr
            FROM top_ctr_df
            WHERE advertiser_id = %s AND date = CURRENT_DATE
        """
        cursor.execute(query, (adv,))
        results = cursor.fetchall()

        # Formatear resultados
        recommendations = [
            TopCTR(
                advertiser_id=row["advertiser_id"],
                product_id=row["product_id"],
                clicks=row["clicks"],
                impressions=row["impressions"],
                ctr=row["ctr"]
            )
            for row in results
        ]

    else:
        conn.close()
        raise HTTPException(status_code=400, detail="Modelo no válido. Use 'top_product' o 'top_ctr'.")

    conn.close()

    # Si no hay resultados
    if not recommendations:
        raise HTTPException(status_code=404, detail="No se encontraron recomendaciones para este advertiser.")

    return RecommendationResponse(
        advertiser=adv,
        model=model,
        recommendations=recommendations
    )
