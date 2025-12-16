from flask import Flask, jsonify, request
import pandas as pd
from db_connector import get_connection

app = Flask(__name__)

# --------------------------------------
# GLOBAL CACHE
# --------------------------------------
products_df = None


# --------------------------------------
# LOAD DATA ON STARTUP
# --------------------------------------
def load_data():
    global products_df
    engine = get_connection()

    query = """
    SELECT
        pd.id AS product_id,
        pd.product_name,
        pd.brand_id,
        pd.category_id,
        pd.mst_category_id,

        b.name AS brand_name,
        b.image AS brand_image,

        bsc.id AS sub_category_id,
        bsc.category_name AS sub_category_name,

        bc.name AS category_name,
        bc.image AS category_image,

        pp.price,
        pp.mrp,
        pp.discount

    FROM UAT.product_detail pd
    LEFT JOIN UAT.brand b ON pd.brand_id = b.id
    LEFT JOIN UAT.brand_sub_categories bsc ON pd.category_id = bsc.id
    LEFT JOIN UAT.brand_categories bc ON pd.mst_category_id = bc.id
    LEFT JOIN UAT.product_pricing pp ON pd.id = pp.product_id
    """

    products_df = pd.read_sql(query, engine)
    print("Products loaded:", len(products_df))


load_data()

# --------------------------------------
# SIMILARITY LOGIC
# --------------------------------------
def calculate_similarity(p1, p2):
    score = 0

    if p1["mst_category_id"] == p2["mst_category_id"]:
        score += 2

    if p1["sub_category_id"] == p2["sub_category_id"]:
        score += 2

    if p1["brand_id"] == p2["brand_id"]:
        score += 3

    if p1["price"] and p2["price"]:
        if abs(p1["price"] - p2["price"]) / p1["price"] <= 0.10:
            score += 1

    return score


# --------------------------------------
# CORE RECOMMENDATION FUNCTION
# --------------------------------------
def get_recommendations(product_id, similar_limit=5, other_limit=10):
    base_df = products_df[products_df["product_id"] == product_id]

    if base_df.empty:
        return None

    base_product = base_df.iloc[0]
    similar = []
    others = []

    for _, row in products_df.iterrows():
        if row["product_id"] == product_id:
            continue

        score = calculate_similarity(base_product, row)

        product_card = {
            "product_id": int(row["product_id"]),
            "product_name": row["product_name"],
            "brand_name": row["brand_name"],
            "category_name": row["category_name"],
            "sub_category_name": row["sub_category_name"],
            "price": float(row["price"] or 0),
            "mrp": float(row["mrp"] or 0),
            "discount": float(row["discount"] or 0),
            "similarity_score": score
        }

        if score > 0:
            similar.append(product_card)
        else:
            others.append(product_card)

    # Sort similar products by score
    similar.sort(key=lambda x: x["similarity_score"], reverse=True)

    return {
        "base_product": {
            "product_id": int(base_product["product_id"]),
            "product_name": base_product["product_name"],
            "brand_name": base_product["brand_name"],
            "category_name": base_product["category_name"],
            "sub_category_name": base_product["sub_category_name"],
            "price": float(base_product["price"] or 0),
        },
        "similar_products": similar[:similar_limit],
        "other_products": others[:other_limit]
    }


# --------------------------------------
# API ENDPOINT
# --------------------------------------
@app.route("/recommendations/<int:product_id>")
def recommendations(product_id):
    similar_limit = int(request.args.get("similar", 5))
    other_limit = int(request.args.get("other", 10))

    data = get_recommendations(product_id, similar_limit, other_limit)

    if not data:
        return jsonify({"error": "Product not found"}), 404

    return jsonify(data)


# --------------------------------------
# HEALTH CHECK
# --------------------------------------
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "rows_loaded": len(products_df)
    })


# --------------------------------------
# RUN APP
# --------------------------------------
if __name__ == "__main__":
    app.run(debug=True)


# http://127.0.0.1:5000/recommendations/101