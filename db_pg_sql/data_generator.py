import random
from typing import Dict, List, Tuple, Optional


BASE_CATEGORIES = [
    {"id": "c_electronics", "name": "Electronics", "parent": None},
    {"id": "c_sports", "name": "Sports", "parent": None},
    {"id": "c_fashion", "name": "Fashion", "parent": None},
    {"id": "c_home_kitchen", "name": "Home & Kitchen", "parent": "c_electronics"},
    {"id": "c_gaming", "name": "Gaming", "parent": "c_electronics"},
    {"id": "c_books", "name": "Books", "parent": None},
    {"id": "c_beauty", "name": "Beauty", "parent": None},
    {"id": "c_toys", "name": "Toys", "parent": None},
]

BASE_BRANDS = [
    {"id": "b_nike", "name": "Nike"},
    {"id": "b_adidas", "name": "Adidas"},
    {"id": "b_sony", "name": "Sony"},
    {"id": "b_logitech", "name": "Logitech"},
    {"id": "b_samsung", "name": "Samsung"},
]

BASE_TAGS = [
    {"id": "t_gaming", "name": "gaming"},
    {"id": "t_wireless", "name": "wireless"},
    {"id": "t_fitness", "name": "fitness"},
    {"id": "t_kids", "name": "kids"},
    {"id": "t_reading", "name": "reading"},
    {"id": "t_home", "name": "home"},
    {"id": "t_beauty", "name": "beauty"},
]

BASE_PRODUCTS = [
    {"id": "p1", "name": "Wireless Mouse", "brandId": "b_logitech", "categoryId": "c_electronics", "price": 29.99, "tags": ["t_wireless"]},
    {"id": "p2", "name": "Gaming Keyboard", "brandId": "b_logitech", "categoryId": "c_gaming", "price": 59.99, "tags": ["t_gaming"]},
    {"id": "p3", "name": "4K TV", "brandId": "b_sony", "categoryId": "c_electronics", "price": 799.99, "tags": ["t_home"]},
    {"id": "p4", "name": "Running Shoes", "brandId": "b_nike", "categoryId": "c_sports", "price": 79.99, "tags": ["t_fitness"]},
    {"id": "p5", "name": "Football", "brandId": "b_adidas", "categoryId": "c_sports", "price": 25.99, "tags": ["t_fitness"]},
    {"id": "p6", "name": "Perfume Set", "brandId": "b_adidas", "categoryId": "c_beauty", "price": 45.00, "tags": ["t_beauty"]},
    {"id": "p7", "name": "Novel Book", "brandId": "b_samsung", "categoryId": "c_books", "price": 14.99, "tags": ["t_reading"]},
    {"id": "p8", "name": "Toy Car", "brandId": "b_samsung", "categoryId": "c_toys", "price": 9.99, "tags": ["t_kids"]},
    {"id": "p9", "name": "AirMax Shoes", "brandId": "b_nike", "categoryId": "c_fashion", "price": 120.00, "tags": ["t_fitness"]},
    {"id": "p10", "name": "Sports Watch", "brandId": "b_samsung", "categoryId": "c_electronics", "price": 199.99, "tags": ["t_fitness"]},
]


# Role:
#   Generate a stable partition key for one dataset size, so multiple runs with the same N
#   can reuse the same partition and be cleaned safely.
# Inputs:
#   - total_products (int): desired number of products
# Output:
#   - str: partition key string (e.g., "bench_N1000")
def make_run_pk(total_products: int) -> str:
    return f"bench_N{max(10, int(total_products))}"


# Role:
#   Generate a SQL dataset that matches the Gremlin graph generation logic:
#     - base master data (categories/brands/tags/base products)
#     - bulk products to reach N
#     - users proportional to N
#     - interactions: VIEWED, BOUGHT (probability), LIKED (probability)
#     - advanced relations: SIMILAR_TO, BOUGHT_TOGETHER, SIMILAR_USER
# Inputs:
#   - N_products (int): total number of products to generate (min 10)
#   - run_pk (Optional[str]): partition key; generated automatically if not provided
#   - seed (int): RNG seed (reproducible dataset)
#   - keep_gremlin_bug_interactions_per_user (bool):
#       * True  -> mimic the Gremlin script behavior where interactions_per_user ends up always = 5
#       * False -> use a more scalable intended rule for interactions_per_user
# Output:
#   - Dict[str, list]: dataset tables as lists of tuples ready to insert in SQL, plus "run_pk"
def generate_data_gremlin_like(
    N_products: int,
    run_pk: Optional[str] = None,
    seed: int = 42,
    keep_gremlin_bug_interactions_per_user: bool = True,
) -> Dict[str, list]:

    N_products = max(10, int(N_products))
    run_pk = run_pk or make_run_pk(N_products)
    rng = random.Random(seed)

    # Each "table" is represented as a list of tuples, including run_pk as the first column
    brands = [(run_pk, b["id"], b["name"]) for b in BASE_BRANDS]
    categories = [(run_pk, c["id"], c["name"], c["parent"]) for c in BASE_CATEGORIES]
    tags = [(run_pk, t["id"], t["name"]) for t in BASE_TAGS]

    num_users = max(10, N_products // 5)
    user_ids = [f"u{i}" for i in range(1, num_users + 1)]
    users = [(run_pk, uid, f"User {uid[1:]}") for uid in user_ids]

    # Product schema tuple:
    #   (run_pk, product_id, name, price, brand_id, category_id)
    products: List[Tuple[str, str, str, float, str, str]] = []

    # Product-tag relation tuple:
    #   (run_pk, product_id, tag_id)
    product_tags: List[Tuple[str, str, str]] = []

    brand_ids = [b["id"] for b in BASE_BRANDS]
    cat_ids = [c["id"] for c in BASE_CATEGORIES]
    tag_ids = [t["id"] for t in BASE_TAGS]

    # Add the 10 base products and their tags (same ids and fields as the Gremlin version)
    for p in BASE_PRODUCTS:
        products.append((run_pk, p["id"], p["name"], float(p["price"]), p["brandId"], p["categoryId"]))
        for tid in p.get("tags", []):
            product_tags.append((run_pk, p["id"], tid))

    # Create extra bulk products to reach the requested size N_products
    remaining = max(0, N_products - len(BASE_PRODUCTS))
    for i in range(remaining):
        pid = f"p_bulk_{i}"
        brand_id = rng.choice(brand_ids)
        category_id = rng.choice(cat_ids)
        price = round(rng.uniform(5.0, 300.0), 2)

        products.append((run_pk, pid, f"Bulk Product {i}", float(price), brand_id, category_id))

        # Ensure each bulk product has at least one tag (like the Gremlin generator)
        product_tags.append((run_pk, pid, rng.choice(tag_ids)))

    product_ids = [p[1] for p in products]

    # Interactions per user:
    # Gremlin bug: max(5, min(0, N//60)) => always 5.
    if keep_gremlin_bug_interactions_per_user:
        interactions_per_user = 5
    else:
        interactions_per_user = max(5, min(50, N_products // 60))

    # Interaction tuple:
    #   (run_pk, user_id, product_id, interaction_type)
    interactions: List[Tuple[str, str, str, str]] = []
    for uid in user_ids:
        k = min(interactions_per_user, len(product_ids))
        viewed_products = rng.sample(product_ids, k=k)
        for pid in viewed_products:
            interactions.append((run_pk, uid, pid, "VIEWED"))
            if rng.random() > 0.5:
                interactions.append((run_pk, uid, pid, "BOUGHT"))
                if rng.random() > 0.7:
                    interactions.append((run_pk, uid, pid, "LIKED"))

    # Group products by category to create similarity-like edges inside each category
    category_to_products: Dict[str, List[str]] = {}
    for (_, pid, _, _, _, cat_id) in products:
        category_to_products.setdefault(cat_id, []).append(pid)

    # Product similarity tuple (SIMILAR_TO):
    #   (run_pk, product_id, similar_product_id, score)
    product_similarity: List[Tuple[str, str, str, float]] = []

    # Bought-together tuple (BOUGHT_TOGETHER):
    #   (run_pk, product_id, other_product_id, support)
    bought_together: List[Tuple[str, str, str, float]] = []

    for _, pid_list in category_to_products.items():
        if len(pid_list) < 2:
            continue
        for pid in pid_list:
            sample_size = min(4, len(pid_list))
            candidates = rng.sample(pid_list, k=sample_size)
            neighbors = [x for x in candidates if x != pid][:3]
            for nb in neighbors:
                score = round(rng.uniform(0.6, 0.95), 2)
                product_similarity.append((run_pk, pid, nb, float(score)))
                bought_together.append((run_pk, pid, nb, float(score)))

    # User similarity tuple (SIMILAR_USER):
    #   (run_pk, user_id, other_user_id, score)
    user_similarity: List[Tuple[str, str, str, float]] = []
    if len(user_ids) > 1:
        for uid in user_ids:
            candidates = [u for u in user_ids if u != uid]
            k = min(2, len(candidates))
            for other in rng.sample(candidates, k=k):
                score = round(rng.uniform(0.2, 0.9), 2)
                user_similarity.append((run_pk, uid, other, float(score)))

    return {
        "run_pk": run_pk,
        "brands": brands,
        "categories": categories,
        "tags": tags,
        "users": users,
        "products": products,
        "product_tags": product_tags,
        "interactions": interactions,
        "product_similarity": product_similarity,
        "bought_together": bought_together,
        "user_similarity": user_similarity,
    }
