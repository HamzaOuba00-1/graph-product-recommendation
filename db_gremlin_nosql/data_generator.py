import random
from typing import List, Dict, Tuple, Optional

from tqdm import tqdm
from gremlin_client import run_gremlin, print_step, print_ok, print_warning


# ============================================================
# Static reference data used to initialize the graph
# ============================================================

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


# ============================================================
# Helper functions
# ============================================================

# Role:
#   Create a stable partition key for one graph generation run.
# Inputs:
#   - total_products (int): number of products to generate
# Output:
#   - str: partition key value used by all vertices
def make_run_pk(total_products: int) -> str:
    return f"bench_V{max(10, total_products)}"


# Role:
#   Build a Gremlin point-read query using partition key and vertex id.
# Inputs:
#   - pk (str): partition key value
#   - vid (str): vertex identifier
# Output:
#   - str: Gremlin query string for fast vertex lookup
def v(pk: str, vid: str) -> str:
    return f"g.V(['{pk}','{vid}'])"


# ============================================================
# Main orchestration function
# ============================================================

# Role:
#   Build the entire graph: reset, generate data, insert vertices and edges.
# Inputs:
#   - gclient: Gremlin client connection
#   - total_products (int): size of the dataset
#   - run_pk (Optional[str]): custom partition key (optional)
#   - seed (Optional[int]): random seed for reproducibility
#   - reset_batch_size (int): number of vertices deleted per batch
# Output:
#   - dict: statistics describing the created graph
def build_graph(
    gclient,
    total_products: int,
    run_pk: Optional[str] = None,
    seed: Optional[int] = 42,
    reset_batch_size: int = 200,
) -> dict:

    total_products = max(10, total_products)
    run_pk = run_pk or make_run_pk(total_products)

    if seed is not None:
        random.seed(seed)

    print_step(f"Resetting graph partition for run_pk='{run_pk}' ...")
    reset_graph_partition(gclient, run_pk, batch_size=reset_batch_size)
    print_ok("Partition cleared")

    categories, brands, tags, products, user_ids = build_master_data(total_products)

    num_users = len(user_ids)
    interactions_per_user = max(5, total_products // 60)

    print_step(f"Using {num_users} users, about {interactions_per_user} interactions per user.")

    print_step("Creating vertices (categories, brands, tags, products, users)...")
    create_vertices(gclient, run_pk, categories, brands, tags, products, user_ids)
    print_ok("All vertices inserted")

    print_step("Creating base relations (IN_CATEGORY, HAS_BRAND, HAS_TAG, PARENT_OF)...")
    create_base_relations(gclient, run_pk, categories, products)
    print_ok("Base relations created")

    print_step("Generating user interactions (VIEWED, BOUGHT, LIKED)...")
    create_user_interactions(gclient, run_pk, user_ids, [p["id"] for p in products], interactions_per_user)
    print_ok("User interactions created")

    print_step("Creating advanced edges (SIMILAR_TO, BOUGHT_TOGETHER, SIMILAR_USER)...")
    create_advanced_edges(gclient, run_pk, products, user_ids)
    print_ok("Advanced edges created")

    return {
        "run_pk": run_pk,
        "products": len(products),
        "users": len(user_ids),
        "categories": len(categories),
        "brands": len(brands),
        "tags": len(tags),
        "approx_interactions": num_users * interactions_per_user,
    }


# ============================================================
# Master data generation
# ============================================================

# Role:
#   Generate all business data before inserting it into the graph.
# Inputs:
#   - total_products (int): number of products to generate
# Output:
#   - Tuple: categories, brands, tags, products, user_ids
def build_master_data(
    total_products: int,
) -> Tuple[List[dict], List[dict], List[dict], List[dict], List[str]]:

    categories = list(BASE_CATEGORIES)
    brands = list(BASE_BRANDS)
    tags = list(BASE_TAGS)

    products: List[dict] = list(BASE_PRODUCTS)
    remaining = max(0, total_products - len(products))

    for i in range(remaining):
        pid = f"p_bulk_{i}"
        cat_obj = random.choice(categories)
        brand_obj = random.choice(brands)
        tag_obj = random.choice(tags)
        price = round(random.uniform(5.0, 300.0), 2)

        products.append(
            {
                "id": pid,
                "name": f"Bulk Product {i}",
                "brandId": brand_obj["id"],
                "categoryId": cat_obj["id"],
                "price": price,
                "tags": [tag_obj["id"]],
            }
        )

    num_users = max(10, total_products // 5)
    user_ids = [f"u{i}" for i in range(1, num_users + 1)]

    return categories, brands, tags, products, user_ids


# ============================================================
# Graph operations
# ============================================================

# Role:
#   Delete all vertices and edges belonging to a specific partition key.
# Inputs:
#   - gclient: Gremlin client
#   - run_pk (str): partition key to delete
#   - batch_size (int): number of vertices deleted per iteration
#   - max_rounds (int): safety cap to avoid infinite loops
# Output:
#   - None
def reset_graph_partition(
    gclient,
    run_pk: str,
    batch_size: int = 200,
    max_rounds: int = 1_000_000,
) -> None:

    rounds = 0
    while rounds < max_rounds:
        remaining = run_gremlin(gclient, f"g.V().has('pk','{run_pk}').limit(1).count()")
        if not remaining or remaining[0] == 0:
            return

        run_gremlin(gclient, f"g.V().has('pk','{run_pk}').limit({batch_size}).drop()")
        rounds += 1

    raise RuntimeError(f"reset_graph_partition exceeded max_rounds for run_pk={run_pk}")


# Role:
#   Insert vertices for categories, brands, tags, products, and users.
# Inputs:
#   - gclient: Gremlin client
#   - run_pk (str): partition key shared by all vertices
#   - categories, brands, tags, products: lists of dicts
#   - user_ids: list of user ids
# Output:
#   - None
def create_vertices(
    gclient,
    run_pk: str,
    categories: List[dict],
    brands: List[dict],
    tags: List[dict],
    products: List[dict],
    user_ids: List[str],
) -> None:

    for c in tqdm(categories, desc="Creating category vertices", unit="cat"):
        q = (
            "g.addV('category')"
            f".property('id','{c['id']}')"
            f".property('name','{c['name']}')"
            f".property('pk','{run_pk}')"
        )
        run_gremlin(gclient, q)

    for b in tqdm(brands, desc="Creating brand vertices", unit="brand"):
        q = (
            "g.addV('brand')"
            f".property('id','{b['id']}')"
            f".property('name','{b['name']}')"
            f".property('pk','{run_pk}')"
        )
        run_gremlin(gclient, q)

    for t in tqdm(tags, desc="Creating tag vertices", unit="tag"):
        q = (
            "g.addV('tag')"
            f".property('id','{t['id']}')"
            f".property('name','{t['name']}')"
            f".property('pk','{run_pk}')"
        )
        run_gremlin(gclient, q)

    for p in tqdm(products, desc="Creating product vertices", unit="product"):
        q = (
            "g.addV('product')"
            f".property('id','{p['id']}')"
            f".property('name','{p['name']}')"
            f".property('brandId','{p['brandId']}')"
            f".property('categoryId','{p['categoryId']}')"
            f".property('price',{p['price']})"
            f".property('pk','{run_pk}')"
        )
        run_gremlin(gclient, q)

    for uid in tqdm(user_ids, desc="Creating user vertices", unit="user"):
        q = (
            "g.addV('user')"
            f".property('id','{uid}')"
            f".property('name','User {uid[1:]}')"
            f".property('pk','{run_pk}')"
        )
        run_gremlin(gclient, q)


# Role:
#   Create core domain edges between products, categories, brands, tags, and category hierarchy.
# Inputs:
#   - gclient: Gremlin client
#   - run_pk (str): partition key shared by all vertices
#   - categories: list of category dicts (for parent-child links)
#   - products: list of product dicts (for product links)
# Output:
#   - None
def create_base_relations(gclient, run_pk: str, categories: List[dict], products: List[dict]) -> None:

    for p in tqdm(products, desc="Linking products to category/brand/tag", unit="prod"):
        pid = p["id"]
        cat_id = p["categoryId"]
        brand_id = p["brandId"]

        run_gremlin(gclient, f"{v(run_pk, pid)}.addE('IN_CATEGORY').to({v(run_pk, cat_id)})")
        run_gremlin(gclient, f"{v(run_pk, pid)}.addE('HAS_BRAND').to({v(run_pk, brand_id)})")

        for tag_id in p["tags"]:
            run_gremlin(gclient, f"{v(run_pk, pid)}.addE('HAS_TAG').to({v(run_pk, tag_id)})")

    for c in tqdm(categories, desc="Linking category hierarchy", unit="cat"):
        if c["parent"] is not None:
            parent_id = c["parent"]
            child_id = c["id"]
            run_gremlin(gclient, f"{v(run_pk, parent_id)}.addE('PARENT_OF').to({v(run_pk, child_id)})")


# Role:
#   Create interaction edges from users to products (VIEWED, BOUGHT, LIKED).
# Inputs:
#   - gclient: Gremlin client
#   - run_pk (str): partition key shared by all vertices
#   - user_ids: list of user ids
#   - product_ids: list of product ids
#   - interactions_per_user (int): number of viewed products per user
# Output:
#   - None
def create_user_interactions(
    gclient,
    run_pk: str,
    user_ids: List[str],
    product_ids: List[str],
    interactions_per_user: int,
) -> None:

    for uid in tqdm(user_ids, desc="Creating user interactions", unit="user"):
        k = min(interactions_per_user, len(product_ids))
        viewed_products = random.sample(product_ids, k=k)

        for pid in viewed_products:
            run_gremlin(gclient, f"{v(run_pk, uid)}.addE('VIEWED').to({v(run_pk, pid)})")

            if random.random() > 0.5:
                run_gremlin(gclient, f"{v(run_pk, uid)}.addE('BOUGHT').to({v(run_pk, pid)})")

                if random.random() > 0.7:
                    run_gremlin(gclient, f"{v(run_pk, uid)}.addE('LIKED').to({v(run_pk, pid)})")


# Role:
#   Create derived/analytic edges: product similarity and user similarity.
# Inputs:
#   - gclient: Gremlin client
#   - run_pk (str): partition key shared by all vertices
#   - products: list of product dicts
#   - user_ids: list of user ids
# Output:
#   - None
def create_advanced_edges(
    gclient,
    run_pk: str,
    products: List[dict],
    user_ids: List[str],
) -> None:

    category_to_products: Dict[str, List[str]] = {}
    for p in products:
        category_to_products.setdefault(p["categoryId"], []).append(p["id"])

    for _, pid_list in tqdm(
        category_to_products.items(),
        desc="Creating SIMILAR_TO & BOUGHT_TOGETHER",
        unit="category",
    ):
        if len(pid_list) < 2:
            continue

        for pid in pid_list:
            sample_size = min(4, len(pid_list))
            candidates = random.sample(pid_list, k=sample_size)
            neighbors = [x for x in candidates if x != pid][:3]
            if not neighbors:
                continue

            for neighbor in neighbors:
                score = round(random.uniform(0.6, 0.95), 2)

                run_gremlin(
                    gclient,
                    f"{v(run_pk, pid)}.addE('SIMILAR_TO').property('score',{score}).to({v(run_pk, neighbor)})",
                )
                run_gremlin(
                    gclient,
                    f"{v(run_pk, pid)}.addE('BOUGHT_TOGETHER').property('support',{score}).to({v(run_pk, neighbor)})",
                )

    if len(user_ids) > 1:
        for uid in tqdm(user_ids, desc="Creating SIMILAR_USER edges", unit="user"):
            candidates = [u for u in user_ids if u != uid]
            k = min(2, len(candidates))
            for other in random.sample(candidates, k=k):
                score = round(random.uniform(0.2, 0.9), 2)
                run_gremlin(
                    gclient,
                    f"{v(run_pk, uid)}.addE('SIMILAR_USER').property('score',{score}).to({v(run_pk, other)})",
                )
