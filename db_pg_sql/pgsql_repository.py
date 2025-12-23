import time
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import execute_values

from config import get_connection
from data_generator import generate_data_gremlin_like, make_run_pk


# Role:
#   Print a banner-style title for console output.
# Inputs:
#   - title (str): text to display
# Output:
#   - None
def print_banner(title: str) -> None:
    line = "=" * (len(title) + 8)
    print(f"\n{line}\n=== {title} ===\n{line}")


# Role:
#   Print a step marker (progress log).
# Inputs:
#   - msg (str): message to display
# Output:
#   - None
def print_step(msg: str) -> None:
    print(f"→ {msg}")


# Role:
#   Print a success marker.
# Inputs:
#   - msg (str): message to display
# Output:
#   - None
def print_ok(msg: str) -> None:
    print(f"✔ {msg}")


# Role:
#   Print a warning marker.
# Inputs:
#   - msg (str): message to display
# Output:
#   - None
def print_warning(msg: str) -> None:
    print(f"⚠ {msg}")


# Role:
#   High-resolution timer (better than time.time for benchmarking).
# Inputs:
#   - None
# Output:
#   - float: current timestamp in seconds
def now() -> float:
    return time.perf_counter()


# Role:
#   Create a PostgreSQL connection to be reused like a "client".
# Inputs:
#   - None (uses get_connection() from config.py)
# Output:
#   - psycopg2 connection: open connection with autocommit disabled
def create_pg_client():
    conn = get_connection()
    conn.autocommit = False
    print_banner("Connected to PostgreSQL (SQL)")
    return conn


# Role:
#   Close the PostgreSQL connection cleanly.
# Inputs:
#   - conn: psycopg2 connection (can be None)
# Output:
#   - None
def close_pg_client(conn) -> None:
    if conn is not None:
        conn.close()
        print_banner("PostgreSQL connection closed")


# Role:
#   List of benchmark tables (useful if you want to loop over them).
# Inputs:
#   - None
# Output:
#   - None (constant list)
TABLES = [
    "user_interactions",
    "user_similarity",
    "product_bought_together",
    "product_similarity",
    "product_tags",
    "products",
    "users",
    "tags",
    "categories",
    "brands",
]


# Role:
#   Check if a given table contains a specific column (schema migration helper).
# Inputs:
#   - cur: psycopg2 cursor
#   - table (str): table name
#   - column (str): column name
# Output:
#   - bool: True if the column exists, else False
def _table_has_column(cur, table: str, column: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s AND column_name=%s
        LIMIT 1;
        """,
        (table, column),
    )
    return cur.fetchone() is not None


# Role:
#   Drop (delete) all benchmark tables. Useful when schema changes.
# Inputs:
#   - conn: psycopg2 connection
# Output:
#   - None
def drop_schema(conn) -> None:
    print_warning("Dropping existing benchmark tables (force recreate)...")
    cur = conn.cursor()
    cur.execute(
        """
        DROP TABLE IF EXISTS
            user_interactions,
            user_similarity,
            product_bought_together,
            product_similarity,
            product_tags,
            products,
            users,
            tags,
            categories,
            brands
        CASCADE;
        """
    )
    conn.commit()
    cur.close()
    print_ok("Old schema dropped.")


# Role:
#   Create all tables + indexes if they don't exist.
#   If a previous schema exists but is missing run_pk, it drops and recreates automatically.
# Inputs:
#   - conn: psycopg2 connection
#   - force_recreate (bool): if True, always drop and recreate
# Output:
#   - None
def init_schema(conn, *, force_recreate: bool = False) -> None:
    cur = conn.cursor()

    if force_recreate:
        drop_schema(conn)
        cur = conn.cursor()
    else:
        # Detect if schema exists
        cur.execute("SELECT to_regclass('public.brands');")
        exists = cur.fetchone()[0] is not None

        # If schema exists but without run_pk column, it's an old version -> recreate
        if exists and not _table_has_column(cur, "brands", "run_pk"):
            cur.close()
            drop_schema(conn)
            cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS brands (
            run_pk   VARCHAR(80) NOT NULL,
            brand_id VARCHAR(50) NOT NULL,
            name     VARCHAR(100) NOT NULL,
            PRIMARY KEY (run_pk, brand_id)
        );

        CREATE TABLE IF NOT EXISTS categories (
            run_pk      VARCHAR(80) NOT NULL,
            category_id VARCHAR(50) NOT NULL,
            name        VARCHAR(100) NOT NULL,
            parent_id   VARCHAR(50),
            PRIMARY KEY (run_pk, category_id)
        );

        CREATE TABLE IF NOT EXISTS tags (
            run_pk VARCHAR(80) NOT NULL,
            tag_id VARCHAR(50) NOT NULL,
            name   VARCHAR(100) NOT NULL,
            PRIMARY KEY (run_pk, tag_id)
        );

        CREATE TABLE IF NOT EXISTS users (
            run_pk  VARCHAR(80) NOT NULL,
            user_id VARCHAR(50) NOT NULL,
            name    VARCHAR(100) NOT NULL,
            PRIMARY KEY (run_pk, user_id)
        );

        CREATE TABLE IF NOT EXISTS products (
            run_pk      VARCHAR(80) NOT NULL,
            product_id  VARCHAR(50) NOT NULL,
            name        VARCHAR(200),
            price       DECIMAL(10,2),
            brand_id    VARCHAR(50) NOT NULL,
            category_id VARCHAR(50) NOT NULL,
            PRIMARY KEY (run_pk, product_id),
            FOREIGN KEY (run_pk, brand_id) REFERENCES brands(run_pk, brand_id),
            FOREIGN KEY (run_pk, category_id) REFERENCES categories(run_pk, category_id)
        );

        CREATE TABLE IF NOT EXISTS product_tags (
            run_pk     VARCHAR(80) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            tag_id     VARCHAR(50) NOT NULL,
            PRIMARY KEY (run_pk, product_id, tag_id),
            FOREIGN KEY (run_pk, product_id) REFERENCES products(run_pk, product_id),
            FOREIGN KEY (run_pk, tag_id) REFERENCES tags(run_pk, tag_id)
        );

        CREATE TABLE IF NOT EXISTS user_interactions (
            id BIGSERIAL PRIMARY KEY,
            run_pk VARCHAR(80) NOT NULL,
            user_id VARCHAR(50) NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            interaction_type VARCHAR(20) NOT NULL,
            interaction_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (run_pk, user_id) REFERENCES users(run_pk, user_id),
            FOREIGN KEY (run_pk, product_id) REFERENCES products(run_pk, product_id)
        );

        CREATE TABLE IF NOT EXISTS product_similarity (
            run_pk VARCHAR(80) NOT NULL,
            src_product_id VARCHAR(50) NOT NULL,
            dst_product_id VARCHAR(50) NOT NULL,
            score FLOAT NOT NULL,
            PRIMARY KEY (run_pk, src_product_id, dst_product_id),
            FOREIGN KEY (run_pk, src_product_id) REFERENCES products(run_pk, product_id),
            FOREIGN KEY (run_pk, dst_product_id) REFERENCES products(run_pk, product_id)
        );

        CREATE TABLE IF NOT EXISTS product_bought_together (
            run_pk VARCHAR(80) NOT NULL,
            src_product_id VARCHAR(50) NOT NULL,
            dst_product_id VARCHAR(50) NOT NULL,
            support FLOAT NOT NULL,
            PRIMARY KEY (run_pk, src_product_id, dst_product_id),
            FOREIGN KEY (run_pk, src_product_id) REFERENCES products(run_pk, product_id),
            FOREIGN KEY (run_pk, dst_product_id) REFERENCES products(run_pk, product_id)
        );

        CREATE TABLE IF NOT EXISTS user_similarity (
            run_pk VARCHAR(80) NOT NULL,
            src_user_id VARCHAR(50) NOT NULL,
            dst_user_id VARCHAR(50) NOT NULL,
            score FLOAT NOT NULL,
            PRIMARY KEY (run_pk, src_user_id, dst_user_id),
            FOREIGN KEY (run_pk, src_user_id) REFERENCES users(run_pk, user_id),
            FOREIGN KEY (run_pk, dst_user_id) REFERENCES users(run_pk, user_id)
        );
        """
    )

    # Indexes to speed up benchmark queries (filters by run_pk + join keys)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_products_run_cat ON products(run_pk, category_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ui_run_prod_type ON user_interactions(run_pk, product_id, interaction_type);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ui_run_user_type ON user_interactions(run_pk, user_id, interaction_type);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sim_run_src ON product_similarity(run_pk, src_product_id, score);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bt_run_src ON product_bought_together(run_pk, src_product_id, support);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_us_run_src ON user_similarity(run_pk, src_user_id, score);")

    conn.commit()
    cur.close()
    print_ok("SQL schema initialized (tables + indexes).")


# Role:
#   Reset only one dataset partition (run_pk) by deleting rows in FK-safe order.
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str): partition key to delete
# Output:
#   - None
def reset_run(conn, run_pk: str) -> None:
    cur = conn.cursor()
    print_step(f"Resetting SQL data for run_pk='{run_pk}' ...")

    # Delete children first, then parents (to respect foreign keys)
    cur.execute("DELETE FROM user_interactions          WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM user_similarity            WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM product_bought_together    WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM product_similarity         WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM product_tags               WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM products                   WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM users                      WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM tags                       WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM categories                 WHERE run_pk=%s;", (run_pk,))
    cur.execute("DELETE FROM brands                     WHERE run_pk=%s;", (run_pk,))

    conn.commit()
    cur.close()
    print_ok("Run partition cleared.")


# Role:
#   Full reset of ALL tables using TRUNCATE (fast) + restart identity.
# Inputs:
#   - conn: psycopg2 connection
# Output:
#   - None
def reset_all_fast_truncate(conn) -> None:
    cur = conn.cursor()
    print_step("TRUNCATE all tables (full reset)...")
    cur.execute(
        """
        TRUNCATE TABLE
            user_interactions,
            user_similarity,
            product_bought_together,
            product_similarity,
            product_tags,
            products,
            users,
            tags,
            categories,
            brands
        RESTART IDENTITY CASCADE;
        """
    )
    conn.commit()
    cur.close()
    print_ok("All data truncated.")


# Role:
#   Bulk insert all generated rows into SQL tables using execute_values for speed.
# Inputs:
#   - conn: psycopg2 connection
#   - data (Dict[str, list]): output from generate_data_gremlin_like(...)
#   - page_size (int): batch size used by execute_values
# Output:
#   - None
def insert_data(conn, data: Dict[str, list], page_size: int = 5000) -> None:
    cur = conn.cursor()
    print_step("Bulk inserting rows...")

    execute_values(
        cur,
        "INSERT INTO brands (run_pk, brand_id, name) VALUES %s ON CONFLICT DO NOTHING",
        data["brands"],
        page_size=page_size,
    )
    execute_values(
        cur,
        "INSERT INTO categories (run_pk, category_id, name, parent_id) VALUES %s ON CONFLICT DO NOTHING",
        data["categories"],
        page_size=page_size,
    )
    execute_values(
        cur,
        "INSERT INTO tags (run_pk, tag_id, name) VALUES %s ON CONFLICT DO NOTHING",
        data["tags"],
        page_size=page_size,
    )
    execute_values(
        cur,
        "INSERT INTO users (run_pk, user_id, name) VALUES %s ON CONFLICT DO NOTHING",
        data["users"],
        page_size=page_size,
    )
    execute_values(
        cur,
        "INSERT INTO products (run_pk, product_id, name, price, brand_id, category_id) VALUES %s ON CONFLICT DO NOTHING",
        data["products"],
        page_size=page_size,
    )

    if data.get("product_tags"):
        execute_values(
            cur,
            "INSERT INTO product_tags (run_pk, product_id, tag_id) VALUES %s ON CONFLICT DO NOTHING",
            data["product_tags"],
            page_size=page_size,
        )

    if data.get("interactions"):
        execute_values(
            cur,
            "INSERT INTO user_interactions (run_pk, user_id, product_id, interaction_type) VALUES %s",
            data["interactions"],
            page_size=page_size,
        )

    if data.get("product_similarity"):
        execute_values(
            cur,
            "INSERT INTO product_similarity (run_pk, src_product_id, dst_product_id, score) VALUES %s ON CONFLICT DO NOTHING",
            data["product_similarity"],
            page_size=page_size,
        )

    if data.get("bought_together"):
        execute_values(
            cur,
            "INSERT INTO product_bought_together (run_pk, src_product_id, dst_product_id, support) VALUES %s ON CONFLICT DO NOTHING",
            data["bought_together"],
            page_size=page_size,
        )

    if data.get("user_similarity"):
        execute_values(
            cur,
            "INSERT INTO user_similarity (run_pk, src_user_id, dst_user_id, score) VALUES %s ON CONFLICT DO NOTHING",
            data["user_similarity"],
            page_size=page_size,
        )

    conn.commit()
    cur.close()
    print_ok("Bulk insert done.")


# Role:
#   Build the SQL dataset equivalent to the Gremlin generator:
#     1) reset only the current run_pk
#     2) generate in-memory dataset (lists of tuples)
#     3) bulk insert into SQL tables
# Inputs:
#   - conn: psycopg2 connection
#   - total_products (int): number of products (min 10)
#   - run_pk (Optional[str]): partition key (generated if not provided)
#   - seed (int): RNG seed for reproducibility
#   - page_size (int): execute_values batch size
#   - keep_gremlin_bug_interactions_per_user (bool): mimic Gremlin interactions scaling behavior
# Output:
#   - Dict[str, Any]: stats and timings for reset/generate/insert/total
def build_sql(
    conn,
    total_products: int,
    run_pk: Optional[str] = None,
    seed: int = 42,
    page_size: int = 5000,
    keep_gremlin_bug_interactions_per_user: bool = True,
) -> Dict[str, Any]:

    total_products = max(10, int(total_products))
    run_pk = run_pk or make_run_pk(total_products)

    t0 = now()
    reset_run(conn, run_pk)
    t_reset = now() - t0

    t0 = now()
    data = generate_data_gremlin_like(
        N_products=total_products,
        run_pk=run_pk,
        seed=seed,
        keep_gremlin_bug_interactions_per_user=keep_gremlin_bug_interactions_per_user,
    )
    t_gen = now() - t0

    t0 = now()
    insert_data(conn, data, page_size=page_size)
    t_ins = now() - t0

    return {
        "run_pk": run_pk,
        "products": total_products,
        "users": len(data["users"]),
        "interactions": len(data["interactions"]),
        "similarities": len(data.get("product_similarity", [])),
        "bought_together": len(data.get("bought_together", [])),
        "user_similarity": len(data.get("user_similarity", [])),
        "reset_sec": t_reset,
        "generate_sec": t_gen,
        "insert_sec": t_ins,
        "build_total_sec": t_reset + t_gen + t_ins,
    }


# Role:
#   Run a SQL query with parameters and measure execution time.
# Inputs:
#   - conn: psycopg2 connection
#   - query (str): SQL query string
#   - params (Tuple[Any, ...]): parameter tuple for placeholders
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def run_query_timed(conn, query: str, params: Tuple[Any, ...]) -> Tuple[List[tuple], float]:
    t0 = now()
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    return rows, (now() - t0)


# Role:
#   Recommendation Query #1: "Similar products by category"
#   Logic:
#     - fetch category of product_id (p1)
#     - return other products in the same category (p2)
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - product_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def similar_by_category(conn, run_pk: str, product_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT p2.*
    FROM products p1
    JOIN products p2
      ON p1.run_pk = p2.run_pk
     AND p1.category_id = p2.category_id
    WHERE p1.run_pk = %s
      AND p1.product_id = %s
      AND p2.product_id <> %s
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, product_id, product_id))


# Role:
#   Recommendation Query #2: "Similar products using product_similarity (SIMILAR_TO)"
#   Logic:
#     - read similarity edges from product_similarity
#     - join to products for full product fields
#     - order by score DESC
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - product_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def similar_by_similarity(conn, run_pk: str, product_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT p_dst.*
    FROM product_similarity s
    JOIN products p_dst
      ON p_dst.run_pk = s.run_pk
     AND p_dst.product_id = s.dst_product_id
    WHERE s.run_pk = %s
      AND s.src_product_id = %s
    ORDER BY s.score DESC
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, product_id))


# Role:
#   Recommendation Query #3: "Customers also bought"
#   Logic:
#     - find users who BOUGHT the target product (ui1)
#     - find other products BOUGHT by those same users (ui2)
#     - return distinct products
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - product_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def customers_also_bought(conn, run_pk: str, product_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT DISTINCT p2.*
    FROM user_interactions ui1
    JOIN user_interactions ui2
      ON ui1.run_pk = ui2.run_pk
     AND ui1.user_id = ui2.user_id
    JOIN products p2
      ON p2.run_pk = ui2.run_pk
     AND p2.product_id = ui2.product_id
    WHERE ui1.run_pk = %s
      AND ui1.product_id = %s
      AND ui1.interaction_type = 'BOUGHT'
      AND ui2.interaction_type = 'BOUGHT'
      AND ui2.product_id <> ui1.product_id
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, product_id))


# Role:
#   Recommendation Query #4: "User-based recommendations"
#   Logic:
#     - take products BOUGHT by the target user (ui_u)
#     - find other users who BOUGHT the same products (ui_others)
#     - recommend other products BOUGHT by those users (ui_rec)
#     - exclude products already bought by the target user
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - user_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def user_recommendations(conn, run_pk: str, user_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT DISTINCT p3.*
    FROM user_interactions ui_u
    JOIN user_interactions ui_others
      ON ui_u.run_pk = ui_others.run_pk
     AND ui_u.product_id = ui_others.product_id
    JOIN user_interactions ui_rec
      ON ui_others.run_pk = ui_rec.run_pk
     AND ui_others.user_id = ui_rec.user_id
    JOIN products p3
      ON p3.run_pk = ui_rec.run_pk
     AND p3.product_id = ui_rec.product_id
    WHERE ui_u.run_pk = %s
      AND ui_u.user_id = %s
      AND ui_u.interaction_type = 'BOUGHT'
      AND ui_others.interaction_type = 'BOUGHT'
      AND ui_rec.interaction_type = 'BOUGHT'
      AND ui_rec.product_id NOT IN (
          SELECT product_id
          FROM user_interactions
          WHERE run_pk = %s AND user_id = %s AND interaction_type = 'BOUGHT'
      )
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, user_id, run_pk, user_id))


# Role:
#   Extra Query: "Bought together" based on product_bought_together table.
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - product_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def bought_together(conn, run_pk: str, product_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT p2.*
    FROM product_bought_together bt
    JOIN products p2
      ON p2.run_pk = bt.run_pk
     AND p2.product_id = bt.dst_product_id
    WHERE bt.run_pk = %s
      AND bt.src_product_id = %s
    ORDER BY bt.support DESC
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, product_id))


# Role:
#   Extra Query: "Similar users" based on user_similarity table.
# Inputs:
#   - conn: psycopg2 connection
#   - run_pk (str)
#   - user_id (str)
# Output:
#   - Tuple[List[tuple], float]: (rows, elapsed_seconds)
def similar_users(conn, run_pk: str, user_id: str) -> Tuple[List[tuple], float]:
    q = """
    SELECT u2.*
    FROM user_similarity us
    JOIN users u2
      ON u2.run_pk = us.run_pk
     AND u2.user_id = us.dst_user_id
    WHERE us.run_pk = %s
      AND us.src_user_id = %s
    ORDER BY us.score DESC
    LIMIT 20;
    """
    return run_query_timed(conn, q, (run_pk, user_id))
