from gremlin_python.driver import client, serializer

from config import ENDPOINT, PRIMARY_KEY, USERNAME


# Role:
#   Create and return a Gremlin client configured for Azure Cosmos DB (Gremlin API).
# Inputs:
#   - None (uses ENDPOINT / USERNAME / PRIMARY_KEY from config.py)
# Output:
#   - client.Client: an open Gremlin client connection ready to submit queries
def create_gremlin_client() -> client.Client:
    gclient = client.Client(
        ENDPOINT,
        "g",
        username=USERNAME,
        password=PRIMARY_KEY,
        message_serializer=serializer.GraphSONSerializersV2d0(),
    )
    print_banner("Connected to Azure Cosmos DB (Gremlin API)")
    return gclient


# Role:
#   Close the Gremlin client cleanly (release sockets / threads).
# Inputs:
#   - gclient (client.Client): an existing Gremlin client instance (can be None)
# Output:
#   - None
def close_gremlin_client(gclient: client.Client) -> None:
    if gclient is not None:
        gclient.close()
        print_banner("Gremlin client closed")


# Role:
#   Print a centered banner in the console (visual grouping of logs).
# Inputs:
#   - title (str): text to display
# Output:
#   - None
def print_banner(title: str) -> None:
    line = "=" * (len(title) + 8)
    print(f"\n{line}\n=== {title} ===\n{line}")


# Role:
#   Print a step message (useful for progress logs).
# Inputs:
#   - message (str): step text
# Output:
#   - None
def print_step(message: str) -> None:
    print(f"→ {message}")


# Role:
#   Print a warning message.
# Inputs:
#   - message (str): warning text
# Output:
#   - None
def print_warning(message: str) -> None:
    print(f"⚠ {message}")


# Role:
#   Print a success message.
# Inputs:
#   - message (str): success text
# Output:
#   - None
def print_ok(message: str) -> None:
    print(f"✔ {message}")


# Role:
#   Execute a Gremlin query and return results as a Python list.
#   Two supported call styles:
#     - run_gremlin(query)              -> uses a global variable named "gclient"
#     - run_gremlin(gclient, query)     -> uses the provided client
# Inputs:
#   - args:
#       * (query: str)
#       * OR (gclient: client.Client, query: str)
# Output:
#   - list: results returned by Gremlin (empty list if no results or if a handled error occurs)
def run_gremlin(*args):
    # Resolve arguments into (gc, query)
    if len(args) == 1:
        query = args[0]
        # If you use run_gremlin(query) style, you must define a module-level variable named gclient
        if "gclient" not in globals() or globals().get("gclient") is None:
            raise RuntimeError("Global gclient is not set. Use run_gremlin(gclient, query) or define gclient at module level.")
        gc = globals()["gclient"]
    elif len(args) == 2:
        gc, query = args
    else:
        raise TypeError("run_gremlin expects (query) or (gclient, query)")

    try:
        # Use synchronous execution (stable in notebooks and simple scripts)
        return gc.submit(query).all().result()

    except Exception as e:
        msg = str(e)

        # Cosmos DB often returns 409 Conflict when inserting an element that already exists.
        # In data generation scripts, this is usually safe to ignore.
        if (
            "Resource with specified id or name already exists" in msg
            or "StatusCode = Conflict" in msg
        ):
            print_warning("Vertex/edge already exists, skipping duplicate creation.")
            return []

        print_warning(f"Gremlin error: {e}")
        print("Query was:")
        print(query)
        return []


# Role:
#   Print Gremlin results in a readable console format (vertices, edges, and paths).
# Inputs:
#   - results (any): typically a list returned by run_gremlin(...)
#   - title (str): display title for the block
# Output:
#   - None
def pretty_print(results, title: str = "Results") -> None:
    if not results:
        print_warning("No results found.")
        return

    print("\n" + "-" * 60)
    print(f"📌 {title}")
    print("-" * 60)

    for idx, item in enumerate(results, start=1):
        # Vertex (GraphSON v2 style)
        if isinstance(item, dict) and item.get("type") == "vertex":
            _print_vertex(item, idx)
        # Edge (GraphSON v2 style)
        elif isinstance(item, dict) and item.get("type") == "edge":
            _print_edge(item, idx)
        # Path objects often contain an "objects" array
        elif isinstance(item, dict) and "objects" in item:
            _print_path(item, idx)
        else:
            print(f"[{idx}] {item}")

    print("-" * 60 + "\n")


# Role:
#   Pretty-print one vertex object (GraphSON v2).
# Inputs:
#   - v (dict): vertex object parsed from GraphSON
#   - idx (int): display index
# Output:
#   - None
def _print_vertex(v: dict, idx: int) -> None:
    label = v.get("label", "vertex")
    vid = v.get("id", "N/A")
    props = v.get("properties", {})

    print(f"\n🔹 [{idx}] VERTEX: {label.upper()} (id={vid})")

    for key, value in props.items():
        # GraphSON vertex properties are often lists of objects: [{"id":..., "value":...}]
        if isinstance(value, list) and value:
            val = value[0].get("value", value[0])
        else:
            val = value
        print(f"    - {key}: {val}")


# Role:
#   Pretty-print one edge object (GraphSON v2).
# Inputs:
#   - e (dict): edge object parsed from GraphSON
#   - idx (int): display index
# Output:
#   - None
def _print_edge(e: dict, idx: int) -> None:
    label = e.get("label", "edge")
    out_v = e.get("outV", "?")
    in_v = e.get("inV", "?")
    props = e.get("properties", {})

    print(f"\n🔸 [{idx}] EDGE: {label}")
    print(f"    FROM: {out_v}")
    print(f"    TO  : {in_v}")

    for key, value in props.items():
        print(f"    - {key}: {value}")


# Role:
#   Pretty-print a path object (GraphSON v2-ish) when the result is a traversal path.
# Inputs:
#   - p (dict): path object containing an "objects" list
#   - idx (int): display index
# Output:
#   - None
def _print_path(p: dict, idx: int) -> None:
    print(f"\n🔷 [{idx}] PATH")
    for obj in p.get("objects", []):
        if isinstance(obj, dict) and obj.get("type") == "vertex":
            print(f"    ▶ ({obj.get('label')}) {obj.get('id')}")
        elif isinstance(obj, dict) and obj.get("type") == "edge":
            print(f"    ──[{obj.get('label')}]──>")


# Role:
#   Count vertices by label (simple helper for quick stats/verification).
# Inputs:
#   - gclient (client.Client): Gremlin client
#   - label (str): vertex label (e.g., "product", "user")
# Output:
#   - int: number of vertices matching the label (0 if none)
def count_vertices_by_label(gclient: client.Client, label: str) -> int:
    query = f"g.V().hasLabel('{label}').count()"
    result = run_gremlin(gclient, query)
    if result and isinstance(result[0], (int, float)):
        return int(result[0])
    return 0



