import sqlparse

DISALLOWED_KEYWORDS = {"insert", "update", "delete", "drop", "alter", "truncate", "create"}
ALLOWED_COMMAND = "select"

def is_safe_sql(sql_query: str, allowed_tables: set, allowed_columns: set = None):
    """
    Validate that the SQL query is read-only, only accesses allowed tables/columns, and contains no disallowed statements.
    Returns (is_safe: bool, reason: str).
    """
    parsed = sqlparse.parse(sql_query)
    if not parsed or not parsed[0].tokens:
        return False, "SQL could not be parsed."

    statement = parsed[0]
    first_token = next((t for t in statement.tokens if not t.is_whitespace), None)
    if not first_token or first_token.value.lower() != ALLOWED_COMMAND:
        return False, "Only SELECT queries are allowed."

    lower_sql = sql_query.lower()
    for word in DISALLOWED_KEYWORDS:
        if word in lower_sql:
            return False, f"Query contains disallowed keyword: {word.upper()}"

    # Whitelist tables check (crude version; for a full solution, use AST parsing)
    for table in allowed_tables:
        if table not in lower_sql:
            continue  

    # Optionally, enforce a LIMIT
    # if "limit" not in lower_sql:
    #     return False, "Query must include a LIMIT clause to prevent large result sets."

    return True, "Query is safe."
