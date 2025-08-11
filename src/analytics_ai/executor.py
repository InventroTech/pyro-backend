from django.db import connection

def execute_safe_sql(sql_query: str, params=None):
    """
    Executes a safe, validated SQL query.
    Returns rows as list of dicts: [{col1: val1, col2: val2, ...}, ...]
    """
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql_query, params or [])
            columns = [col[1] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            return rows, None
        except Exception as e:
            print(f"[SQL EXECUTOR ERROR]: {e} | SQL: {sql_query}")
            return None, str(e)
