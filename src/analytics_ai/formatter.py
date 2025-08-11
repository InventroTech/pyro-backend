def format_results_for_table(results, max_rows=100):
    """
    Formats a list of dict results for tabular frontend consumption.
    Returns a dict: { type, columns, rows, message }
    """
    if not results:
        return {
            "type": "table",
            "columns": [],
            "rows": [],
            "message": "No results found."
        }
    columns = list(results[0].keys())
    rows = [ [row.get(col) for col in columns] for row in results[:max_rows] ]
    return {
        "type": "table",
        "columns": columns,
        "rows": rows,
        "message": None
    }
