from typing import Iterable, List, Sequence

def rows_to_dicts(columns: Sequence[str], rows: Iterable[Sequence]):
    if not columns:
        return []
    return [dict(zip(columns, r)) for r in rows]
