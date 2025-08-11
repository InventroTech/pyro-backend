from datetime import timedelta, datetime

# --- Utility Functions ---
def convert_seconds(value, unit):
    """Convert seconds to the requested unit."""
    if unit == 'seconds':
        return round(value, 2)
    elif unit == 'minutes':
        return round(value / 60, 2)
    elif unit == 'hours':
        return round(value / 3600, 2)
    elif unit == 'days':
        return round(value / 86400, 2)
    return round(value / 3600, 2)  # Default: hours

def convert_timedelta(td, unit):
    """Convert timedelta to requested unit."""
    return convert_seconds(td.total_seconds(), unit)

def get_date_range(start, end):
    """Generate list of dates between start and end, inclusive."""
    delta = (end - start).days
    return [start + timedelta(days=i) for i in range(delta + 1)]

def safe_strptime(date_str):
    """Parse date safely, return None if parsing fails."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None
    
def extract_date_range_from_request(qs, request, created_field='completed_at'):
    start = request.query_params.get('start')
    end = request.query_params.get('end')
    min_date = qs.order_by(created_field).first()
    min_date = getattr(min_date, created_field).date() if min_date else datetime.today().date()
    max_date = qs.order_by(f'-{created_field}').first()
    max_date = getattr(max_date, created_field).date() if max_date else datetime.today().date()
    start_date = safe_strptime(start) or min_date
    end_date = safe_strptime(end) or max_date
    return start_date, end_date

def filter_by_tenant(qs, request):
    tenant_id = request.query_params.get('tenant_id')
    return qs.filter(tenant_id=tenant_id) if tenant_id else qs

