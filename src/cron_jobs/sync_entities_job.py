import logging
from core.models import Tenant
from core.services import aggregate_all_entities

logger = logging.getLogger(__name__)

def run_entity_aggregation():
    """
    Cron job entry point for record aggregation.
    Discovers entity schemas by scanning new records and building snapshots
    with distinct field values.
    """
    logger.info("Starting global record aggregation job...")
    
    try:
        stats = aggregate_all_entities(chunk_size=1000)
        logger.info(
            f"Finished record aggregation job. "
            f"Processed {stats.get('total_records_processed', 0)} records across "
            f"{stats.get('total_entities_processed', 0)} entities. "
            f"Errors: {len(stats.get('errors', []))}"
        )
        return stats
        
    except Exception as e:
        logger.error(f"Record aggregation job failed: {str(e)}")
        raise

# If your team runs these as direct python scripts from a server cron tab:
if __name__ == "__main__":
    run_entity_aggregation()