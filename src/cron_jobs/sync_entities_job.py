import logging
from core.models import Tenant
from crm_records.services import sync_entity_schema

logger = logging.getLogger(__name__)

def run_entity_sync():
    """
    Cron job entry point. Loops through all active tenants and updates 
    their entity schemas for specific record types.
    """
    logger.info("Starting global entity schema sync job...")
    
    # 1. Get all tenants in the system
    tenants = Tenant.objects.all()
    
    # 2. Define which record types we want to track schemas for
    # (Check with your team if there are others besides 'lead' and 'ticket')
    entity_types_to_track = ['lead', 'ticket'] 
    
    total_records_processed = 0
    
    # 3. Loop through every tenant and every entity type
    for tenant in tenants:
        for entity_type in entity_types_to_track:
            try:
                # Call the "Brain" we built in services.py!
                processed_count = sync_entity_schema(tenant, entity_type)
                total_records_processed += processed_count
                
            except Exception as e:
                # If one tenant's data is corrupted, catch the error so the whole job doesn't crash
                logger.error(f"Failed to sync {entity_type} for tenant {tenant.slug or tenant.id}: {str(e)}")

    logger.info(f"Finished entity schema sync job. Total new records processed: {total_records_processed}")

# If your team runs these as direct python scripts from a server cron tab:
if __name__ == "__main__":
    run_entity_sync()