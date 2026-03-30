import os
import requests
import logging
import json
from typing import Dict, Any, Optional
from django.conf import settings
from pathlib import Path
from crm_records.models import Record, EntityTypeSchema
import environ

logger = logging.getLogger(__name__)

# Initialize environ.Env and read .env file
env = environ.Env()
# Read .env file from project root (same pattern as settings.py)
# services.py is at src/crm_records/services.py
# parent.parent = src (same as settings.py BASE_DIR)
# parent.parent.parent = project root (where .env file is)
BASE_DIR_SERVICES = Path(__file__).resolve().parent.parent
env.read_env(os.path.join(BASE_DIR_SERVICES.parent, '.env'))


class PrajaService:
    """
    Service for sending data to Praja server when:
    - Record entity_type is 'lead' and tenant matches PRAJA_TENANT
    - SupportTicket tenant matches PRAJA_TENANT
    """
    
    def __init__(self):
        # Use env() to read from .env file, with fallback to os.environ
        # Also try os.environ.get() as fallback in case .env wasn't loaded
        self.praja_server_url = env("PRAJA_SERVER_URL", default=None) or os.environ.get("PRAJA_SERVER_URL")
        self.praja_tenant_id = env("PRAJA_TENANT", default=None) or os.environ.get("PRAJA_TENANT")
    
    def _is_praja_tenant(self, tenant) -> bool:
        """Check if tenant matches PRAJA_TENANT from env."""
        if not self.praja_tenant_id or not tenant:
            return False
        return str(tenant.id) == str(self.praja_tenant_id)
    
    def should_send_record_to_praja(self, record) -> bool:
        """
        Check if record should be sent to Praja server.
        
        Conditions:
        - entity_type must be 'lead'
        - tenant.id must match PRAJA_TENANT from env
        """
        if not self.praja_server_url or not self.praja_tenant_id:
            print(f"⚠️  [PRAJA] Skipping - PRAJA_SERVER_URL or PRAJA_TENANT not configured")
            return False
        
        if record.entity_type != 'lead':
            print(f"⚠️  [PRAJA] Skipping record {record.id} - entity_type is '{record.entity_type}', not 'lead'")
            return False
        
        is_praja = self._is_praja_tenant(record.tenant)
        if not is_praja:
            print(f"⚠️  [PRAJA] Skipping record {record.id} - tenant {record.tenant.id if record.tenant else None} doesn't match PRAJA_TENANT")
        else:
            print(f"✅ [PRAJA] Record {record.id} matches conditions - will send to Praja server")
        return is_praja
    
    def should_send_ticket_to_praja(self, ticket) -> bool:
        """
        Check if support ticket should be sent to Praja server.
        
        Conditions:
        - tenant.id must match PRAJA_TENANT from env
        """
        if not self.praja_server_url or not self.praja_tenant_id:
            print(f"⚠️  [PRAJA] Skipping - PRAJA_SERVER_URL or PRAJA_TENANT not configured")
            return False
        
        is_praja = self._is_praja_tenant(ticket.tenant)
        if not is_praja:
            print(f"⚠️  [PRAJA] Skipping ticket {ticket.id} - tenant {ticket.tenant.id if ticket.tenant else None} doesn't match PRAJA_TENANT")
        else:
            print(f"✅ [PRAJA] Ticket {ticket.id} matches conditions - will send to Praja server")
        return is_praja
    
    def prepare_record_data(self, record) -> Dict[str, Any]:
        """
        Prepare full record data for sending to Praja server.
        Includes all record fields including data and pyro_data.
        """
        return {
            "id": record.id,
            "tenant_id": str(record.tenant.id) if record.tenant else None,
            "entity_type": record.entity_type,
            "data": record.data or {},
            "pyro_data": record.pyro_data or {},
            "created_at": record.created_at.isoformat() if record.created_at else None,
            "updated_at": record.updated_at.isoformat() if record.updated_at else None,
        }
    
    def prepare_ticket_data(self, ticket) -> Dict[str, Any]:
        """
        Prepare full support ticket data for sending to Praja server.
        Includes all ticket fields.
        """
        return {
            "id": ticket.id,
            "tenant_id": str(ticket.tenant.id) if ticket.tenant else None,
            "created_at": ticket.created_at.isoformat() if ticket.created_at else None,
            "ticket_date": ticket.ticket_date.isoformat() if ticket.ticket_date else None,
            "user_id": ticket.user_id,
            "name": ticket.name,
            "phone": ticket.phone,
            "source": ticket.source,
            "subscription_status": ticket.subscription_status,
            "atleast_paid_once": ticket.atleast_paid_once,
            "reason": ticket.reason,
            "other_reasons": ticket.other_reasons or [],
            "badge": ticket.badge,
            "poster": ticket.poster,
            "assigned_to": str(ticket.assigned_to.id) if ticket.assigned_to else None,
            "layout_status": ticket.layout_status,
            "state": ticket.state,
            "resolution_status": ticket.resolution_status,
            "resolution_time": ticket.resolution_time,
            "cse_name": ticket.cse_name,
            "cse_remarks": ticket.cse_remarks,
            "call_status": ticket.call_status,
            "call_attempts": ticket.call_attempts,
            "rm_name": ticket.rm_name,
            "completed_at": ticket.completed_at.isoformat() if ticket.completed_at else None,
            "snooze_until": ticket.snooze_until.isoformat() if ticket.snooze_until else None,
            "praja_dashboard_user_link": ticket.praja_dashboard_user_link,
            "display_pic_url": ticket.display_pic_url,
            "dumped_at": ticket.dumped_at.isoformat() if ticket.dumped_at else None,
            "review_requested": ticket.review_requested,
        }
    
    def send_record_to_praja(self, record) -> bool:
        """
        Send full record data to Praja server via POST request.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.should_send_record_to_praja(record):
            return False
        
        try:
            payload = self.prepare_record_data(record)
            
            print(f"📤 [PRAJA] Sending lead record to Praja server...")
            print(f"   Record ID: {record.id}")
            print(f"   Tenant ID: {record.tenant.id if record.tenant else None}")
            print(f"   URL: {self.praja_server_url}")
            
            logger.info(
                f"📤 Sending lead record to Praja server: "
                f"record_id={record.id} tenant_id={record.tenant.id if record.tenant else None} "
                f"url={self.praja_server_url}"
            )
            
            response = requests.post(
                self.praja_server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                },
                timeout=30
            )
            
            print(f"📥 [PRAJA] Response Status: {response.status_code}")
            logger.info(
                f"📥 Praja server response: status={response.status_code} "
                f"record_id={record.id}"
            )
            
            if not response.ok:
                print(f"❌ [PRAJA] Error: Status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                logger.error(
                    f"❌ Praja server error: status={response.status_code} "
                    f"response={response.text[:500]} record_id={record.id}"
                )
                return False
            
            print(f"✅ [PRAJA] Successfully sent lead record {record.id} to Praja server!")
            logger.info(
                f"✅ Successfully sent lead record to Praja server: "
                f"record_id={record.id}"
            )
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(
                f"❌ Error sending record to Praja server: {e} "
                f"record_id={record.id}"
            )
            return False
        except Exception as e:
            logger.exception(
                f"❌ Unexpected error sending record to Praja server: {e} "
                f"record_id={record.id}"
            )
            return False
    
    def send_ticket_to_praja(self, ticket) -> bool:
        """
        Send full support ticket data to Praja server via POST request.
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.should_send_ticket_to_praja(ticket):
            return False
        
        try:
            payload = self.prepare_ticket_data(ticket)
            
            print(f"📤 [PRAJA] Sending support ticket to Praja server...")
            print(f"   Ticket ID: {ticket.id}")
            print(f"   Tenant ID: {ticket.tenant.id if ticket.tenant else None}")
            print(f"   URL: {self.praja_server_url}")
            
            logger.info(
                f"📤 Sending support ticket to Praja server: "
                f"ticket_id={ticket.id} tenant_id={ticket.tenant.id if ticket.tenant else None} "
                f"url={self.praja_server_url}"
            )
            
            response = requests.post(
                self.praja_server_url,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                },
                timeout=30
            )
            
            print(f"📥 [PRAJA] Response Status: {response.status_code}")
            logger.info(
                f"📥 Praja server response: status={response.status_code} "
                f"ticket_id={ticket.id}"
            )
            
            if not response.ok:
                print(f"❌ [PRAJA] Error: Status {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                logger.error(
                    f"❌ Praja server error: status={response.status_code} "
                    f"response={response.text[:500]} ticket_id={ticket.id}"
                )
                return False
            
            print(f"✅ [PRAJA] Successfully sent support ticket {ticket.id} to Praja server!")
            logger.info(
                f"✅ Successfully sent support ticket to Praja server: "
                f"ticket_id={ticket.id}"
            )
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(
                f"❌ Error sending ticket to Praja server: {e} "
                f"ticket_id={ticket.id}"
            )
            return False
        except Exception as e:
            logger.exception(
                f"❌ Unexpected error sending ticket to Praja server: {e} "
                f"ticket_id={ticket.id}"
            )
            return False


logger = logging.getLogger(__name__)

def sync_entity_schema(tenant, entity_type, chunk_size=1000):
    """
    Scans new records and updates the EntityTypeSchema attributes (ArrayField) 
    with a list of all unique field names discovered.
    """
    entity_obj, created = EntityTypeSchema.objects.get_or_create(
        tenant=tenant,
        entity_type=entity_type
    )

    new_records = Record.objects.filter(
        tenant=tenant,
        entity_type=entity_type,
    ).order_by('-id')[:chunk_size]

    if not new_records.exists():
        return 0

    # 1. READ: Get the existing list of fields from the ArrayField (or start empty)
    # If the column is null, default to an empty list
    current_fields_list = entity_obj.attributes or []

    # Convert it to a Python 'set' for super fast duplicate checking
    unique_fields = set(current_fields_list)

    # 2. PROCESS: Add any brand new keys we find in the records
    for record in new_records:
        record_data = record.data or {}
        
        # We only care about the keys (field names), not the values or data types anymore!
        for key in record_data.keys():
            unique_fields.add(key)

    # 3. SAVE: Convert the set back to a standard Python list and save
    entity_obj.attributes = list(unique_fields)
    entity_obj.save()
    
    logger.info(f"Synced {len(new_records)} {entity_type} records for tenant {tenant.id}")
    
    return len(new_records)