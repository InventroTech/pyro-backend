#!/usr/bin/env python3
import os
import sys
import json
import logging
import time
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from pathlib import Path
import requests
import argparse
from supabase import create_client, Client

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    # Try to load from parent directory (where Django expects it)
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
except ImportError:
    pass  # dotenv not available, continue without it

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pyro_ticket_copy_supabase.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Staging users will be fetched dynamically from Supabase auth.users table

class SupabaseTicketCopier:
    def __init__(self, source_url: str, source_key: str, staging_url: str, staging_key: str):
        """
        Initialize the Supabase support ticket copier.
        
        Args:
            source_url: Supabase project URL for source
            source_key: Supabase anon key for source
            staging_url: Supabase project URL for staging
            staging_key: Supabase anon key for staging
        """
        self.source_url = source_url.rstrip('/')
        self.source_key = source_key
        self.staging_url = staging_url.rstrip('/')
        self.staging_key = staging_key
        
        # Default staging tenant ID
        self.staging_tenant_id = os.getenv('STAGING_TENANT_ID', 'e35e7279-d92d-4cdf-8014-98deaab639c0')
        
        # Get staging URL from environment variable if not provided
        if not self.staging_url:
            self.staging_url = os.getenv('STAGING_SUPABASE_URL')
            if not self.staging_url:
                raise ValueError("STAGING_SUPABASE_URL environment variable is required")
            self.staging_url = self.staging_url.rstrip('/')
        
        # Configuration
        self.batch_size = int(os.getenv('BATCH_SIZE', '1'))  # Use batch size of 1 for reliability
        
        # User mapping for round-robin assignment - will be populated dynamically
        self.staging_users = []
        self.staging_user_count = 0
        self.user_mappings = {}  # Cache for production_user_id -> staging_user mapping
        self.mapping_counter = 0  # Counter for round-robin
        self.max_retries = int(os.getenv('MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('RETRY_DELAY', '1.0'))
        
        # Fetch staging users dynamically
        self._fetch_staging_users()

    def _fetch_staging_users(self):
        """
        Fetch staging users dynamically from Supabase auth.users table.
        This replaces the hardcoded STAGING_USERS list.
        """
        try:
            logger.info("Fetching staging users dynamically from Supabase...")
            
            # Use auth.users endpoint to get user list
            url = f"{self.staging_url}/auth/v1/admin/users"
            
            # Need to use service role key for auth admin endpoints
            service_role_key = os.getenv('STAGING_SERVICE_ROLE_KEY')
            if not service_role_key:
                logger.error("STAGING_SERVICE_ROLE_KEY is required to fetch users from auth.users table")
                raise ValueError("STAGING_SERVICE_ROLE_KEY environment variable is required")
            
            headers = {
                'apikey': service_role_key,
                'Authorization': f'Bearer {service_role_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            users_data = response.json()
            users = users_data.get('users', []) if isinstance(users_data, dict) else users_data
            
            if not users:
                logger.warning("No users found in staging environment")
                # Fallback to a single default user if no users found
                self.staging_users = [{
                    'uid': '2e81a97e-c091-45a8-a7f4-213d00c6db7a',
                    'email': 'bibhab@thepyro.ai',
                    'display_name': 'Default User'
                }]
            else:
                # Transform user data to match expected format
                self.staging_users = []
                for user in users:
                    user_data = {
                        'uid': user.get('id'),
                        'email': user.get('email'),
                        'display_name': user.get('user_metadata', {}).get('full_name') or 
                                       user.get('raw_user_meta_data', {}).get('full_name') or
                                       user.get('email', '').split('@')[0]
                    }
                    
                    # Only include users with valid uid and email
                    if user_data['uid'] and user_data['email']:
                        self.staging_users.append(user_data)
                
                logger.info(f"Successfully fetched {len(self.staging_users)} staging users:")
                for user in self.staging_users:
                    logger.info(f"  - {user['display_name']} ({user['email']})")
            
            self.staging_user_count = len(self.staging_users)
            
            if self.staging_user_count == 0:
                raise ValueError("No valid staging users found")
                
        except Exception as e:
            logger.error(f"Failed to fetch staging users dynamically: {e}")
            logger.warning("Falling back to default user...")
            
            # Fallback to a single default user
            self.staging_users = [{
                'uid': '2e81a97e-c091-45a8-a7f4-213d00c6db7a',
                'email': 'bibhab@thepyro.ai',
                'display_name': 'Default User (Fallback)'
            }]
            self.staging_user_count = 1
            
            logger.info("Using fallback user for staging assignments")

    def get_staging_user_for_production_user(self, production_user_id: str, production_user_email: str = None) -> Optional[Dict]:
        """
        Get the staging user mapped to a production user using round-robin logic.
        
        Args:
            production_user_id: UUID of the production user
            production_user_email: Email of the production user (optional)
            
        Returns:
            Dict containing staging user info or None if not found
        """
        try:
            # Always use round-robin based on the counter, not caching by production user
            staging_user_index = self.mapping_counter % self.staging_user_count
            staging_user = self.staging_users[staging_user_index]
            
            # Increment counter for next assignment
            self.mapping_counter += 1
            
            logger.debug(f"Round-robin assignment #{self.mapping_counter}: {production_user_email or production_user_id} -> {staging_user['email']}")
            
            return staging_user
            
        except Exception as e:
            logger.error(f"Error getting staging user for production user {production_user_id}: {e}")
            return None

    def get_user_mapping_statistics(self) -> Dict:
        """
        Get statistics about staging user assignments.
        
        Returns:
            Dict with assignment counts per staging user
        """
        stats = {}
        total_assignments = self.mapping_counter
        
        for i, user in enumerate(self.staging_users):
            # Calculate how many assignments this user should have gotten
            # Each user gets assignments in cycles
            cycles = total_assignments // self.staging_user_count
            remainder = total_assignments % self.staging_user_count
            
            # Base count from complete cycles
            count = cycles
            
            # Add one more if this user is within the remainder
            if i < remainder:
                count += 1
            
            stats[user['email']] = {
                'uid': user['uid'],
                'display_name': user['display_name'],
                'assignment_count': count
            }
        return stats

    def update_existing_ticket_assignments(self, limit: int = 1000) -> Dict[str, Any]:
        """
        Update existing tickets in staging to use round-robin user mapping.
        This is useful for tickets that were copied before the round-robin logic was implemented.
        
        Args:
            limit: Maximum number of tickets to update
            
        Returns:
            Dict with update results
        """
        logger.info(f"Starting update of existing ticket assignments (limit: {limit})")
        
        try:
            # Fetch existing tickets from staging with pagination to handle large datasets
            all_tickets = []
            page_size = 1000  # Supabase max per request
            offset = 0
            
            while len(all_tickets) < limit:
                remaining = limit - len(all_tickets)
                current_page_size = min(page_size, remaining)
                
                url = f"{self.staging_url}/rest/v1/support_ticket?select=id,assigned_to,cse_name,user_id&limit={current_page_size}&offset={offset}&or=(assigned_to.not.is.null,cse_name.not.is.null)"
                page_tickets = self._make_request(url, self.staging_key, 'GET')
                
                if not page_tickets:
                    break  # No more tickets
                
                all_tickets.extend(page_tickets)
                offset += len(page_tickets)
                
                logger.info(f"Fetched {len(all_tickets)} tickets so far...")
                
                # If we got fewer tickets than requested, we've reached the end
                if len(page_tickets) < current_page_size:
                    break
            
            existing_tickets = all_tickets[:limit]  # Ensure we don't exceed the limit
            
            if not existing_tickets:
                logger.info("No existing tickets with assignments found")
                return {
                    "success": True,
                    "message": "No existing tickets to update",
                    "updated_count": 0
                }
            
            logger.info(f"Found {len(existing_tickets)} existing tickets with assignments")
            
            # Group tickets by their current assignment (either assigned_to or cse_name)
            tickets_by_current_user = {}
            for ticket in existing_tickets:
                # Check both assigned_to and cse_name fields
                current_assigned_to = ticket.get('assigned_to')
                current_cse_name = ticket.get('cse_name')
                
                # Use assigned_to if available, otherwise use cse_name
                current_assignment = current_assigned_to or current_cse_name
                
                if current_assignment not in tickets_by_current_user:
                    tickets_by_current_user[current_assignment] = []
                tickets_by_current_user[current_assignment].append(ticket)
            
            # Update tickets with new round-robin assignments
            updated_count = 0
            update_batch = []
            
            for ticket in existing_tickets:
                ticket_id = ticket['id']
                current_assigned_to = ticket.get('assigned_to')
                current_cse_name = ticket.get('cse_name')
                
                # Use assigned_to if available, otherwise use cse_name as the identifier
                current_assignment = current_assigned_to or current_cse_name
                
                # Get new assignment using round-robin logic
                new_staging_user = self.get_staging_user_for_production_user(
                    production_user_id=current_assignment,
                    production_user_email=None  # We don't have email info for existing tickets
                )
                
                if new_staging_user:
                    # Determine what fields to update based on what exists
                    update_data = {'id': ticket_id}
                    needs_update = False
                    
                    # Update assigned_to if it exists
                    if current_assigned_to:
                        new_assigned_to = new_staging_user['uid']
                        if new_assigned_to != current_assigned_to:
                            update_data['assigned_to'] = new_assigned_to
                            needs_update = True
                    
                    # Update cse_name if it exists
                    if current_cse_name:
                        new_cse_name = new_staging_user['display_name'] or new_staging_user['email']
                        if new_cse_name != current_cse_name:
                            update_data['cse_name'] = new_cse_name
                            needs_update = True
                    
                    # Only update if something actually changed
                    if needs_update:
                        update_batch.append(update_data)
                        
                        if len(update_batch) >= self.batch_size:
                            # Perform batch update
                            batch_updated = self._update_ticket_batch(update_batch)
                            updated_count += batch_updated
                            update_batch = []
                            logger.info(f"Updated batch: {batch_updated} tickets")
            
            # Update remaining tickets in the last batch
            if update_batch:
                batch_updated = self._update_ticket_batch(update_batch)
                updated_count += batch_updated
                logger.info(f"Updated final batch: {batch_updated} tickets")
            
            # Log statistics
            mapping_stats = self.get_user_mapping_statistics()
            logger.info("Updated ticket assignment statistics:")
            for email, stats in mapping_stats.items():
                logger.info(f"  {stats['display_name'] or 'No Name'} ({email}): {stats['assignment_count']} assignments")
            
            return {
                "success": True,
                "message": f"Successfully updated {updated_count} ticket assignments",
                "updated_count": updated_count,
                "user_mappings": mapping_stats
            }
            
        except Exception as e:
            logger.error(f"Error updating existing ticket assignments: {e}")
            return {
                "success": False,
                "message": f"Failed to update ticket assignments: {e}",
                "updated_count": 0
            }

    def _update_ticket_batch(self, update_batch: List[Dict]) -> int:
        """
        Update a batch of tickets in staging.
        
        Args:
            update_batch: List of ticket updates
            
        Returns:
            Number of successfully updated tickets
        """
        try:
            url = f"{self.staging_url}/rest/v1/support_ticket"
            
            # Use PATCH method to update multiple tickets
            # We'll update them one by one for reliability
            updated_count = 0
            
            for ticket_update in update_batch:
                ticket_id = ticket_update['id']
                
                # Prepare update data - exclude the 'id' field
                update_data = {k: v for k, v in ticket_update.items() if k != 'id'}
                
                try:
                    patch_url = f"{url}?id=eq.{ticket_id}"
                    response = self._make_request(patch_url, self.staging_key, 'PATCH', update_data)
                    updated_count += 1
                    
                except Exception as e:
                    logger.warning(f"Failed to update ticket {ticket_id}: {e}")
                    continue
            
            return updated_count
            
        except Exception as e:
            logger.error(f"Error updating ticket batch: {e}")
            return 0



    def _make_request(self, url: str, api_key: str, method: str = 'GET', data: Any = None) -> Any:
        """
        Make HTTP request to Supabase REST API.
        
        Args:
            url: Full URL to request
            api_key: Supabase anon key
            method: HTTP method
            data: Request data (for POST/PUT)
            
        Returns:
            Response data
            
        Raises:
            Exception: If request fails
        """
        headers = {
            'apikey': api_key,
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=representation'
        }
        
        # Use service role key for staging operations to bypass RLS
        # Get staging URL from environment variable instead of hardcoded value
        staging_env_url = os.getenv('STAGING_SUPABASE_URL', '')
        if staging_env_url and staging_env_url.rstrip('/') in url:
            # For staging, we need to use service role to bypass RLS
            service_role_key = os.getenv('STAGING_SERVICE_ROLE_KEY')
            if service_role_key:
                headers['Authorization'] = f'Bearer {service_role_key}'
                headers['apikey'] = service_role_key
            else:
                logger.warning("No STAGING_SERVICE_ROLE_KEY found. RLS might block operations.")
        
        for attempt in range(self.max_retries):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, timeout=30)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=data, timeout=30)
                elif method.upper() == 'PUT':
                    response = requests.put(url, headers=headers, json=data, timeout=30)
                elif method.upper() == 'PATCH':
                    response = requests.patch(url, headers=headers, json=data, timeout=30)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                
                # Handle empty responses
                if response.status_code == 204:
                    return None
                
                # Handle successful responses (200 or 201)
                if response.status_code in [200, 201]:
                    return response.json()
                
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                else:
                    raise Exception(f"Request failed after {self.max_retries} attempts: {e}")

    def fetch_tickets_from_source(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Fetch tickets from source Supabase project.
        
        Args:
            limit: Maximum number of tickets to fetch (None for all tickets)
            
        Returns:
            List of ticket data
        """
        all_tickets = []
        offset = 0
        page_size = 1000
        
        while True:
            if limit and len(all_tickets) >= limit:
                break
                
            current_limit = min(page_size, limit - len(all_tickets) if limit else page_size)
            url = f"{self.source_url}/rest/v1/support_ticket?select=*&limit={current_limit}&offset={offset}"
            
            try:
                data = self._make_request(url, self.source_key, 'GET')
                if not data or len(data) == 0:
                    break
                    
                all_tickets.extend(data)
                logger.info(f"Fetched {len(data)} tickets (offset: {offset})")
                
                if len(data) < current_limit:
                    break  # No more data
                    
                offset += len(data)
                
            except Exception as e:
                logger.error(f"Failed to fetch tickets from source: {e}")
                break
        
        logger.info(f"Total fetched: {len(all_tickets)} tickets from source")
        return all_tickets



    def fetch_tickets_from_staging(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Fetch tickets from staging Supabase project.
        
        Args:
            limit: Maximum number of tickets to fetch (None for all tickets)
            
        Returns:
            List of ticket data
        """
        all_tickets = []
        offset = 0
        page_size = 1000
        
        while True:
            if limit and len(all_tickets) >= limit:
                break
                
            current_limit = min(page_size, limit - len(all_tickets) if limit else page_size)
            url = f"{self.staging_url}/rest/v1/support_ticket?select=*&limit={current_limit}&offset={offset}"
            
            try:
                data = self._make_request(url, self.staging_key, 'GET')
                if not data or len(data) == 0:
                    break
                    
                all_tickets.extend(data)
                logger.info(f"Fetched {len(data)} tickets from staging (offset: {offset})")
                
                if len(data) < current_limit:
                    break  # No more data
                    
                offset += len(data)
                
            except Exception as e:
                logger.error(f"Failed to fetch tickets from staging: {e}")
                break
        
        logger.info(f"Total fetched: {len(all_tickets)} tickets from staging")
        return all_tickets

    def find_missing_tickets(self, source_tickets: List[Dict[str, Any]], staging_tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find tickets that exist in source but not in staging.
        
        Args:
            source_tickets: List of tickets from source
            staging_tickets: List of tickets from staging
            
        Returns:
            List of tickets that need to be copied
        """
        # Create sets of ticket IDs for comparison
        source_ids = {ticket.get('id') for ticket in source_tickets if ticket.get('id')}
        staging_ids = {ticket.get('id') for ticket in staging_tickets if ticket.get('id')}
        
        # Find missing IDs
        missing_ids = source_ids - staging_ids
        
        # Get the full ticket data for missing tickets
        missing_tickets = [ticket for ticket in source_tickets if ticket.get('id') in missing_ids]
        
        logger.info(f"Missing tickets: {len(missing_tickets)}")
        
        return missing_tickets

    def find_changed_tickets(self, source_tickets: List[Dict[str, Any]], staging_tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Find tickets that exist in both source and staging but have different data.
        
        Args:
            source_tickets: List of tickets from source
            staging_tickets: List of tickets from staging
            
        Returns:
            List of tickets that have changed and need updating
        """
        # Create a mapping of staging tickets by ID for quick lookup
        staging_by_id = {ticket.get('id'): ticket for ticket in staging_tickets if ticket.get('id')}
        
        changed_tickets = []
        changed_fields_summary = {}
        
        for source_ticket in source_tickets:
            ticket_id = source_ticket.get('id')
            if not ticket_id:
                continue
                
            staging_ticket = staging_by_id.get(ticket_id)
            if not staging_ticket:
                continue
            
            # Compare relevant fields (exclude system fields like created_at, updated_at, and fixed staging fields)
            fields_to_compare = [
                'name', 'reason', 'ticket_date', 'user_id', 'phone', 'source', 
                'subscription_status', 'badge', 'poster', 'layout_status', 
                'resolution_status', 'resolution_time', 'cse_name', 'cse_remarks', 
                'call_status', 'call_attempts', 'rm_name', 
                'completed_at', 'snooze_until', 'praja_dashboard_user_link', 
                'display_pic_url', 'dumped_at', 'atleast_paid_once', 'other_reasons'
                # Excluded: 'assigned_to' (fixed for staging), 'tenant_id' (fixed for staging)
            ]
            
            changed_fields = []
            for field in fields_to_compare:
                source_value = source_ticket.get(field)
                staging_value = staging_ticket.get(field)
                
                # Handle different data types and null values
                if source_value != staging_value:
                    # Special handling for snooze_until to debug the issue
                    if field == 'snooze_until':
                        logger.info(f"DEBUG - Ticket {ticket_id} snooze_until comparison:")
                        logger.info(f"  Source value: {source_value} (type: {type(source_value)})")
                        logger.info(f"  Staging value: {staging_value} (type: {type(staging_value)})")
                        logger.info(f"  Direct comparison (source_value != staging_value): {source_value != staging_value}")
                        logger.info(f"  Source is None: {source_value is None}")
                        logger.info(f"  Staging is None: {staging_value is None}")
                        logger.info(f"  Source is empty string: {source_value == ''}")
                        logger.info(f"  Staging is empty string: {staging_value == ''}")
                    
                    # Convert to string for comparison to handle different data types
                    source_str = str(source_value) if source_value is not None else ''
                    staging_str = str(staging_value) if staging_value is not None else ''
                    
                    if source_str != staging_str:
                        changed_fields.append(field)
                        # Track field changes for summary
                        if field not in changed_fields_summary:
                            changed_fields_summary[field] = 0
                        changed_fields_summary[field] += 1
            
            if changed_fields:
                # Create a ticket with only the changed fields
                changed_ticket = {'id': ticket_id}
                for field in changed_fields:
                    changed_ticket[field] = source_ticket.get(field)
                
                changed_tickets.append(changed_ticket)
                # Only log the first few tickets for debugging, not all
                if len(changed_tickets) <= 5:
                    logger.info(f"Ticket {ticket_id} changed fields: {', '.join(changed_fields)}")
        
        # Log summary of most commonly changed fields
        if changed_fields_summary:
            sorted_fields = sorted(changed_fields_summary.items(), key=lambda x: x[1], reverse=True)
            logger.info("Most commonly changed fields:")
            for field, count in sorted_fields[:10]:  # Show top 10
                logger.info(f"  {field}: {count} tickets")
        
        logger.info(f"Total tickets with changes: {len(changed_tickets)}")
        return changed_tickets

    def transform_ticket_data(self, tickets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform ticket data for staging environment.
        
        Args:
            tickets: Raw ticket data from source
            
        Returns:
            Transformed ticket data for staging
        """
        transformed_tickets = []
        
        for ticket in tickets:
            # Create a clean ticket with original ID from source
            transformed_ticket = {
                'id': ticket.get('id'),  # Use original ID from source
                'name': ticket.get('name', 'Unknown'),
                'reason': ticket.get('reason', 'No reason provided'),
                'atleast_paid_once': False
            }
            
            # Set tenant_id separately to ensure proper UUID format
            if self.staging_tenant_id:
                transformed_ticket['tenant_id'] = self.staging_tenant_id
            
            # Copy fields from source ticket if they exist (excluding tenant_id)
            field_mapping = {
                'ticket_date': 'ticket_date',
                'user_id': 'user_id',
                'phone': 'phone',
                'source': 'source',
                'subscription_status': 'subscription_status',
                'badge': 'badge',
                'poster': 'poster',
                'layout_status': 'layout_status',
                'resolution_status': 'resolution_status',
                'resolution_time': 'resolution_time',
                'cse_name': 'cse_name',
                'cse_remarks': 'cse_remarks',
                'call_status': 'call_status',
                'call_attempts': 'call_attempts',
                'assigned_to': 'assigned_to',
                'rm_name': 'rm_name',
                'completed_at': 'completed_at',
                'snooze_until': 'snooze_until',
                'praja_dashboard_user_link': 'praja_dashboard_user_link',
                'display_pic_url': 'display_pic_url',
                'dumped_at': 'dumped_at'
            }
            
            # Copy fields from source ticket
            for source_field, target_field in field_mapping.items():
                if source_field in ticket and ticket[source_field] is not None:
                    transformed_ticket[target_field] = ticket[source_field]
            
            # Handle atleast_paid_once specifically
            if 'atleast_paid_once' in ticket and ticket['atleast_paid_once'] is not None:
                transformed_ticket['atleast_paid_once'] = ticket['atleast_paid_once']
            
            # Handle other_reasons specifically (should be text array)
            if 'other_reasons' in ticket and ticket['other_reasons'] is not None:
                if isinstance(ticket['other_reasons'], list):
                    transformed_ticket['other_reasons'] = ticket['other_reasons']
                elif isinstance(ticket['other_reasons'], str):
                    try:
                        # Try to parse as JSON first
                        parsed = json.loads(ticket['other_reasons'])
                        if isinstance(parsed, list):
                            transformed_ticket['other_reasons'] = parsed
                        else:
                            transformed_ticket['other_reasons'] = [ticket['other_reasons']]
                    except:
                        transformed_ticket['other_reasons'] = [ticket['other_reasons']]
            # Don't set other_reasons if not present (let it be null)
            
            # Handle call_status specifically - only copy if present in source, don't set defaults
            # This prevents unnecessary updates when call_status is null in source
            
            # Map production user to staging user using round-robin logic
            staging_user = None
            
            # Handle assigned_to field
            if 'assigned_to' in transformed_ticket and transformed_ticket['assigned_to'] is not None:
                production_user_id = transformed_ticket['assigned_to']
                staging_user = self.get_staging_user_for_production_user(
                    production_user_id=production_user_id,
                    production_user_email=ticket.get('user_email')  # Assuming user_email exists in source data
                )
                
                if staging_user:
                    transformed_ticket['assigned_to'] = staging_user['uid']
                    logger.debug(f"Mapped production user {production_user_id} to staging user {staging_user['email']}")
                else:
                    # Fallback to first staging user if mapping fails
                    fallback_user = self.staging_users[0] if self.staging_users else None
                    if fallback_user:
                        transformed_ticket['assigned_to'] = fallback_user['uid']
                        logger.warning(f"Failed to map production user {production_user_id}, using fallback: {fallback_user['email']}")
                    else:
                        logger.error(f"No staging users available for fallback assignment")
                        # Remove assigned_to field if no users available
                        transformed_ticket.pop('assigned_to', None)
            
            # Handle cse_name field - also apply round-robin logic
            if 'cse_name' in transformed_ticket and transformed_ticket['cse_name'] is not None:
                # If we already have a staging_user from assigned_to mapping, use it
                # Otherwise, get a new staging user using round-robin
                if not staging_user:
                    staging_user = self.get_staging_user_for_production_user(
                        production_user_id=transformed_ticket.get('assigned_to', 'unknown'),
                        production_user_email=ticket.get('user_email')
                    )
                
                if staging_user:
                    # Use display_name if available, otherwise use email
                    transformed_ticket['cse_name'] = staging_user['display_name'] or staging_user['email']
                    logger.debug(f"Mapped cse_name to staging user {staging_user['email']}")
                else:
                    # Fallback to first staging user name
                    fallback_user = self.staging_users[0] if self.staging_users else None
                    if fallback_user:
                        transformed_ticket['cse_name'] = fallback_user['display_name'] or fallback_user['email']
                        logger.warning(f"Failed to map cse_name, using fallback: {fallback_user['email']}")
                    else:
                        logger.error(f"No staging users available for cse_name fallback")
                        # Remove cse_name field if no users available
                        transformed_ticket.pop('cse_name', None)
            
            transformed_tickets.append(transformed_ticket)
        
        logger.info(f"Transformed {len(transformed_tickets)} tickets")
        return transformed_tickets

    def _get_next_available_id(self) -> int:
        """
        Get the next available ID from the staging support_ticket table.
        
        Returns:
            Next available ID
        """
        try:
            url = f"{self.staging_url}/rest/v1/support_ticket?select=id&order=id.desc&limit=1"
            data = self._make_request(url, self.staging_key, 'GET')
            
            if data and len(data) > 0:
                max_id = data[0].get('id', 0)
                return max_id + 1
            else:
                return 1  # Start from 1 if table is empty
                
        except Exception as e:
            logger.warning(f"Failed to get next ID, using fallback: {e}")
            return 1000000  # Use a high number as fallback

    def insert_tickets_to_staging(self, tickets: List[Dict[str, Any]]) -> int:
        """
        Insert transformed tickets into staging support_ticket table.
        
        Args:
            tickets: List of transformed tickets
            
        Returns:
            Number of successfully inserted tickets
        """
        url = f"{self.staging_url}/rest/v1/support_ticket"
        
        # First, test if we can access the table
        try:
            test_response = self._make_request(f"{url}?limit=1", self.staging_key, 'GET')
            logger.info(f"Table access test successful: {len(test_response) if test_response else 0} records found")
        except Exception as e:
            logger.error(f"Table access test failed: {e}")
            return 0
        
        total_inserted = 0
        total_batches = (len(tickets) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(tickets), self.batch_size):
            batch = tickets[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch)} tickets)")
            
            try:
                # Log the first ticket in the batch for debugging
                if batch_num == 1 and len(batch) > 0:
                    logger.info(f"Sample ticket data being inserted: {json.dumps(batch[0], indent=2, default=str)}")
                
                data = self._make_request(url, self.staging_key, 'POST', batch)
                inserted_count = len(data) if isinstance(data, list) else 1
                total_inserted += inserted_count
                
                logger.info(f"Batch {batch_num}: Inserted {inserted_count}/{len(batch)} tickets")
                
            except Exception as e:
                logger.error(f"Failed to insert batch {batch_num}: {e}")
                # Log the error response if available
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response status: {e.response.status_code}")
                    logger.error(f"Response headers: {dict(e.response.headers)}")
                    try:
                        error_detail = e.response.json()
                        logger.error(f"Error details: {json.dumps(error_detail, indent=2)}")
                    except Exception as json_error:
                        logger.error(f"Error response text: {e.response.text}")
                        logger.error(f"JSON parse error: {json_error}")
                # Also log the actual data being sent for debugging
                if batch_num == 1 and len(batch) > 0:
                    logger.error(f"Failed data sample: {json.dumps(batch[0], indent=2, default=str)}")
                continue
        
        logger.info(f"Total inserted: {total_inserted}/{len(tickets)} tickets")
        return total_inserted

    def update_tickets_in_staging(self, tickets: List[Dict[str, Any]]) -> int:
        """
        Update existing tickets in staging support_ticket table.
        
        Args:
            tickets: List of transformed tickets to update
            
        Returns:
            Number of successfully updated tickets
        """
        total_updated = 0
        total_batches = (len(tickets) + self.batch_size - 1) // self.batch_size
        
        for i in range(0, len(tickets), self.batch_size):
            batch = tickets[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            
            logger.info(f"Processing update batch {batch_num}/{total_batches} ({len(batch)} tickets)")
            
            for ticket in batch:
                try:
                    ticket_id = ticket.get('id')
                    if not ticket_id:
                        logger.warning(f"Skipping ticket without ID: {ticket}")
                        continue
                    
                    # Remove ID and fixed staging fields from update data to avoid conflicts
                    fixed_staging_fields = ['id', 'assigned_to', 'tenant_id']
                    update_data = {k: v for k, v in ticket.items() if k not in fixed_staging_fields}
                    
                    url = f"{self.staging_url}/rest/v1/support_ticket?id=eq.{ticket_id}"
                    
                    # Log the first few tickets with their update data for debugging
                    if batch_num == 1 and batch.index(ticket) == 0:
                        logger.info(f"Sample ticket data being updated: {json.dumps(update_data, indent=2, default=str)}")
                    elif batch_num <= 3 and batch.index(ticket) < 3:  # Log first 3 batches, first 3 tickets each
                        logger.info(f"Updated ticket {ticket_id} with fields: {list(update_data.keys())}")
                    
                    data = self._make_request(url, self.staging_key, 'PATCH', update_data)
                    total_updated += 1
                    
                    # Only log every 100th ticket to avoid spam
                    if total_updated % 100 == 0:
                        logger.info(f"Updated ticket {ticket_id} (total: {total_updated})")
                    
                except Exception as e:
                    logger.error(f"Failed to update ticket {ticket.get('id', 'unknown')}: {e}")
                    continue
        
        logger.info(f"Total updated: {total_updated}/{len(tickets)} tickets")
        return total_updated

    def sync_tickets_with_staging(self, limit: int = 1000, check_missing: bool = False, update_existing: bool = False) -> Dict[str, Any]:
        """
        Main method to sync support tickets from source to staging.
        This function can both insert new tickets and update existing ones.
        
        Args:
            limit: Maximum number of tickets to sync
            check_missing: If True, only sync tickets that don't exist in staging
            update_existing: If True, also update existing tickets in staging
            
        Returns:
            Summary of the sync operation
        """
        logger.info("Starting support ticket sync process...")
        
        # Step 1: Fetch tickets from source
        source_tickets = self.fetch_tickets_from_source(limit)
        if not source_tickets:
            logger.warning("No tickets found in source")
            return {"success": False, "message": "No tickets found in source"}
        
        # Step 2: If checking for missing tickets or updating existing, fetch staging tickets and compare
        tickets_to_insert = []
        tickets_to_update = []
        
        if check_missing or update_existing:
            staging_tickets = self.fetch_tickets_from_staging(None)
            
            if check_missing:
                tickets_to_insert = self.find_missing_tickets(source_tickets, staging_tickets)
            
            if update_existing:
                # Find tickets that exist in both source and staging AND have changed
                tickets_to_update = self.find_changed_tickets(source_tickets, staging_tickets)
            
            if not tickets_to_insert and not tickets_to_update:
                logger.info("No tickets to sync")
                return {
                    "success": True,
                    "message": "No tickets to sync",
                    "source_count": len(source_tickets),
                    "inserted_count": 0,
                    "updated_count": 0
                }
        else:
            # If neither check_missing nor update_existing is specified, insert all tickets
            tickets_to_insert = source_tickets
        
        # Step 3: Transform tickets
        transformed_insert_tickets = self.transform_ticket_data(tickets_to_insert) if tickets_to_insert else []
        # Don't transform update tickets - they already contain only the changed fields
        transformed_update_tickets = tickets_to_update if tickets_to_update else []
        
        # Step 4: Insert new tickets to staging
        inserted_count = 0
        if transformed_insert_tickets:
            inserted_count = self.insert_tickets_to_staging(transformed_insert_tickets)
        
        # Step 5: Update existing tickets in staging
        updated_count = 0
        if transformed_update_tickets:
            updated_count = self.update_tickets_in_staging(transformed_update_tickets)
        
        # Step 6: Log user mapping statistics
        mapping_stats = self.get_user_mapping_statistics()
        logger.info("User mapping statistics:")
        for email, stats in mapping_stats.items():
            logger.info(f"  {stats['display_name'] or 'No Name'} ({email}): {stats['assignment_count']} assignments")
        
        # Step 7: Return summary
        total_processed = len(tickets_to_insert) + len(tickets_to_update)
        total_synced = inserted_count + updated_count
        success = total_synced > 0 or total_processed == 0
        
        message = f"Synced {total_synced}/{total_processed} tickets (inserted: {inserted_count}, updated: {updated_count})"
        
        logger.info(f"Sync process completed: {message}")
        
        return {
            "success": success,
            "message": message,
            "source_count": len(source_tickets),
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "user_mappings": mapping_stats
        }

    def copy_tickets(self, limit: int = 1000, check_missing: bool = False) -> Dict[str, Any]:
        """
        Main method to copy support tickets from source to staging.
        
        Args:
            limit: Maximum number of tickets to copy
            check_missing: If True, only copy tickets that don't exist in staging
            
        Returns:
            Summary of the copy operation
        """
        logger.info("Starting support ticket copy process...")
        
        # Step 1: Fetch tickets from source
        source_tickets = self.fetch_tickets_from_source(limit)
        if not source_tickets:
            logger.warning("No tickets found in source")
            return {"success": False, "message": "No tickets found in source"}
        
        # Step 2: If checking for missing tickets, fetch staging tickets and compare
        tickets_to_copy = source_tickets
        if check_missing:
            staging_tickets = self.fetch_tickets_from_staging(None)
            tickets_to_copy = self.find_missing_tickets(source_tickets, staging_tickets)
            
            if not tickets_to_copy:
                logger.info("No missing tickets found")
                return {
                    "success": True,
                    "message": "No missing tickets to copy",
                    "source_count": len(source_tickets),
                    "inserted_count": 0
                }
        
        # Step 3: Transform tickets
        transformed_tickets = self.transform_ticket_data(tickets_to_copy)
        
        # Step 4: Insert tickets to staging
        inserted_count = self.insert_tickets_to_staging(transformed_tickets)
        
        # Step 5: Return summary
        success = inserted_count > 0
        message = f"Copied {inserted_count}/{len(tickets_to_copy)} tickets"
        
        logger.info(f"Copy process completed: {message}")
        
        return {
            "success": success,
            "message": message,
            "source_count": len(source_tickets),
            "inserted_count": inserted_count
        }

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description='Sync support tickets from Pyro to staging Supabase')
    parser.add_argument('--source-url', help='Source Supabase project URL')
    parser.add_argument('--source-key', help='Source Supabase anon key')
    parser.add_argument('--staging-url', help='Staging Supabase project URL')
    parser.add_argument('--staging-key', help='Staging Supabase anon key')
    parser.add_argument('--staging-service-key', help='Staging Supabase service role key (required for bypassing RLS)')
    parser.add_argument('--limit', type=int, default=1000, help='Maximum tickets to sync')
    parser.add_argument('--check-missing', action='store_true', help='Only sync tickets that don\'t exist in staging')
    parser.add_argument('--update-existing', action='store_true', help='Also update existing tickets in staging')
    parser.add_argument('--update-assignments', action='store_true', help='Update existing ticket assignments with round-robin mapping')
    parser.add_argument('--test-users', action='store_true', help='Test fetching staging users (no ticket operations)')
    
    args = parser.parse_args()
    
    # Load configuration from environment variables
    source_url = args.source_url or os.getenv('SOURCE_SUPABASE_URL')
    source_key = args.source_key or os.getenv('SOURCE_SUPABASE_KEY')
    staging_url = args.staging_url or os.getenv('STAGING_SUPABASE_URL')
    staging_key = args.staging_key or os.getenv('STAGING_SUPABASE_KEY')
    staging_service_key = args.staging_service_key or os.getenv('STAGING_SERVICE_ROLE_KEY')
    
    # Validate required environment variables
    missing_vars = []
    if not source_url:
        missing_vars.append('SOURCE_SUPABASE_URL')
    if not source_key:
        missing_vars.append('SOURCE_SUPABASE_KEY')
    if not staging_url:
        missing_vars.append('STAGING_SUPABASE_URL')
    if not staging_key:
        missing_vars.append('STAGING_SUPABASE_KEY')
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set the following environment variables:")
        print("  SOURCE_SUPABASE_URL - Source Supabase project URL")
        print("  SOURCE_SUPABASE_KEY - Source Supabase anon key")
        print("  STAGING_SUPABASE_URL - Staging Supabase project URL")
        print("  STAGING_SUPABASE_KEY - Staging Supabase anon key")
        print("  STAGING_SERVICE_ROLE_KEY - Staging service role key (optional but recommended)")
        print("  STAGING_TENANT_ID - Staging tenant ID (optional, has default)")
        sys.exit(1)
    
    # Create copier instance
    copier = SupabaseTicketCopier(
        source_url=source_url,
        source_key=source_key,
        staging_url=staging_url,
        staging_key=staging_key
    )
    
    # Set service role key if provided
    if staging_service_key:
        os.environ['STAGING_SERVICE_ROLE_KEY'] = staging_service_key
    
    # Execute operation based on arguments
    if args.test_users:
        # Test user fetching functionality
        print("🔍 Testing staging user fetching...")
        print(f"✅ Successfully fetched {copier.staging_user_count} staging users:")
        print("=" * 60)
        
        for i, user in enumerate(copier.staging_users, 1):
            print(f"{i:2d}. {user['display_name']} ({user['email']})")
            print(f"    UID: {user['uid']}")
            print()
        
        # Test round-robin assignment
        print("🔄 Testing round-robin assignment (first 10 assignments):")
        print("=" * 60)
        
        # Reset counter for clean test
        copier.mapping_counter = 0
        
        for i in range(min(10, copier.staging_user_count * 2)):  # Test at least 2 full cycles
            test_user = copier.get_staging_user_for_production_user(f"test-user-{i}", f"test{i}@example.com")
            if test_user:
                print(f"Assignment #{i+1}: {test_user['display_name']} ({test_user['email']})")
            else:
                print(f"Assignment #{i+1}: FAILED")
        
        print(f"\n📊 Final Statistics:")
        stats = copier.get_user_mapping_statistics()
        for email, stat in stats.items():
            print(f"  {stat['display_name']} ({email}): {stat['assignment_count']} assignments")
        
        sys.exit(0)
    elif args.update_assignments:
        # Update existing ticket assignments with round-robin mapping
        result = copier.update_existing_ticket_assignments(args.limit)
    else:
        # Execute sync operation
        result = copier.sync_tickets_with_staging(args.limit, args.check_missing, args.update_existing)
    
    # Print result
    if result["success"]:
        print(f"✅ {result['message']}")
        
        # Print user mapping statistics
        if "user_mappings" in result:
            print("\n📊 User Mapping Statistics:")
            print("=" * 50)
            for email, stats in result["user_mappings"].items():
                display_name = stats['display_name'] or 'No Name'
                print(f"{display_name} ({email}): {stats['assignment_count']} assignments")
        
        sys.exit(0)
    else:
        print(f"❌ {result['message']}")
        sys.exit(1)

if __name__ == "__main__":
    main() 