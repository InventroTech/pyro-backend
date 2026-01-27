import os
import json
import requests
import logging
import base64
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class MixpanelService:
    """
    Service for sending events to Mixpanel via custom API
    """
    
    def __init__(self):
        self.mixpanel_token = os.environ.get("MIXPANEL_TOKEN")
        self.mixpanel_api_url = "https://api.thecircleapp.in/pyro/send_to_mixpanel"
    
    def send_to_mixpanel_sync(self, user_id: str, event_name: str, properties: Dict[str, Any]) -> bool:
        """
        Send event to Mixpanel via custom API
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return False
            
            # Handle user_id - convert to int if numeric, otherwise keep as string (for UUIDs)
            user_id_for_api = int(user_id) if str(user_id).isdigit() else user_id
            
            # Custom API payload structure
            payload = {
                'user_id': user_id_for_api,
                'event_name': event_name,
                'properties': properties
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.mixpanel_token}'
            }
            
            # Detailed logging
            logger.info("=" * 80)
            logger.info(f"🎯 [Mixpanel] Sending {event_name} for user_id={user_id_for_api}")
            logger.info(f"   URL: {self.mixpanel_api_url}")
            logger.info(f"   Event: {event_name}")
            logger.info(f"   User ID: {user_id} → {user_id_for_api} ({type(user_id_for_api).__name__})")
            logger.info(f"   Properties Count: {len(properties)}")
            
            # Log key properties
            if 'lead_id' in properties:
                logger.info(f"   Lead ID: {properties.get('lead_id')}")
            if 'ticket_id' in properties:
                logger.info(f"   Ticket ID: {properties.get('ticket_id')}")
            if 'praja_id' in properties:
                logger.info(f"   Praja ID: {properties.get('praja_id')}")
            if 'name' in properties:
                logger.info(f"   Name: {properties.get('name')}")
            if 'phone_number' in properties or 'phone' in properties:
                phone = properties.get('phone_number') or properties.get('phone')
                logger.info(f"   Phone: {phone}")
            if 'tenant_id' in properties:
                logger.info(f"   Tenant ID: {properties.get('tenant_id')}")
            
            logger.info(f"   Token: {self.mixpanel_token[:15]}...{self.mixpanel_token[-5:] if len(self.mixpanel_token) > 20 else ''}")
            logger.info("=" * 80)
            
            # Log full payload for debugging
            logger.info("=" * 80)
            logger.info(f"📤 [Mixpanel] Request Payload:")
            logger.info(f"   {json.dumps(payload, indent=2, default=str)}")
            logger.info("=" * 80)
            
            response = requests.post(
                self.mixpanel_api_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info("=" * 80)
            logger.info(f"📥 [Mixpanel] Response for {event_name}")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Headers: {dict(response.headers)}")
            
            # Try to parse JSON response
            try:
                response_json = response.json()
                logger.info(f"   Response Body: {json.dumps(response_json, indent=2, default=str)}")
            except:
                logger.info(f"   Response Text: {response.text[:500]}{'...' if len(response.text) > 500 else ''}")
            
            logger.info("=" * 80)
            
            if not response.ok:
                logger.error("=" * 80)
                logger.error(f"❌ [Mixpanel] Failed: {event_name} status={response.status_code}")
                
                # Detailed error handling
                if response.status_code == 401:
                    logger.error(f"   Error: Unauthorized - Check MIXPANEL_TOKEN in .env file")
                    logger.error(f"   Token Preview: {self.mixpanel_token[:20]}...")
                elif response.status_code == 404:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', 'Unknown error')
                        logger.error(f"   Error: User Not Found (404)")
                        logger.error(f"   Message: {error_msg}")
                        logger.error(f"   User ID Sent: {user_id_for_api} ({type(user_id_for_api).__name__})")
                        logger.error(f"   Note: This may be expected for new leads/users that don't exist in Mixpanel yet")
                    except:
                        logger.error(f"   Error: 404 Not Found - {response.text[:200]}")
                elif response.status_code >= 500:
                    logger.error(f"   Error: Server Error ({response.status_code})")
                    logger.error(f"   This is a server-side issue with the Mixpanel API")
                else:
                    logger.error(f"   Error: HTTP {response.status_code}")
                    try:
                        error_data = response.json()
                        logger.error(f"   Details: {json.dumps(error_data, indent=2, default=str)}")
                    except:
                        logger.error(f"   Response: {response.text[:200]}")
                
                logger.error("=" * 80)
                return False
            
            logger.info(f"✅ [Mixpanel] Success: {event_name} for user_id={user_id_for_api}")
            return True
            
        except Exception as error:
            logger.error(f'❌ [Mixpanel] Error: {event_name} - {error}')
            logger.error(f"   Error Type: {type(error).__name__}")
            return False
    


class RMAssignedMixpanelService:
    """
    Service for sending RM assigned events to Mixpanel via custom API
    """
    
    def __init__(self):
        self.mixpanel_token = os.environ.get("MIXPANEL_TOKEN")
        self.mixpanel_api_url = "https://api.thecircleapp.in/pyro/rm_assigned"
    
    def send_to_mixpanel_sync(self, praja_id: int, rm_email: str) -> bool:
        """
        Send RM assigned event to Mixpanel via custom API
        
        Args:
            praja_id: Praja ID as integer (will be sent as user_id in payload)
            rm_email: RM email address
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return False
            
            # Ensure praja_id is an integer
            praja_id_int = int(praja_id) if praja_id else None
            
            if not praja_id_int or not rm_email:
                logger.warning("praja_id and rm_email are required for RM assigned Mixpanel event")
                return False
            
            # Payload structure - send praja_id as user_id
            payload = {
                'user_id': praja_id_int,
                'rm_email': rm_email
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.mixpanel_token}'
            }
            
            # Detailed logging
            logger.info("=" * 80)
            logger.info(f"🎯 [Mixpanel] Sending rm_assigned for praja_id={praja_id_int}")
            logger.info(f"   URL: {self.mixpanel_api_url}")
            logger.info(f"   Praja ID: {praja_id_int} (sent as user_id)")
            logger.info(f"   RM Email: {rm_email}")
            logger.info(f"   Token: {self.mixpanel_token[:15]}...{self.mixpanel_token[-5:] if len(self.mixpanel_token) > 20 else ''}")
            logger.info("=" * 80)
            
            # Log full payload for debugging
            logger.info("=" * 80)
            logger.info(f"📤 [Mixpanel] Request Payload:")
            logger.info(f"   {json.dumps(payload, indent=2, default=str)}")
            logger.info("=" * 80)
            
            response = requests.post(
                self.mixpanel_api_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info("=" * 80)
            logger.info(f"📥 [Mixpanel] Response for rm_assigned")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Headers: {dict(response.headers)}")
            
            # Try to parse JSON response
            try:
                response_json = response.json()
                logger.info(f"   Response Body: {json.dumps(response_json, indent=2, default=str)}")
            except:
                logger.info(f"   Response Text: {response.text[:500]}{'...' if len(response.text) > 500 else ''}")
            
            logger.info("=" * 80)
            
            if not response.ok:
                logger.error("=" * 80)
                logger.error(f"❌ [Mixpanel] Failed: rm_assigned status={response.status_code}")
                
                # Detailed error handling
                if response.status_code == 401:
                    logger.error(f"   Error: Unauthorized - Check MIXPANEL_TOKEN in .env file")
                    logger.error(f"   Token Preview: {self.mixpanel_token[:20]}...")
                elif response.status_code == 404:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('message', 'Unknown error')
                        logger.error(f"   Error: User Not Found (404)")
                        logger.error(f"   Message: {error_msg}")
                        logger.error(f"   Praja ID Sent: {praja_id_int}")
                        logger.error(f"   Note: User may not exist in Mixpanel yet")
                    except:
                        logger.error(f"   Error: 404 Not Found - {response.text[:200]}")
                elif response.status_code >= 500:
                    logger.error(f"   Error: Server Error ({response.status_code})")
                    logger.error(f"   This is a server-side issue with the Mixpanel API")
                else:
                    logger.error(f"   Error: HTTP {response.status_code}")
                    try:
                        error_data = response.json()
                        logger.error(f"   Details: {json.dumps(error_data, indent=2, default=str)}")
                    except:
                        logger.error(f"   Response: {response.text[:200]}")
                
                logger.error("=" * 80)
                return False
            
            logger.info(f"✅ [Mixpanel] Success: rm_assigned for praja_id={praja_id_int}")
            return True
            
        except Exception as error:
            logger.error(f'❌ [Mixpanel] Error: rm_assigned - {error}')
            logger.error(f"   Error Type: {type(error).__name__}")
            return False


class TicketTimeService:
    """
    Service for handling ticket resolution time calculations
    """
    
    @staticmethod
    def add_time_strings(time1: Optional[str], time2: Optional[str]) -> str:
        """
        Add two MM:SS time strings directly
        
        Args:
            time1: First time string in MM:SS format
            time2: Second time string in MM:SS format
            
        Returns:
            str: Sum of both times in MM:SS format
        """
        if not time1 or ":" not in time1:
            time1 = "0:00"
        if not time2 or ":" not in time2:
            time2 = "0:00"
        
        # Parse both times
        parts1 = time1.split(":")
        parts2 = time2.split(":")
        
        total_minutes = 0
        total_seconds = 0
        
        if len(parts1) == 2:
            total_minutes += int(parts1[0]) or 0
            total_seconds += int(parts1[1]) or 0
        
        if len(parts2) == 2:
            total_minutes += int(parts2[0]) or 0
            total_seconds += int(parts2[1]) or 0
        
        # Handle carryover
        total_minutes += total_seconds // 60
        total_seconds = total_seconds % 60
        
        # Format result
        return f"{total_minutes}:{total_seconds:02d}"
