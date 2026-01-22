import os
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
            # This matches the pattern used in support_ticket/utils.py
            user_id_for_api = int(user_id) if str(user_id).isdigit() else user_id
            
            # Custom API payload structure
            payload = {
                'user_id': user_id_for_api,
                'event_name': event_name,
                'properties': properties
            }
            
            logger.info(f"[Mixpanel] User ID format: {type(user_id_for_api).__name__} = {user_id_for_api}")
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.mixpanel_token}'
            }
            
            logger.info("=" * 80)
            logger.info("🎯 [MixpanelService] Sending event to custom Mixpanel API")
            logger.info(f"   URL: {self.mixpanel_api_url}")
            logger.info(f"   Event Name: {event_name}")
            logger.info(f"   User ID: {user_id} (formatted: {user_id_for_api})")
            logger.info(f"   Properties Count: {len(properties)}")
            logger.info(f"   Property Keys: {list(properties.keys())[:10]}{'...' if len(properties) > 10 else ''}")
            logger.info(f"   Token: {self.mixpanel_token[:10] if self.mixpanel_token else 'None'}...{self.mixpanel_token[-5:] if self.mixpanel_token else ''}")
            logger.info("   Payload:")
            logger.info(f"     - user_id: {user_id_for_api}")
            logger.info(f"     - event_name: {event_name}")
            logger.info(f"     - properties: {len(properties)} items")
            if 'lead_id' in properties:
                logger.info(f"     - lead_id: {properties.get('lead_id')}")
            if 'praja_id' in properties:
                logger.info(f"     - praja_id: {properties.get('praja_id')}")
            if 'rm_email' in properties:
                logger.info(f"     - rm_email: {properties.get('rm_email')}")
            logger.info("=" * 80)
            
            response = requests.post(
                self.mixpanel_api_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info("=" * 80)
            logger.info(f"📥 [MixpanelService] API Response Received")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Body: {response.text[:200]}{'...' if len(response.text) > 200 else ''}")
            logger.info("=" * 80)
            
            if not response.ok:
                logger.error("=" * 80)
                logger.error(f"❌ [MixpanelService] API Error")
                logger.error(f"   Status Code: {response.status_code}")
                logger.error(f"   Response: {response.text}")
                logger.error("=" * 80)
                return False
            
            logger.info("=" * 80)
            logger.info(f"✅ [MixpanelService] Event sent successfully!")
            logger.info(f"   Event: {event_name}")
            logger.info(f"   User ID: {user_id_for_api}")
            logger.info("=" * 80)
            return True
            
        except Exception as error:
            logger.error(f'❌ Error sending to custom Mixpanel API: {error}')
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
                'user_id': praja_id_int,  # praja_id is sent as user_id
                'rm_email': rm_email
            }
            
            logger.info("=" * 80)
            logger.info("🎯 [RMAssignedMixpanelService] Sending RM assigned event")
            logger.info(f"   URL: {self.mixpanel_api_url}")
            logger.info(f"   Praja ID: {praja_id_int} (sent as user_id in payload)")
            logger.info(f"   RM Email: {rm_email}")
            logger.info(f"   Token: {self.mixpanel_token[:10] if self.mixpanel_token else 'None'}...{self.mixpanel_token[-5:] if self.mixpanel_token else ''}")
            logger.info("   Payload:")
            logger.info(f"     - user_id: {praja_id_int} (this is the praja_id)")
            logger.info(f"     - rm_email: {rm_email}")
            logger.info("=" * 80)
            
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.mixpanel_token}'
            }
            
            response = requests.post(
                self.mixpanel_api_url,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info("=" * 80)
            logger.info(f"📥 [RMAssignedMixpanelService] API Response Received")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Body: {response.text[:200]}{'...' if len(response.text) > 200 else ''}")
            logger.info("=" * 80)
            
            if not response.ok:
                logger.error("=" * 80)
                logger.error(f"❌ [RMAssignedMixpanelService] API Error")
                logger.error(f"   Status Code: {response.status_code}")
                logger.error(f"   Response: {response.text}")
                logger.error("=" * 80)
                return False
            
            logger.info("=" * 80)
            logger.info(f"✅ [RMAssignedMixpanelService] Event sent successfully!")
            logger.info(f"   Praja ID: {praja_id_int}")
            logger.info(f"   RM Email: {rm_email}")
            logger.info("=" * 80)
            return True
            
        except Exception as error:
            logger.error(f'❌ Error sending to custom Mixpanel API: {error}')
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
