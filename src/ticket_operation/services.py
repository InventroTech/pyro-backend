import logging
import os
import requests
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class MixpanelService:
    """
    Service for sending events to Mixpanel via the working Edge function
    """
    
    def __init__(self):
        # Use the working Edge function instead of calling Mixpanel directly
        self.edge_function_url = os.environ.get("SUPABASE_EDGE_FUNCTION_URL", "https://hihrftwrriygnbrsvlrr.supabase.co/functions/v1/save-and-continue")
        self.mixpanel_token = os.environ.get("MIXPANEL_TOKEN")
    
    def send_to_mixpanel_sync(self, user_id: str, event_name: str, properties: Dict[str, Any]) -> bool:
        """
        Send event to Mixpanel - EXACTLY like the working Edge function
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return False
            
            # EXACT payload structure from working Edge function
            payload = {
                'user_id': int(user_id),  # parseInt(userId) in Edge function
                'event_name': event_name,
                'properties': properties
            }
            
            # EXACT headers from working Edge function
            headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.mixpanel_token}'
            }
            
            logger.info(f"🎯 Calling Mixpanel API exactly like working Edge function")
            logger.info(f"  - URL: https://api.thecircleapp.in/pyro/send_to_mixpanel")
            logger.info(f"  - Event: {event_name}")
            logger.info(f"  - User ID: {user_id}")
            logger.info(f"  - Token: {self.mixpanel_token[:10]}...{self.mixpanel_token[-5:]}")
            
            response = requests.post(
                "https://api.thecircleapp.in/pyro/send_to_mixpanel",  # Exact URL from Edge function
                json=payload,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"📥 Mixpanel API Response: {response.status_code}")
            logger.info(f"📥 Response body: {response.text}")
            
            if not response.ok:
                logger.error(f"❌ Mixpanel API error: {response.status_code} {response.text}")
                return False
            
            logger.info(f"✅ Mixpanel event sent successfully: {event_name}")
            return True
            
        except Exception as error:
            logger.error(f'❌ Error sending to Mixpanel: {error}')
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
