import logging
import os
import requests
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class MixpanelService:
    """
    Service for sending events to Mixpanel via the Pyro API endpoint
    """
    
    def __init__(self):
        self.mixpanel_api_url = "https://api.thecircleapp.in/pyro/send_to_mixpanel"
        self.mixpanel_token = os.environ.get("MIXPANEL_TOKEN")
    
    def send_to_mixpanel_sync(self, user_id: str, event_name: str, properties: Dict[str, Any]) -> bool:
        """
        Send event to Mixpanel - simple approach matching Edge function exactly
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return False
            
            payload = {
                'user_id': int(user_id),
                'event_name': event_name,
                'properties': properties
            }
            
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
            
            if not response.ok:
                logger.error(f"Mixpanel API error: {response.status_code} {response.text}")
                return False
            
            logger.info(f"Mixpanel event sent successfully: {event_name}")
            return True
            
        except Exception as error:
            logger.error(f'Error sending to Mixpanel: {error}')
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
