import os
import json
import requests
import logging
import base64
from typing import Any, Dict, Literal, Mapping, Optional

from support_ticket.constants import PRAJA_SAVE_SUPPORT_TICKET_URL, normalize_praja_ticket_status

RmAssignedSendResult = Literal["success", "skipped_not_found", "failed"]
CseAssignedSendResult = Literal[
    "success",
    "skipped_not_found",
    "skipped_invalid_user",
    "failed",
]

logger = logging.getLogger(__name__)


def _praja_ticket_type_from_record_data(data: Mapping[str, Any]) -> Optional[str]:
    """Snake_case ``support_ticket_type`` for Praja ``save_support_ticket``."""
    raw = data.get("support_ticket_type")
    if raw is None or str(raw).strip() == "":
        return None
    return (
        str(raw)
        .strip()
        .lower()
        .replace("-", "_")
        .replace(" ", "_")
    )


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
    
    def send_to_mixpanel_sync(self, praja_id: int, rm_email: str) -> RmAssignedSendResult:
        """
        Send RM assigned event to Mixpanel via custom API
        
        Args:
            praja_id: Praja ID as integer (will be sent as user_id in payload)
            rm_email: RM email address

        Returns:
            ``success`` on 2xx, ``skipped_not_found`` on 404 (Praja user missing in Circle),
            ``failed`` for other errors.
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return "failed"
            
            # Ensure praja_id is an integer
            praja_id_int = int(praja_id) if praja_id else None
            
            if not praja_id_int or not rm_email:
                logger.warning("praja_id and rm_email are required for RM assigned Mixpanel event")
                return "failed"
            
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
                if response.status_code == 404:
                    try:
                        error_data = response.json()
                        error_msg = error_data.get("message", "Unknown error")
                    except Exception:
                        error_msg = response.text[:200] if response.text else "Not found"
                    logger.warning(
                        "[Mixpanel] rm_assigned skipped (404 user not found): praja_id=%s rm_email=%s message=%s",
                        praja_id_int,
                        rm_email,
                        error_msg,
                    )
                    return "skipped_not_found"

                logger.error("=" * 80)
                logger.error(f"❌ [Mixpanel] Failed: rm_assigned status={response.status_code}")

                if response.status_code == 401:
                    logger.error(f"   Error: Unauthorized - Check MIXPANEL_TOKEN in .env file")
                    logger.error(f"   Token Preview: {self.mixpanel_token[:20]}...")
                elif response.status_code >= 500:
                    logger.error(f"   Error: Server Error ({response.status_code})")
                    logger.error(f"   This is a server-side issue with the Mixpanel API")
                else:
                    logger.error(f"   Error: HTTP {response.status_code}")
                    try:
                        error_data = response.json()
                        logger.error(f"   Details: {json.dumps(error_data, indent=2, default=str)}")
                    except Exception:
                        logger.error(f"   Response: {response.text[:200]}")

                logger.error("=" * 80)
                return "failed"

            logger.info(f"✅ [Mixpanel] Success: rm_assigned for praja_id={praja_id_int}")
            return "success"

        except Exception as error:
            logger.error(f'❌ [Mixpanel] Error: rm_assigned - {error}')
            logger.error(f"   Error Type: {type(error).__name__}")
            return "failed"


class CSEAssignedMixpanelService:
    """
    Service for sending CSE assigned events to Mixpanel via custom API
    """

    def __init__(self):
        self.mixpanel_token = os.environ.get("MIXPANEL_TOKEN")
        self.mixpanel_api_url = "https://api.thecircleapp.in/pyro/cse_assigned"

    def send_to_mixpanel_sync(self, user_id: int, cse_email: str) -> CseAssignedSendResult:
        """
        Send CSE assigned event to Mixpanel via custom API.

        Args:
            user_id: Customer user ID as integer (sent as user_id in payload)
            cse_email: CSE email address

        Returns:
            ``success`` on 2xx,
            ``skipped_not_found`` on 404 (user missing in Circle),
            ``skipped_invalid_user`` on 422 with ``{"error": "Invalid user"}``,
            ``failed`` for other errors.
        """
        try:
            if not self.mixpanel_token:
                logger.warning("MIXPANEL_TOKEN not configured, skipping Mixpanel event")
                return "failed"

            user_id_int = int(user_id) if user_id else None

            if not user_id_int or not cse_email:
                logger.warning("user_id and cse_email are required for CSE assigned Mixpanel event")
                return "failed"

            payload = {
                "user_id": user_id_int,
                "cse_email": cse_email,
            }

            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.mixpanel_token}",
            }

            logger.info("=" * 80)
            logger.info(f"🎯 [Mixpanel] Sending cse_assigned for user_id={user_id_int}")
            logger.info(f"   URL: {self.mixpanel_api_url}")
            logger.info(f"   User ID: {user_id_int}")
            logger.info(f"   CSE Email: {cse_email}")
            logger.info(
                f"   Token: {self.mixpanel_token[:15]}..."
                f"{self.mixpanel_token[-5:] if len(self.mixpanel_token) > 20 else ''}"
            )
            logger.info("=" * 80)
            logger.info("📤 [Mixpanel] Request Payload:")
            logger.info(f"   {json.dumps(payload, indent=2, default=str)}")
            logger.info("=" * 80)

            response = requests.post(
                self.mixpanel_api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )

            logger.info("=" * 80)
            logger.info("📥 [Mixpanel] Response for cse_assigned")
            logger.info(f"   Status Code: {response.status_code}")
            logger.info(f"   Response Headers: {dict(response.headers)}")

            try:
                response_json = response.json()
                logger.info(f"   Response Body: {json.dumps(response_json, indent=2, default=str)}")
            except Exception:
                response_json = None
                logger.info(
                    f"   Response Text: {response.text[:500]}"
                    f"{'...' if len(response.text) > 500 else ''}"
                )

            logger.info("=" * 80)

            if not response.ok:
                if response.status_code == 404:
                    try:
                        error_data = response_json if isinstance(response_json, dict) else response.json()
                        error_msg = error_data.get("message", "Unknown error")
                    except Exception:
                        error_msg = response.text[:200] if response.text else "Not found"
                    logger.warning(
                        "[Mixpanel] cse_assigned skipped (404 user not found): "
                        "user_id=%s cse_email=%s message=%s",
                        user_id_int,
                        cse_email,
                        error_msg,
                    )
                    return "skipped_not_found"

                if response.status_code == 422:
                    try:
                        error_data = response_json if isinstance(response_json, dict) else response.json()
                    except Exception:
                        error_data = {}
                    if isinstance(error_data, dict) and error_data.get("error") == "Invalid user":
                        logger.warning(
                            "[Mixpanel] cse_assigned skipped (422 invalid user): "
                            "user_id=%s cse_email=%s",
                            user_id_int,
                            cse_email,
                        )
                        return "skipped_invalid_user"

                logger.error("=" * 80)
                logger.error(f"❌ [Mixpanel] Failed: cse_assigned status={response.status_code}")

                if response.status_code == 401:
                    logger.error("   Error: Unauthorized - Check MIXPANEL_TOKEN in .env file")
                    logger.error(f"   Token Preview: {self.mixpanel_token[:20]}...")
                elif response.status_code >= 500:
                    logger.error(f"   Error: Server Error ({response.status_code})")
                    logger.error("   This is a server-side issue with the Mixpanel API")
                else:
                    logger.error(f"   Error: HTTP {response.status_code}")
                    try:
                        error_data = response_json if isinstance(response_json, dict) else response.json()
                        logger.error(f"   Details: {json.dumps(error_data, indent=2, default=str)}")
                    except Exception:
                        logger.error(f"   Response: {response.text[:200]}")

                logger.error("=" * 80)
                return "failed"

            logger.info(f"✅ [Mixpanel] Success: cse_assigned for user_id={user_id_int}")
            return "success"

        except Exception as error:
            logger.error(f"❌ [Mixpanel] Error: cse_assigned - {error}")
            logger.error(f"   Error Type: {type(error).__name__}")
            return "failed"


class SaveResolvedTicketPrajaService:
    """POST support ticket snapshot to Praja ``save_support_ticket``."""

    def __init__(self):
        from django.conf import settings

        self.api_url = getattr(settings, "PRAJA_API_URL", None) or os.environ.get(
            "PRAJA_API_URL",
            PRAJA_SAVE_SUPPORT_TICKET_URL,
        )

    def _request_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        token = os.environ.get("PRAJA_TOKEN", "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _coerce_api_id(self, value: Any) -> Any:
        return int(value) if str(value).isdigit() else value

    def build_payload(
        self,
        record,
        *,
        resolution_status: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        from support_ticket.records import (
            all_support_ticket_tasks_completed,
            resolution_status_from_latest_object_history,
        )

        data = record.data or {}
        user_id = data.get("user_id")
        if user_id is None:
            logger.warning(
                "[Praja] Skipping save_support_ticket — missing user_id "
                "record_id=%s",
                record.id,
            )
            return None

        ticket_type = _praja_ticket_type_from_record_data(data)
        if ticket_type is None:
            logger.warning(
                "[Praja] Skipping save_support_ticket — missing support_ticket_type "
                "record_id=%s",
                record.id,
            )
            return None

        resolution = (
            resolution_status
            or resolution_status_from_latest_object_history(record)
        )
        ticket_status = normalize_praja_ticket_status(resolution) if resolution else ""
        payload = {
            "user_id": self._coerce_api_id(user_id),
            "ticket_id": self._coerce_api_id(record.id),
            "ticket_type": ticket_type,
            "ticket_status": ticket_status,
            "all_tasks_completed": all_support_ticket_tasks_completed(data),
        }
        if ticket_status == "OPEN":
            logger.info(
                "[Praja] Built OPEN save_support_ticket payload record_id=%s "
                "user_id=%s ticket_id=%s ticket_type=%s resolution_status=%r "
                "all_tasks_completed=%s",
                record.id,
                payload["user_id"],
                payload["ticket_id"],
                payload["ticket_type"],
                resolution,
                payload["all_tasks_completed"],
            )
        return payload

    def save_resolved_ticket(
        self,
        user_id,
        ticket_id,
        ticket_type,
        ticket_status,
        all_tasks_completed,
    ) -> bool:
        payload = {
            "user_id": self._coerce_api_id(user_id),
            "ticket_id": self._coerce_api_id(ticket_id),
            "ticket_type": str(ticket_type).strip().lower().replace("-", "_").replace(" ", "_"),
            "ticket_status": normalize_praja_ticket_status(ticket_status),
            "all_tasks_completed": bool(all_tasks_completed),
        }
        headers = self._request_headers()
        is_open = str(payload.get("ticket_status") or "").upper() == "OPEN"

        try:
            if is_open:
                logger.info(
                    "[Praja] POST OPEN save_support_ticket url=%s "
                    "user_id=%s ticket_id=%s ticket_type=%s ticket_status=%s "
                    "all_tasks_completed=%s payload=%s",
                    self.api_url,
                    payload.get("user_id"),
                    payload.get("ticket_id"),
                    payload.get("ticket_type"),
                    payload.get("ticket_status"),
                    payload.get("all_tasks_completed"),
                    json.dumps(payload, default=str),
                )
            else:
                logger.info(
                    "[Praja] POST save_support_ticket url=%s payload=%s",
                    self.api_url,
                    json.dumps(payload, default=str),
                )
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )
            if not response.ok:
                logger.error(
                    "[Praja] save_support_ticket failed ticket_status=%s "
                    "ticket_id=%s http_status=%s body=%s",
                    payload.get("ticket_status"),
                    payload.get("ticket_id"),
                    response.status_code,
                    response.text[:500],
                )
                return False
            if is_open:
                logger.info(
                    "[Praja] OPEN save_support_ticket success "
                    "user_id=%s ticket_id=%s ticket_type=%s",
                    payload.get("user_id"),
                    payload.get("ticket_id"),
                    payload.get("ticket_type"),
                )
            else:
                logger.info(
                    "[Praja] save_support_ticket success ticket_id=%s ticket_status=%s",
                    payload.get("ticket_id"),
                    payload.get("ticket_status"),
                )
            return True
        except requests.exceptions.RequestException as exc:
            logger.error(
                "[Praja] save_support_ticket error ticket_status=%s ticket_id=%s: %s",
                payload.get("ticket_status"),
                payload.get("ticket_id"),
                exc,
            )
            return False

    def save_record(self, record) -> bool:
        payload = self.build_payload(record)
        if not payload:
            return False
        return self.save_resolved_ticket(
            user_id=payload["user_id"],
            ticket_id=payload["ticket_id"],
            ticket_type=payload["ticket_type"],
            ticket_status=payload["ticket_status"],
            all_tasks_completed=payload["all_tasks_completed"],
        )


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
