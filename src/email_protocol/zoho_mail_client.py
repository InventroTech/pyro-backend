"""
Thin Zoho Mail REST client (list inbox, fetch content).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import requests

from .zoho_oauth import ZohoOAuthError

logger = logging.getLogger(__name__)


class ZohoMailClient:
    def __init__(self, *, access_token: str, mail_api_base_url: str):
        self.access_token = (access_token or "").strip()
        self.base = (mail_api_base_url or "").rstrip("/")
        if not self.access_token:
            raise ZohoOAuthError("Missing Zoho access token.")
        if not self.base:
            raise ZohoOAuthError("Missing Zoho Mail API base URL.")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base}{path}"
        try:
            resp = requests.get(url, headers=self._headers(), params=params or {}, timeout=30)
        except requests.RequestException as exc:
            raise ZohoOAuthError(f"Zoho Mail request failed: {exc}") from exc
        try:
            payload = resp.json()
        except Exception as exc:
            raise ZohoOAuthError(f"Invalid Zoho Mail JSON ({resp.status_code})") from exc
        if resp.status_code >= 400:
            err = payload.get("data") or payload.get("error") or payload
            raise ZohoOAuthError(f"Zoho Mail API error {resp.status_code}: {err}")
        return payload.get("data", payload)

    def list_accounts(self) -> List[Dict[str, Any]]:
        data = self._get("/accounts")
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "accountId" in data:
            return [data]
        return []

    def list_folders(self, account_id: str) -> List[Dict[str, Any]]:
        data = self._get(f"/accounts/{account_id}/folders")
        if isinstance(data, list):
            return data
        return []

    def find_inbox_folder_id(self, account_id: str) -> Optional[str]:
        for folder in self.list_folders(account_id):
            ftype = str(folder.get("folderType") or "").lower()
            name = str(folder.get("folderName") or "").lower()
            if ftype == "inbox" or name == "inbox":
                fid = folder.get("folderId")
                return str(fid) if fid is not None else None
        return None

    def list_messages(
        self,
        *,
        account_id: str,
        folder_id: str,
        start: int = 1,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        data = self._get(
            f"/accounts/{account_id}/messages/view",
            params={
                "folderId": folder_id,
                "start": start,
                "limit": max(1, min(limit, 200)),
                "includeto": "true",
            },
        )
        if isinstance(data, list):
            return data
        return []

    def get_message_content(
        self,
        *,
        account_id: str,
        folder_id: str,
        message_id: str,
    ) -> Dict[str, Any]:
        data = self._get(
            f"/accounts/{account_id}/folders/{folder_id}/messages/{message_id}/content"
        )
        return data if isinstance(data, dict) else {"content": data}

    @staticmethod
    def pick_primary_account(accounts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not accounts:
            return None
        for acc in accounts:
            if acc.get("isDefault") or acc.get("primary") or str(acc.get("type", "")).lower() == "email":
                return acc
        return accounts[0]
