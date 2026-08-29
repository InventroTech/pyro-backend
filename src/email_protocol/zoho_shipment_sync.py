"""
Sync Zoho Mail ops inbox → auto-fill inventory_request / unmannd_request tracking fields.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from django.db.models import Q
from django.utils import timezone

from crm_records.inventory_shipment_tracking import apply_shipment_tracking_normalization
from crm_records.models import Record
from crm_records.shipment_email_parse import parse_shipment_email

from .models import ZohoMailConnection, ZohoMailProcessedMessage
from .zoho_mail_client import ZohoMailClient
from .zoho_oauth import ZohoOAuthError, refresh_access_token, token_expiry_from_payload

logger = logging.getLogger(__name__)

ENTITY_TYPES = ("inventory_request", "unmannd_request")
ELIGIBLE_STATUSES = ("IN_SHIPPING", "ORDERED", "VENDOR_IDENTIFIED", "APPROVED")
ITEM_NAME_FIELDS = (
    "item_name_freeform",
    "item_name",
    "part_number_or_sku",
    "product_name",
)
_MIN_ITEM_NAME_LEN = 4
_MARKETPLACE_SUFFIX_RE = re.compile(
    r"\s*[:|\-–—]\s*(amazon\.in|amazon\.com|flipkart\.com|myntra\.com|"
    r"toys\s*&\s*games|industrial\s*&\s*scientific).*$",
    re.I,
)


def _normalize_item_text(value: str) -> str:
    text = (value or "").lower()
    text = _MARKETPLACE_SUFFIX_RE.sub("", text)
    text = re.sub(r"[^\w\s+./-]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _record_item_names(data: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for field in ITEM_NAME_FIELDS:
        raw = str(data.get(field) or "").strip()
        if not raw:
            continue
        for candidate in (raw, _MARKETPLACE_SUFFIX_RE.sub("", raw).strip()):
            norm = _normalize_item_text(candidate)
            if len(norm) < _MIN_ITEM_NAME_LEN or norm in seen:
                continue
            seen.add(norm)
            names.append(norm)
    return names


def ensure_fresh_access_token(connection: ZohoMailConnection) -> str:
    """Refresh Zoho access token if missing/expired; persist on connection."""
    now = timezone.now()
    if (
        connection.access_token
        and connection.access_token_expires_at
        and connection.access_token_expires_at > now
    ):
        return connection.access_token

    payload = refresh_access_token(
        refresh_token=connection.refresh_token,
        accounts_base_url=connection.accounts_base_url,
    )
    connection.access_token = payload["access_token"]
    connection.access_token_expires_at = token_expiry_from_payload(payload)
    connection.save(update_fields=["access_token", "access_token_expires_at", "updated_at"])
    return connection.access_token


def ensure_account_and_inbox(connection: ZohoMailConnection, client: ZohoMailClient) -> None:
    changed = False
    if not connection.account_id:
        accounts = client.list_accounts()
        primary = ZohoMailClient.pick_primary_account(accounts)
        if not primary:
            raise ZohoOAuthError("No Zoho Mail account found for this connection.")
        connection.account_id = str(primary.get("accountId") or primary.get("account_id") or "")
        email = (
            primary.get("mailboxAddress")
            or primary.get("emailAddress")
            or primary.get("primaryEmailAddress")
            or ""
        )
        if email:
            connection.email_address = str(email)
        if not connection.account_id:
            raise ZohoOAuthError("Zoho accountId missing from accounts response.")
        changed = True

    if not connection.inbox_folder_id:
        folder_id = client.find_inbox_folder_id(connection.account_id)
        if not folder_id:
            raise ZohoOAuthError("Could not find Zoho Inbox folder.")
        connection.inbox_folder_id = folder_id
        changed = True

    if changed:
        connection.save(
            update_fields=[
                "account_id",
                "email_address",
                "inbox_folder_id",
                "updated_at",
            ]
        )


def _candidate_records(tenant_id) -> List[Record]:
    """Open inventory requests that still need tracking filled."""
    status_q = Q()
    for status in ELIGIBLE_STATUSES:
        status_q |= Q(data__status__iexact=status)

    qs = (
        Record.objects.filter(tenant_id=tenant_id, entity_type__in=ENTITY_TYPES)
        .filter(status_q)
        .order_by("-updated_at")[:300]
    )
    out: List[Record] = []
    for rec in qs:
        data = rec.data if isinstance(rec.data, dict) else {}
        number = str(data.get("tracking_number") or "").strip()
        link = str(data.get("tracking_link") or "").strip()
        if number or link:
            continue
        out.append(rec)
        if len(out) >= 200:
            break
    return out


def match_record_for_email(
    *,
    tenant_id,
    parsed: Dict[str, Any],
    candidates: Optional[List[Record]] = None,
) -> Tuple[Optional[Record], str]:
    """
    Return (record, reason).

    Match only by item name: the request's item name must appear in the email
    subject/body. Longer names win when several candidates match substrings
    (e.g. "Drone with Dual 4K Camera" beats "Drone"). Ambiguous ties skip.
    """
    if not (parsed.get("tracking_number") or parsed.get("tracking_link")):
        return None, "no_tracking_payload"

    pool = candidates if candidates is not None else _candidate_records(tenant_id)
    email_blob = _normalize_item_text(
        str(parsed.get("email_text") or parsed.get("subject") or "")
    )
    if not email_blob:
        return None, "no_item_match"

    scored: List[Tuple[int, Record]] = []
    for rec in pool:
        data = rec.data if isinstance(rec.data, dict) else {}
        best_len = 0
        for name in _record_item_names(data):
            if name in email_blob:
                best_len = max(best_len, len(name))
        if best_len:
            scored.append((best_len, rec))

    if not scored:
        return None, "no_item_match"

    max_len = max(length for length, _ in scored)
    winners = {rec.id: rec for length, rec in scored if length == max_len}
    if len(winners) == 1:
        return next(iter(winners.values())), "item_name"
    return None, "ambiguous_item_name"


def apply_tracking_to_record(record: Record, parsed: Dict[str, Any]) -> bool:
    """Write tracking fields if empty; return True when saved."""
    data = dict(record.data) if isinstance(record.data, dict) else {}
    previous = dict(data)

    number = str(data.get("tracking_number") or "").strip()
    link = str(data.get("tracking_link") or "").strip()
    if not number and parsed.get("tracking_number"):
        data["tracking_number"] = parsed["tracking_number"]
    if not link and parsed.get("tracking_link"):
        data["tracking_link"] = parsed["tracking_link"]
    if not str(data.get("courier_name") or "").strip() and parsed.get("courier_name"):
        data["courier_name"] = parsed["courier_name"]
    if not str(data.get("eta") or "").strip() and parsed.get("eta"):
        data["eta"] = str(parsed["eta"])[:32]
    if not str(data.get("shipment_status") or "").strip():
        data["shipment_status"] = "ORDERED"

    apply_shipment_tracking_normalization(data, previous=previous)

    if data == previous:
        return False

    record.data = data
    record.save(update_fields=["data", "updated_at"])
    return True


def sync_zoho_shipment_emails(
    connection: ZohoMailConnection,
    *,
    max_messages: int = 40,
) -> Dict[str, Any]:
    """
    Poll Zoho inbox for recent shipment emails and auto-fill matching records.
    """
    if not connection.is_active:
        return {"success": True, "skipped": "inactive"}

    access_token = ensure_fresh_access_token(connection)
    client = ZohoMailClient(
        access_token=access_token,
        mail_api_base_url=connection.mail_api_base_url,
    )
    ensure_account_and_inbox(connection, client)

    messages = client.list_messages(
        account_id=connection.account_id,
        folder_id=connection.inbox_folder_id,
        start=1,
        limit=max_messages,
    )

    cursor = connection.last_received_time_ms
    newest_seen = cursor
    candidates = _candidate_records(connection.tenant_id)

    scanned = 0
    shipment_like = 0
    applied = 0
    unmatched = 0
    skipped = 0
    errors = 0

    for msg in messages:
        message_id = str(msg.get("messageId") or msg.get("message_id") or "").strip()
        if not message_id:
            continue

        received_raw = msg.get("receivedTime") or msg.get("sentDateInGMT") or 0
        try:
            received_ms = int(received_raw)
        except (TypeError, ValueError):
            received_ms = 0

        if cursor and received_ms and received_ms <= cursor:
            skipped += 1
            continue

        if ZohoMailProcessedMessage.objects.filter(
            connection=connection, message_id=message_id
        ).exists():
            skipped += 1
            continue

        scanned += 1
        subject = str(msg.get("subject") or "")
        from_address = str(
            msg.get("fromAddress")
            or msg.get("sender")
            or msg.get("from")
            or msg.get("fromAddr")
            or ""
        )
        folder_id = str(msg.get("folderId") or connection.inbox_folder_id)

        try:
            content_payload = client.get_message_content(
                account_id=connection.account_id,
                folder_id=folder_id,
                message_id=message_id,
            )
            body = (
                content_payload.get("content")
                or content_payload.get("message")
                or content_payload.get("html")
                or msg.get("summary")
                or ""
            )
            # Content endpoint sometimes carries From when list view does not.
            if not from_address:
                from_address = str(
                    content_payload.get("fromAddress")
                    or content_payload.get("sender")
                    or content_payload.get("from")
                    or ""
                )
            parsed = parse_shipment_email(
                subject=subject,
                html_or_text=str(body),
                from_address=from_address,
            )
        except Exception:
            errors += 1
            logger.exception(
                "[ZohoShipmentSync] failed reading message_id=%s tenant=%s",
                message_id,
                connection.tenant_id,
            )
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason="read_error",
            )
            continue

        if not parsed.get("is_shipment"):
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason="not_delivery_partner",
            )
            if received_ms and (newest_seen is None or received_ms > newest_seen):
                newest_seen = received_ms
            continue

        shipment_like += 1
        if not (parsed.get("tracking_number") or parsed.get("tracking_link")):
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason="no_tracking_payload",
            )
            unmatched += 1
            if received_ms and (newest_seen is None or received_ms > newest_seen):
                newest_seen = received_ms
            continue

        record, reason = match_record_for_email(
            tenant_id=connection.tenant_id,
            parsed=parsed,
            candidates=candidates,
        )
        if not record:
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason=reason or "no_match",
            )
            unmatched += 1
            if received_ms and (newest_seen is None or received_ms > newest_seen):
                newest_seen = received_ms
            continue

        changed = apply_tracking_to_record(record, parsed)
        ZohoMailProcessedMessage.objects.create(
            connection=connection,
            message_id=message_id,
            subject=subject[:512],
            matched_record_id=record.id,
            applied=changed,
            skip_reason="" if changed else "unchanged",
        )
        if changed:
            applied += 1
            candidates = [c for c in candidates if c.id != record.id]
            logger.info(
                "[ZohoShipmentSync] applied message_id=%s record_id=%s reason=%s awb=%s",
                message_id,
                record.id,
                reason,
                parsed.get("tracking_number"),
            )
        else:
            skipped += 1

        if received_ms and (newest_seen is None or received_ms > newest_seen):
            newest_seen = received_ms

    connection.last_synced_at = timezone.now()
    if newest_seen is not None:
        connection.last_received_time_ms = newest_seen
    connection.save(update_fields=["last_synced_at", "last_received_time_ms", "updated_at"])

    return {
        "success": True,
        "scanned": scanned,
        "shipment_like": shipment_like,
        "applied": applied,
        "unmatched": unmatched,
        "skipped": skipped,
        "errors": errors,
        "timestamp": timezone.now().isoformat(),
    }
