"""
Sync Zoho Mail ops inbox → auto-fill inventory_request / unmannd_request tracking fields.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from django.conf import settings
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

DEFAULT_BACKFILL_PAGE_SIZE = 200
DEFAULT_BACKFILL_MAX_MESSAGES_PER_RUN = 500
DEFAULT_INCREMENTAL_BATCH_SIZE = 40


def _backfill_page_size() -> int:
    return int(getattr(settings, "ZOHO_MAIL_BACKFILL_PAGE_SIZE", DEFAULT_BACKFILL_PAGE_SIZE))


def _backfill_max_messages_per_run() -> int:
    return int(
        getattr(
            settings,
            "ZOHO_MAIL_BACKFILL_MAX_MESSAGES_PER_RUN",
            DEFAULT_BACKFILL_MAX_MESSAGES_PER_RUN,
        )
    )


@dataclass
class _SyncStats:
    scanned: int = 0
    shipment_like: int = 0
    applied: int = 0
    unmatched: int = 0
    skipped: int = 0
    errors: int = 0
    fetched: int = 0
    newest_seen: Optional[int] = None

    def merge_newest(self, received_ms: int) -> None:
        if received_ms and (self.newest_seen is None or received_ms > self.newest_seen):
            self.newest_seen = received_ms

    def as_result(self) -> Dict[str, Any]:
        return {
            "scanned": self.scanned,
            "shipment_like": self.shipment_like,
            "applied": self.applied,
            "unmatched": self.unmatched,
            "skipped": self.skipped,
            "errors": self.errors,
            "timestamp": timezone.now().isoformat(),
        }


def _normalize_item_text(value: str) -> str:
    text = (value or "").lower()
    text = _MARKETPLACE_SUFFIX_RE.sub("", text)
    text = re.sub(r"[^\w\s+./-]", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _record_item_names(data: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    seen: set[str] = set()
    for field_name in ITEM_NAME_FIELDS:
        raw = str(data.get(field_name) or "").strip()
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
    accounts = client.list_accounts()
    primary = ZohoMailClient.pick_primary_account(accounts)
    if not primary:
        raise ZohoOAuthError("No Zoho Mail account found for this connection.")

    account_id = str(primary.get("accountId") or primary.get("account_id") or "")
    if not account_id:
        raise ZohoOAuthError("Zoho accountId missing from accounts response.")

    email = (
        primary.get("mailboxAddress")
        or primary.get("emailAddress")
        or primary.get("primaryEmailAddress")
        or ""
    )

    folder_id = connection.inbox_folder_id
    if not folder_id or connection.account_id != account_id:
        folder_id = client.find_inbox_folder_id(account_id)
        if not folder_id:
            raise ZohoOAuthError("Could not find Zoho Inbox folder.")

    changed = (
        connection.account_id != account_id
        or (email and connection.email_address != str(email))
        or connection.inbox_folder_id != folder_id
    )
    connection.account_id = account_id
    if email:
        connection.email_address = str(email)
    connection.inbox_folder_id = folder_id

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


def _batch_message_ids(messages: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    for msg in messages:
        message_id = str(msg.get("messageId") or msg.get("message_id") or "").strip()
        if message_id:
            ids.append(message_id)
    return ids


def _process_message_batch(
    *,
    connection: ZohoMailConnection,
    client: ZohoMailClient,
    messages: List[Dict[str, Any]],
    stats: _SyncStats,
    candidates: List[Record],
) -> List[Record]:
    """Scan a batch of inbox list rows; mutate stats and return updated candidates."""
    for msg in messages:
        message_id = str(msg.get("messageId") or msg.get("message_id") or "").strip()
        if not message_id:
            continue

        received_raw = msg.get("receivedTime") or msg.get("sentDateInGMT") or 0
        try:
            received_ms = int(received_raw)
        except (TypeError, ValueError):
            received_ms = 0

        if ZohoMailProcessedMessage.objects.filter(
            connection=connection, message_id=message_id
        ).exists():
            stats.skipped += 1
            stats.merge_newest(received_ms)
            continue

        stats.scanned += 1
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
            stats.errors += 1
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
            stats.merge_newest(received_ms)
            continue

        if not parsed.get("is_shipment"):
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason="not_delivery_partner",
            )
            stats.merge_newest(received_ms)
            continue

        stats.shipment_like += 1
        if not (parsed.get("tracking_number") or parsed.get("tracking_link")):
            ZohoMailProcessedMessage.objects.create(
                connection=connection,
                message_id=message_id,
                subject=subject[:512],
                applied=False,
                skip_reason="no_tracking_payload",
            )
            stats.unmatched += 1
            stats.merge_newest(received_ms)
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
            stats.unmatched += 1
            stats.merge_newest(received_ms)
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
            stats.applied += 1
            candidates = [c for c in candidates if c.id != record.id]
            logger.info(
                "[ZohoShipmentSync] applied message_id=%s record_id=%s reason=%s awb=%s",
                message_id,
                record.id,
                reason,
                parsed.get("tracking_number"),
            )
        else:
            stats.skipped += 1

        stats.merge_newest(received_ms)

    return candidates


def _run_paginated_sync(
    *,
    connection: ZohoMailConnection,
    client: ZohoMailClient,
    stats: _SyncStats,
    candidates: List[Record],
    max_messages_per_run: int,
) -> List[Record]:
    """
    Walk the inbox sequentially from ``backfill_next_start``.

    Initial connect scans the whole mailbox across runs. After the end is
    reached the pointer wraps to ``start=1`` for new mail. Ongoing syncs
    continue pagination from the saved pointer — never jump back with a
    separate "last 40 + time cursor" mode.
    """
    page_size = _backfill_page_size()
    start = max(1, int(connection.backfill_next_start or 1))
    fetched_this_run = 0
    reached_inbox_end = False

    while fetched_this_run < max_messages_per_run:
        limit = min(page_size, max_messages_per_run - fetched_this_run)
        messages = client.list_messages(
            account_id=connection.account_id,
            folder_id=connection.inbox_folder_id,
            start=start,
            limit=limit,
        )
        if not messages:
            reached_inbox_end = True
            break

        stats.fetched += len(messages)
        scanned_before = stats.scanned
        candidates = _process_message_batch(
            connection=connection,
            client=client,
            messages=messages,
            stats=stats,
            candidates=candidates,
        )
        fetched_this_run += len(messages)

        message_ids = _batch_message_ids(messages)
        if (
            connection.initial_backfill_completed
            and start == 1
            and message_ids
            and stats.scanned == scanned_before
            and ZohoMailProcessedMessage.objects.filter(
                connection=connection,
                message_id__in=message_ids,
            ).count()
            == len(message_ids)
        ):
            # Caught up at inbox head — no new mail since last sync.
            connection.backfill_next_start = 1
            break

        start += len(messages)

        if len(messages) < limit:
            reached_inbox_end = True
            break

    if reached_inbox_end:
        connection.backfill_next_start = 1
        if not connection.initial_backfill_completed:
            connection.initial_backfill_completed = True
            logger.info(
                "[ZohoShipmentSync] initial backfill complete tenant=%s connection=%s scanned=%s",
                connection.tenant_id,
                connection.id,
                stats.scanned,
            )
    else:
        connection.backfill_next_start = start
        if not connection.initial_backfill_completed:
            logger.info(
                "[ZohoShipmentSync] inbox scan progress tenant=%s connection=%s next_start=%s",
                connection.tenant_id,
                connection.id,
                connection.backfill_next_start,
            )

    return candidates


def sync_zoho_shipment_emails(
    connection: ZohoMailConnection,
    *,
    max_messages: int = DEFAULT_INCREMENTAL_BATCH_SIZE,
) -> Dict[str, Any]:
    """
    Poll Zoho inbox for shipment emails and auto-fill matching records.

    Always paginates from ``backfill_next_start``. New connections scan the full
    inbox across runs; after the end is reached the pointer wraps to ``start=1``
    for new mail. Idempotency is via processed-message rows, not received-time cutoffs.
    """
    if not connection.is_active:
        return {"success": True, "skipped": "inactive"}

    access_token = ensure_fresh_access_token(connection)
    client = ZohoMailClient(
        access_token=access_token,
        mail_api_base_url=connection.mail_api_base_url,
    )
    ensure_account_and_inbox(connection, client)

    stats = _SyncStats(newest_seen=connection.last_received_time_ms)
    candidates = _candidate_records(connection.tenant_id)

    if connection.initial_backfill_completed:
        max_per_run = max(1, min(int(max_messages or DEFAULT_INCREMENTAL_BATCH_SIZE), 200))
    else:
        max_per_run = _backfill_max_messages_per_run()

    candidates = _run_paginated_sync(
        connection=connection,
        client=client,
        stats=stats,
        candidates=candidates,
        max_messages_per_run=max_per_run,
    )

    connection.last_synced_at = timezone.now()
    if stats.newest_seen is not None:
        connection.last_received_time_ms = stats.newest_seen

    connection.save(
        update_fields=[
            "last_synced_at",
            "last_received_time_ms",
            "initial_backfill_completed",
            "backfill_next_start",
            "updated_at",
        ]
    )

    result = stats.as_result()
    result["success"] = True
    result["fetched"] = stats.fetched
    result["initial_backfill_completed"] = connection.initial_backfill_completed
    result["backfill_next_start"] = connection.backfill_next_start
    return result
