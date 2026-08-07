"""Hybrid orchestrator: classify intent → RAG and/or CRM/ERP tools."""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from chatbot.constants import (
    INTENT_CLARIFY,
    INTENT_DATA,
    INTENT_HELP,
    INTENT_HYBRID,
    MAX_HISTORY_MESSAGES,
    MAX_TOOL_ROUNDS,
    MODE_CLARIFY,
    MODE_HYBRID,
    MODE_RAG,
    MODE_TOOLS,
)
from chatbot.services import rag as rag_service
from chatbot.services.llm import (
    LLMQuotaError,
    chat_completion,
    message_text,
)
from chatbot.services.tools import run_tool, tools_to_openai


DATA_KEYWORDS = re.compile(
    r"\b("
    r"how many|count|counts|stats?|statistic|metric|analytics?|dashboard|report|"
    r"open tickets?|my leads?|unresolved|resolved|"
    r"inventory|stock|sku|shipment|purchase|erp|crm|"
    r"breakdown|assigned|unassigned|available quantity|allocated|"
    r"cse|sla|resolution time|show me|summary|overview|"
    r"billing|invoice|enqueue|background job|pyro job|run job|jobs?|"
    # Page actions — match "create a page", "create page", "my pages", etc.
    r"create (a )?page|new page|list (my )?pages|my pages|update (the )?page|"
    r"delete (the |a )?page|remove (the |a )?page|"
    r"add (a )?(lead )?table|lead table|widgets?|\bpages?\b"
    r")\b",
    re.I,
)
HELP_KEYWORDS = re.compile(
    r"\b("
    r"how (do|does|to)|what is|what are|explain|help|guide|workflow|"
    r"bucket|queue|scoring|feature|mean|works?"
    r")\b",
    re.I,
)
CLARIFY_ONLY = re.compile(
    r"^\s*(hi|hello|hey|yo|sup|thanks|thank you|ok|okay|\?+)\s*$",
    re.I,
)
# Short affirmations after the assistant asked to confirm a mutating tool.
CONFIRM_REPLY = re.compile(
    r"^\s*(yes|yep|yeah|y|confirm|confirmed|go ahead|do it|proceed|sure|please do|"
    r"ok(ay)?(,?\s*(create|confirm|do it|go ahead|proceed))?)\s*[.!]*\s*$",
    re.I,
)
PENDING_ACTION_HINT = re.compile(
    r"confirm|shall i create|page preview|ready to (create|update|delete)|"
    r"create.{0,24}page|update.{0,24}page|"
    r"delete.{0,24}page|remove.{0,40}(page|from)|"
    r"permanently remove|add.{0,24}(widget|table)|enqueue|run (the )?job",
    re.I,
)
ROLE_HINT = re.compile(
    r"\b(?:visibility|visible)\s*(?:for|to|=|:)\s*([A-Za-z0-9_-]{1,40})"
    r"|\bonly\s+([A-Za-z0-9_-]{1,40})\s+users?\b"
    r"|\bfor\s+([A-Za-z0-9_-]{1,40})\s+role\b"
    r"|\brole\s*(?:for|to|=|:)\s*([A-Za-z0-9_-]{1,40})\b",
    re.I,
)
KNOWN_ROLE_TOKEN = re.compile(
    r"\b(GM|RM|CSE|PM|EM|ASM|COO|CTO|HM|Manager)\b",
    re.I,
)


def _heuristic_intent(question: str) -> str:
    q = question or ""
    if CLARIFY_ONLY.match(q):
        return INTENT_CLARIFY
    has_data = bool(DATA_KEYWORDS.search(q))
    has_help = bool(HELP_KEYWORDS.search(q))
    if has_data and has_help:
        return INTENT_HYBRID
    if has_data:
        return INTENT_DATA
    if has_help:
        return INTENT_HELP
    # Short product nouns → treat as live data, not clarify
    if re.search(r"\b(analytics|leads?|tickets?|inventory|crm|erp)\b", q, re.I):
        return INTENT_DATA
    return INTENT_HELP


def _has_pending_confirm_tool(history: Optional[list[dict[str, Any]]] = None) -> bool:
    """True when recent assistant tool_calls include a confirm_required preview."""
    if not history:
        return False
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        for tc in item.get("tool_calls") or []:
            result = tc.get("result") or {}
            if result.get("error") == "confirm_required":
                return True
            # Successful mutate ends the pending confirm chain for that tool.
            if result.get("created") or result.get("updated") or result.get("deleted"):
                return False
        # Only inspect the latest assistant turn that had tools or content.
        if item.get("tool_calls") or item.get("content"):
            break
    return False


def _is_confirm_followup(
    question: str, history: Optional[list[dict[str, Any]]] = None
) -> bool:
    """True when user is affirming a pending create/update/delete/enqueue confirm."""
    if not CONFIRM_REPLY.match(question or ""):
        return False
    if not history:
        return False
    # Prefer structured tool previews — LLM wording is unreliable here.
    if (
        _pending_delete_page_preview(history)
        or _pending_update_page_preview(history)
        or _pending_create_page_preview(history)
        or _has_pending_confirm_tool(history)
    ):
        return True
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        if item.get("content") and PENDING_ACTION_HINT.search(item["content"]):
            return True
        break
    return False


def _extract_role_hint(text: str) -> str:
    text = text or ""
    known = KNOWN_ROLE_TOKEN.search(text)
    if known:
        return known.group(1)
    m = ROLE_HINT.search(text)
    if not m:
        return ""
    for g in m.groups():
        if g and g.strip().lower() not in {"only", "all", "none", "users", "user", "role"}:
            return g.strip()
    return ""


def _pending_create_page_preview(
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    """
    Find the latest create_page confirm_required preview from history tool_calls,
    with a light fallback to Page Preview text in the last assistant message.
    """
    if not history:
        return None
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        for tc in reversed(item.get("tool_calls") or []):
            if tc.get("name") != "create_page":
                continue
            result = tc.get("result") or {}
            if result.get("created"):
                return None
            if result.get("error") == "confirm_required" and isinstance(
                result.get("preview"), dict
            ):
                preview = dict(result["preview"])
                # Enrich null role from later chat wording if present in this message.
                if not preview.get("role"):
                    preview["role"] = _extract_role_hint(item.get("content") or "") or None
                return preview
        content = item.get("content") or ""
        if "Page Preview" in content or "Shall I create" in content:
            name_m = re.search(r"Name:\*?\*?\s*(.+?)(?:\n|$)", content, re.I)
            if name_m:
                name = name_m.group(1).strip().strip("*").strip()
                role = ""
                vis = re.search(
                    r"Visibility:\*?\*?\s*(.+?)(?:\n|$)", content, re.I
                )
                if vis:
                    vis_text = vis.group(1)
                    if re.search(r"no role|all users", vis_text, re.I):
                        role = ""
                    else:
                        role = _extract_role_hint(vis_text)
                return {
                    "name": name,
                    "header_title": name,
                    "icon_name": "Sparkles",
                    "display_order": 0,
                    "role": role or None,
                }
    return None


def _pending_delete_page_preview(
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    if not history:
        return None
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        for tc in reversed(item.get("tool_calls") or []):
            if tc.get("name") != "delete_page":
                continue
            result = tc.get("result") or {}
            if result.get("deleted"):
                return None
            if result.get("error") == "confirm_required" and isinstance(
                result.get("preview"), dict
            ):
                return dict(result["preview"])
    return None


def _format_delete_page_answer(result: dict[str, Any]) -> str:
    if result.get("error") == "confirm_required":
        preview = result.get("preview") or {}
        return (
            "Ready to delete this page:\n"
            f"- Page: {preview.get('page_name')} (`{preview.get('page_id')}`)\n"
            f"- Owner: {preview.get('page_owner_email')}\n\n"
            "Reply **yes** to permanently remove it from My Pages."
        )
    if result.get("error"):
        return f"Couldn't delete the page: {result.get('error')}"
    if result.get("deleted"):
        return (
            f'Page **{result.get("name")}** deleted.\n'
            f"- Id: `{result.get('id')}`\n\n"
            "Refresh My Pages / the app nav to see it gone."
        )
    return json.dumps(result, default=str)


def _deterministic_page_tool_answer(
    tool_trace: list[dict[str, Any]],
) -> Optional[str]:
    """
    Prefer verified tool payloads over LLM paraphrases for page mutations.
    Prevents "deleted successfully" when confirm_required / error actually returned.
    """
    for tc in reversed(tool_trace or []):
        name = tc.get("name")
        result = tc.get("result") or {}
        if not isinstance(result, dict):
            continue
        if name == "delete_page":
            if (
                result.get("deleted")
                or result.get("error") == "confirm_required"
                or result.get("error")
            ):
                return _format_delete_page_answer(result)
        if name == "create_page":
            if (
                result.get("created")
                or result.get("error") == "confirm_required"
                or result.get("error")
            ):
                return _format_create_page_answer(result)
        if name == "update_page":
            if (
                result.get("updated")
                or result.get("error") == "confirm_required"
                or result.get("error")
            ):
                return _format_update_page_answer(result)
    return None


def _pending_delete_matches_question(
    question: str, preview: Optional[dict[str, Any]]
) -> bool:
    """True when user re-asks to delete/remove the same pending page (not just 'yes')."""
    if not preview:
        return False
    name = (preview.get("page_name") or "").strip().lower()
    q = (question or "").strip().lower()
    if not name or name not in q:
        return False
    return bool(re.search(r"\b(delete|remove)\b", q, re.I))


def _maybe_finalize_delete_page(
    question: str,
    *,
    tenant,
    user_id=None,
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    preview = _pending_delete_page_preview(history)
    if not preview or not preview.get("page_id"):
        return None
    if not (
        _is_confirm_followup(question, history)
        or _pending_delete_matches_question(question, preview)
    ):
        return None

    result = run_tool(
        "delete_page",
        tenant,
        {
            "page_id": preview.get("page_id") or "",
            "page_name": preview.get("page_name") or "",
            "confirm": True,
        },
        user_id=user_id,
    )
    return {
        "answer": _format_delete_page_answer(result),
        "sources": [{"type": "tools", "title": "delete_page"}],
        "tool_calls": [
            {
                "name": "delete_page",
                "arguments": {
                    "page_id": preview.get("page_id"),
                    "page_name": preview.get("page_name"),
                    "confirm": True,
                },
                "result": result,
            }
        ],
        "mode": MODE_TOOLS,
        "intent": INTENT_DATA,
    }


def _pending_update_page_preview(
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    if not history:
        return None
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        for tc in reversed(item.get("tool_calls") or []):
            if tc.get("name") != "update_page":
                continue
            result = tc.get("result") or {}
            if result.get("updated"):
                return None
            if result.get("error") == "confirm_required" and isinstance(
                result.get("preview"), dict
            ):
                return dict(result["preview"])
    return None


def _format_update_page_answer(result: dict[str, Any]) -> str:
    if result.get("error") == "confirm_required":
        preview = result.get("preview") or {}
        return (
            "Ready to update this page:\n"
            f"- Page: {preview.get('page_name')} (`{preview.get('page_id')}`)\n"
            f"- Change: {preview.get('change')}\n"
            f"- Widgets: {preview.get('current_widget_count')} → "
            f"{preview.get('next_widget_count')}\n\n"
            "Reply **yes** to confirm."
        )
    if result.get("error"):
        return f"Couldn't update the page: {result.get('error')}"
    if result.get("updated"):
        return (
            f'Page **{result.get("name")}** updated successfully.\n'
            f"- Id: `{result.get('id')}`\n"
            f"- Widgets now: {result.get('widget_count')}\n"
            f"- Added: {result.get('widget_type') or 'n/a'}\n\n"
            "Refresh the page in the builder / app to see the change."
        )
    return json.dumps(result, default=str)


def _maybe_finalize_update_page(
    question: str,
    *,
    tenant,
    user_id=None,
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    preview = _pending_update_page_preview(history)
    if not preview or not preview.get("page_id"):
        return None
    if not _is_confirm_followup(question, history):
        return None

    result = run_tool(
        "update_page",
        tenant,
        {
            "page_id": preview.get("page_id") or "",
            "page_name": preview.get("page_name") or "",
            "action": preview.get("action") or "add_widget",
            "widget_type": preview.get("widget_type") or "",
            "confirm": True,
        },
        user_id=user_id,
    )
    return {
        "answer": _format_update_page_answer(result),
        "sources": [{"type": "tools", "title": "update_page"}],
        "tool_calls": [
            {
                "name": "update_page",
                "arguments": {
                    "page_id": preview.get("page_id"),
                    "action": preview.get("action"),
                    "widget_type": preview.get("widget_type"),
                    "confirm": True,
                },
                "result": result,
            }
        ],
        "mode": MODE_TOOLS,
        "intent": INTENT_DATA,
    }


def _format_create_page_answer(result: dict[str, Any]) -> str:
    if result.get("error"):
        if result.get("error") == "confirm_required":
            preview = result.get("preview") or {}
            return (
                "Ready to create this page:\n"
                f"- Name: {preview.get('name')}\n"
                f"- Owner: {preview.get('page_owner_email')}\n"
                f"- Role: {preview.get('role') or 'none (My Pages only)'}\n\n"
                "Reply **yes** to confirm."
            )
        return f"Couldn't create the page: {result.get('error')}"
    if result.get("created"):
        return (
            f'Page **{result.get("name")}** created successfully.\n'
            f"- Id: `{result.get('id')}`\n"
            f"- Owner: {result.get('page_owner_email')}\n"
            f"- Role: {result.get('role_id') or 'none'}\n\n"
            "Open **My Pages** (as that owner) and refresh to see it. "
            "If a role was set, it also appears in that role's app nav."
        )
    return json.dumps(result, default=str)


def _maybe_finalize_create_page(
    question: str,
    *,
    tenant,
    user_id=None,
    history: Optional[list[dict[str, Any]]] = None,
) -> Optional[dict[str, Any]]:
    """
    If the user confirmed a pending create_page preview, create it without
    relying on the LLM to call the tool again (avoids hallucinated success).
    """
    preview = _pending_create_page_preview(history)
    if not preview or not preview.get("name"):
        return None
    if not _is_confirm_followup(question, history):
        return None

    role = _extract_role_hint(question) or (preview.get("role") or "") or ""
    if not role:
        for item in reversed(history or []):
            hinted = _extract_role_hint(item.get("content") or "")
            if hinted:
                role = hinted
                break

    result = run_tool(
        "create_page",
        tenant,
        {
            "name": preview.get("name") or "",
            "header_title": preview.get("header_title") or preview.get("name") or "",
            "icon_name": preview.get("icon_name") or "Sparkles",
            "display_order": int(preview.get("display_order") or 0),
            "role": role,
            "confirm": True,
        },
        user_id=user_id,
    )
    return {
        "answer": _format_create_page_answer(result),
        "sources": [{"type": "tools", "title": "create_page"}],
        "tool_calls": [
            {
                "name": "create_page",
                "arguments": {
                    "name": preview.get("name"),
                    "role": role or None,
                    "confirm": True,
                },
                "result": result,
            }
        ],
        "mode": MODE_TOOLS,
        "intent": INTENT_DATA,
    }


def classify_intent(question: str) -> str:
    """Classify intent; never over-clarify domain words like analytics."""
    heuristic = _heuristic_intent(question)
    prompt = (
        "Classify the user question for a Pyro CRM+ERP+analytics assistant.\n"
        "Return ONLY one word: help | data | hybrid | clarify\n"
        "- help: product how-to / feature explanation (NOT create/list actions)\n"
        "- data: live counts, analytics, dashboard, records, inventory, tickets, leads, "
        "reports, billing, jobs, AND actions like create page / list pages / enqueue job\n"
        "- hybrid: needs both explanation and live data\n"
        "- clarify: ONLY pure greetings or empty noise (hi/hello/thanks), NOT product words\n"
        "Important: 'analytics', 'show analytics', 'dashboard', 'ticket stats', "
        "'create a page named X', 'list my pages', 'add lead table to Ops Home', "
        "'delete page Ops Home' are ALWAYS data.\n\n"
        f"Question: {question}"
    )
    try:
        resp = chat_completion(
            [{"role": "user", "content": prompt}],
            temperature=0,
            purpose="classify",
        )
        label = (message_text(resp) or "").strip().lower().split()[0]
        if label not in {INTENT_HELP, INTENT_DATA, INTENT_HYBRID, INTENT_CLARIFY}:
            return heuristic
        # Don't let the model clarify when heuristics already know it's data/help
        if label == INTENT_CLARIFY and heuristic != INTENT_CLARIFY:
            return heuristic
        # Don't let the model send action/live-data requests into RAG-only help
        if label == INTENT_HELP and heuristic in {INTENT_DATA, INTENT_HYBRID}:
            return heuristic
        return label
    except Exception:
        return heuristic


def _history_for_llm(history: Optional[list[dict[str, str]]]) -> list[dict[str, str]]:
    if not history:
        return []
    cleaned = []
    for item in history[-MAX_HISTORY_MESSAGES:]:
        role = item.get("role")
        content = item.get("content")
        if role in ("user", "assistant") and content:
            cleaned.append({"role": role, "content": content})
    return cleaned


def _run_tools_loop(
    question: str,
    *,
    tenant,
    user_id=None,
    history: Optional[list[dict[str, str]]] = None,
    page_context: Optional[dict] = None,
    rag_context: Optional[str] = None,
) -> dict[str, Any]:
    system = (
        "You are Sparky, Pyro's friendly CRM + ERP + ops assistant for this tenant. "
        "Use tools for live data and bob actions (billing, background jobs, pyro jobs, pages). "
        "Never invent counts or job ids. "
        "For enqueue_background_job / enqueue_pyro_job / create_page / update_page / delete_page: "
        "ALWAYS ask the user to confirm before calling with confirm=true. "
        "If a tool returns confirm_required, ask them. "
        "Prefer get_billing_report for billing questions. "
        "When the user asks to create / list / update / delete pages or add widgets, "
        "ALWAYS call create_page / list_my_pages / update_page / delete_page — "
        "do not say you lack a tool or documentation. "
        "For create_page, call once without confirm to preview, then after yes call again with confirm=true. "
        "To add a lead table to an existing page (e.g. Ops Home), call update_page with "
        "action=add_widget, widget_type=leadTable, page_id or page_name, then confirm=true after yes. "
        "To delete/remove a page, call delete_page with page_name set to the exact name "
        "(page_id optional only for disambiguation). Never delete a different page than named. "
        "Then confirm=true after yes. "
        "NEVER say a page was created/updated/deleted unless the tool result has "
        "created=true, updated=true, or deleted=true for that same page name. "
        "If the user asks for role visibility (e.g. GM), pass role on create_page. "
        "List job types before enqueueing if the user is unsure of the name. "
        "Be concise."
    )
    if rag_context:
        system += (
            "\nYou also have product context below; combine it with tool results when useful.\n"
            f"Product context:\n{rag_context}"
        )

    messages: list[dict[str, Any]] = [{"role": "system", "content": system}]
    messages.extend(_history_for_llm(history))
    user_content = question
    if page_context:
        user_content = f"UI context: {page_context}\n\nQuestion: {question}"
    messages.append({"role": "user", "content": user_content})

    tool_trace: list[dict[str, Any]] = []
    tools = tools_to_openai()

    try:
        for _ in range(MAX_TOOL_ROUNDS):
            result = chat_completion(
                messages,
                temperature=0.1,
                tools=tools,
                tool_choice="auto",
            )
            tool_calls = result.tool_calls or []

            if not tool_calls:
                deterministic = _deterministic_page_tool_answer(tool_trace)
                return {
                    "answer": deterministic
                    or result.content
                    or "No answer generated.",
                    "sources": [{"type": "tools", "title": "Live CRM/ERP data"}],
                    "tool_calls": tool_trace,
                    "mode": MODE_HYBRID if rag_context else MODE_TOOLS,
                }

            messages.append(
                {
                    "role": "assistant",
                    "content": result.content or "",
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.name,
                                "arguments": tc.arguments,
                            },
                        }
                        for tc in tool_calls
                    ],
                }
            )

            for tc in tool_calls:
                tool_result = run_tool(
                    tc.name, tenant, tc.arguments, user_id=user_id
                )
                tool_trace.append(
                    {"name": tc.name, "arguments": tc.arguments, "result": tool_result}
                )
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tc.id,
                        "content": json.dumps(tool_result, default=str),
                    }
                )

        final = chat_completion(messages, temperature=0.1)
        deterministic = _deterministic_page_tool_answer(tool_trace)
        return {
            "answer": deterministic
            or message_text(final)
            or "I reached the tool call limit. Please narrow your question.",
            "sources": [{"type": "tools", "title": "Live CRM/ERP data"}],
            "tool_calls": tool_trace,
            "mode": MODE_HYBRID if rag_context else MODE_TOOLS,
        }
    except LLMQuotaError:
        if rag_context:
            return {
                "answer": (
                    "Live data tools need an LLM with available API credits. "
                    "Here is related product documentation instead:\n\n"
                    f"{rag_context}"
                ),
                "sources": [{"type": "docs", "title": "Product docs (LLM quota exceeded)"}],
                "tool_calls": tool_trace,
                "mode": MODE_RAG,
            }
        # Deterministic overview without LLM
        overview = run_tool("domain_overview", tenant, {}, user_id=user_id)
        return {
            "answer": (
                "LLM API credits are exhausted, so I can't reason over tools right now. "
                "Here is a basic CRM/ERP count overview for your tenant:\n\n"
                f"{json.dumps(overview, indent=2)}\n\n"
                "Add Anthropic (`ANTHROPIC_API_KEY`) or top up OpenAI credits to restore full chat."
            ),
            "sources": [{"type": "tools", "title": "domain_overview (no LLM)"}],
            "tool_calls": tool_trace + [{"name": "domain_overview", "result": overview}],
            "mode": MODE_TOOLS,
        }


def handle_message(
    question: str,
    *,
    tenant,
    user_id=None,
    history: Optional[list[dict[str, str]]] = None,
    page_context: Optional[dict] = None,
) -> dict[str, Any]:
    question = (question or "").strip()
    if not question:
        return {
            "answer": "Please ask a question about Pyro CRM or ERP.",
            "sources": [],
            "tool_calls": [],
            "mode": MODE_CLARIFY,
            "intent": INTENT_CLARIFY,
        }

    # Deterministic confirm → mutate (LLM previously hallucinated success here).
    finalized = _maybe_finalize_delete_page(
        question, tenant=tenant, user_id=user_id, history=history
    )
    if finalized:
        return finalized
    finalized = _maybe_finalize_update_page(
        question, tenant=tenant, user_id=user_id, history=history
    )
    if finalized:
        return finalized
    finalized = _maybe_finalize_create_page(
        question, tenant=tenant, user_id=user_id, history=history
    )
    if finalized:
        return finalized

    pending_create = _pending_create_page_preview(history)
    pending_update = _pending_update_page_preview(history)
    pending_delete = _pending_delete_page_preview(history)
    # Affirmations / page-create tweaks after a preview must stay on the tools path.
    if _is_confirm_followup(question, history) or (
        (pending_create or pending_update or pending_delete)
        and (
            DATA_KEYWORDS.search(question)
            or _extract_role_hint(question)
            or re.search(
                r"\b(visibility|role|create|update|delete|remove|widget|table)\b",
                question,
                re.I,
            )
        )
    ):
        intent = INTENT_DATA
    else:
        intent = classify_intent(question)

    if intent == INTENT_CLARIFY:
        return {
            "answer": (
                "Hey — I'm Sparky! Ask me things like:\n"
                "- \"Show analytics\"\n"
                "- \"Show billing for this month\"\n"
                "- \"Create a page named Ops Home\"\n"
                "- \"List background job types\"\n"
                "- \"How do lead buckets work?\""
            ),
            "sources": [],
            "tool_calls": [],
            "mode": MODE_CLARIFY,
            "intent": intent,
        }

    if intent == INTENT_HELP:
        result = rag_service.answer_from_rag(
            question,
            tenant=tenant,
            history=_history_for_llm(history),
            page_context=page_context,
        )
        result["tool_calls"] = []
        result["intent"] = intent
        result["mode"] = MODE_RAG
        return result

    rag_context = None
    sources: list[dict[str, Any]] = []
    if intent == INTENT_HYBRID:
        chunks = rag_service.retrieve_chunks(question, tenant=tenant, top_k=3)
        if chunks:
            rag_context = "\n\n".join(
                f"- {c['title']}: {c['content'][:800]}" for c in chunks
            )
            sources.extend(
                {
                    "title": c["title"],
                    "domain": c["domain"],
                    "slug": c["slug"],
                    "score": c["score"],
                    "type": "doc",
                }
                for c in chunks
            )

    tools_result = _run_tools_loop(
        question,
        tenant=tenant,
        user_id=user_id,
        history=history,
        page_context=page_context,
        rag_context=rag_context,
    )
    if sources:
        tools_result["sources"] = sources + list(tools_result.get("sources") or [])
    tools_result["intent"] = intent
    if intent == INTENT_HYBRID:
        tools_result["mode"] = MODE_HYBRID
    return tools_result
