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
    r"create page|my pages|new page|pages?"
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


def classify_intent(question: str) -> str:
    """Classify intent; never over-clarify domain words like analytics."""
    heuristic = _heuristic_intent(question)
    prompt = (
        "Classify the user question for a Pyro CRM+ERP+analytics assistant.\n"
        "Return ONLY one word: help | data | hybrid | clarify\n"
        "- help: product how-to / feature explanation\n"
        "- data: live counts, analytics, dashboard, records, inventory, tickets, leads, reports\n"
        "- hybrid: needs both explanation and live data\n"
        "- clarify: ONLY pure greetings or empty noise (hi/hello/thanks), NOT product words\n"
        "Important: 'analytics', 'show analytics', 'dashboard', 'ticket stats' are ALWAYS data.\n\n"
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
        "You are Pyro's CRM + ERP + ops assistant for this tenant. "
        "Use tools for live data and bob actions (billing, background jobs, pyro jobs, pages). "
        "Never invent counts or job ids. "
        "For enqueue_background_job / enqueue_pyro_job / create_page: ALWAYS ask the user to confirm "
        "before calling with confirm=true. If a tool returns confirm_required, ask them. "
        "Prefer get_billing_report for billing questions. "
        "Use create_page / list_my_pages for dashboard pages. "
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
                return {
                    "answer": result.content or "No answer generated.",
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
        return {
            "answer": message_text(final)
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

    intent = classify_intent(question)

    if intent == INTENT_CLARIFY:
        return {
            "answer": (
                "Hi! Ask me things like:\n"
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
