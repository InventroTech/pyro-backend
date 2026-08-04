"""
Multi-provider chat LLM layer (OpenAI + Anthropic Claude).

Env:
  CHATBOT_PROVIDER=auto|both|openai|anthropic   (default auto)
  CHATBOT_PROVIDER_PRIMARY=anthropic|openai     (order when both/auto)
  ANTHROPIC_API_KEY=...
  OPENAI_API_KEY=... or PYRO_OPEN_AI_API=...
  CHATBOT_MODEL_ANTHROPIC / CHATBOT_MODEL_OPENAI
  CHATBOT_CLASSIFY_MODEL_ANTHROPIC / CHATBOT_CLASSIFY_MODEL_OPENAI

Note: Claude Pro (claude.ai subscription) does NOT include API access.
Create a key at https://console.anthropic.com/ — billed separately.
With provider=both|auto and both keys set, the chatbot fails over
(Claude first by default, then OpenAI if Claude errors).
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Optional

from chatbot.constants import (
    CHAT_MODEL_DEFAULT,
    CHAT_MODEL_ENV,
    CHAT_MODEL_ANTHROPIC_ENV,
    CHAT_MODEL_OPENAI_ENV,
    CLASSIFY_MODEL_DEFAULT,
    CLASSIFY_MODEL_ENV,
    CLASSIFY_MODEL_ANTHROPIC_ENV,
    CLASSIFY_MODEL_OPENAI_ENV,
    PROVIDER_ANTHROPIC,
    PROVIDER_AUTO,
    PROVIDER_BOTH,
    PROVIDER_ENV,
    PROVIDER_OPENAI,
)

ANTHROPIC_CHAT_DEFAULT = "claude-sonnet-4-5"
ANTHROPIC_CLASSIFY_DEFAULT = "claude-haiku-4-5"


def _openai_api_key() -> str:
    return (os.getenv("OPENAI_API_KEY") or os.getenv("PYRO_OPEN_AI_API") or "").strip()


def _anthropic_api_key() -> str:
    return (os.getenv("ANTHROPIC_API_KEY") or "").strip()


# Kept for backwards-compatible imports; prefer helpers above (lazy read).
OPENAI_API_KEY = _openai_api_key()
ANTHROPIC_API_KEY = _anthropic_api_key()


@dataclass
class NormalizedToolCall:
    id: str
    name: str
    arguments: str  # JSON string


@dataclass
class NormalizedChatResult:
    content: str = ""
    tool_calls: list[NormalizedToolCall] = field(default_factory=list)
    provider: str = ""
    model: str = ""


class LLMQuotaError(RuntimeError):
    """Raised when the provider reports insufficient credits / rate limit."""


def available_providers() -> list[str]:
    """Providers that have API keys configured."""
    out: list[str] = []
    if _anthropic_api_key():
        out.append(PROVIDER_ANTHROPIC)
    if _openai_api_key():
        out.append(PROVIDER_OPENAI)
    return out


def provider_chain() -> list[str]:
    """
    Ordered providers to try for a chat turn.

    - openai / anthropic → only that provider
    - both / auto → prefer Anthropic then OpenAI when both keys exist
      (override preference with CHATBOT_PROVIDER_PRIMARY=openai|anthropic)
    """
    raw = (os.getenv(PROVIDER_ENV) or PROVIDER_AUTO).strip().lower()
    anthropic_key = _anthropic_api_key()
    openai_key = _openai_api_key()

    if raw == PROVIDER_ANTHROPIC:
        if not anthropic_key:
            raise RuntimeError(
                "CHATBOT_PROVIDER=anthropic but ANTHROPIC_API_KEY is not set. "
                "Create a key at https://console.anthropic.com/"
            )
        return [PROVIDER_ANTHROPIC]

    if raw == PROVIDER_OPENAI:
        if not openai_key:
            raise RuntimeError("CHATBOT_PROVIDER=openai but OPENAI_API_KEY is not set.")
        return [PROVIDER_OPENAI]

    # both / auto → use every configured provider (failover)
    if raw not in {PROVIDER_AUTO, PROVIDER_BOTH, ""}:
        raise RuntimeError(
            f"Unknown CHATBOT_PROVIDER={raw!r}. Use auto|both|openai|anthropic."
        )

    primary = (os.getenv("CHATBOT_PROVIDER_PRIMARY") or PROVIDER_ANTHROPIC).strip().lower()
    chain: list[str] = []
    if primary == PROVIDER_OPENAI:
        order = [PROVIDER_OPENAI, PROVIDER_ANTHROPIC]
    else:
        order = [PROVIDER_ANTHROPIC, PROVIDER_OPENAI]

    for p in order:
        if p == PROVIDER_ANTHROPIC and anthropic_key:
            chain.append(p)
        if p == PROVIDER_OPENAI and openai_key:
            chain.append(p)

    if not chain:
        raise RuntimeError(
            "No LLM API key configured. Set ANTHROPIC_API_KEY and/or OPENAI_API_KEY."
        )
    return chain


def resolve_provider() -> str:
    """Primary provider (first in the failover chain)."""
    return provider_chain()[0]


def chat_model(provider: Optional[str] = None) -> str:
    p = provider or resolve_provider()
    if p == PROVIDER_ANTHROPIC:
        return (
            os.getenv(CHAT_MODEL_ANTHROPIC_ENV)
            or (os.getenv(CHAT_MODEL_ENV) if (os.getenv(PROVIDER_ENV) or "").strip().lower() == PROVIDER_ANTHROPIC else None)
            or ANTHROPIC_CHAT_DEFAULT
        )
    # openai
    return (
        os.getenv(CHAT_MODEL_OPENAI_ENV)
        or (os.getenv(CHAT_MODEL_ENV) if (os.getenv(PROVIDER_ENV) or "").strip().lower() == PROVIDER_OPENAI else None)
        or CHAT_MODEL_DEFAULT
    )


def classify_model(provider: Optional[str] = None) -> str:
    p = provider or resolve_provider()
    if p == PROVIDER_ANTHROPIC:
        return (
            os.getenv(CLASSIFY_MODEL_ANTHROPIC_ENV)
            or (os.getenv(CLASSIFY_MODEL_ENV) if (os.getenv(PROVIDER_ENV) or "").strip().lower() == PROVIDER_ANTHROPIC else None)
            or ANTHROPIC_CLASSIFY_DEFAULT
        )
    return (
        os.getenv(CLASSIFY_MODEL_OPENAI_ENV)
        or (os.getenv(CLASSIFY_MODEL_ENV) if (os.getenv(PROVIDER_ENV) or "").strip().lower() == PROVIDER_OPENAI else None)
        or CLASSIFY_MODEL_DEFAULT
    )


def get_openai_client():
    import openai

    key = _openai_api_key()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    return openai.OpenAI(api_key=key)


def get_anthropic_client():
    import anthropic

    key = _anthropic_api_key()
    if not key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set")
    return anthropic.Anthropic(api_key=key)


# Back-compat alias used by embeddings.py
def get_client():
    return get_openai_client()


def _is_quota_error(exc: BaseException) -> bool:
    text = str(exc).lower()
    markers = (
        "insufficient_quota",
        "credit_balance_exhausted",
        "you have no credits",
        "rate_limit",
        "429",
        "credit balance is too low",
    )
    return any(m in text for m in markers)


def _openai_tools_to_anthropic(tools: Optional[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for t in tools or []:
        fn = t.get("function") or {}
        out.append(
            {
                "name": fn.get("name") or t.get("name"),
                "description": fn.get("description") or t.get("description") or "",
                "input_schema": fn.get("parameters")
                or t.get("input_schema")
                or {"type": "object", "properties": {}},
            }
        )
    return out


def _split_system(messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    system_parts: list[str] = []
    rest: list[dict[str, Any]] = []
    for m in messages:
        if m.get("role") == "system":
            content = m.get("content") or ""
            if content:
                system_parts.append(str(content))
        else:
            rest.append(m)
    return "\n\n".join(system_parts), rest


def _openai_messages_to_anthropic(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert OpenAI-style chat messages (including tool_calls / tool results)
    into Anthropic Messages API format.
    """
    converted: list[dict[str, Any]] = []
    pending_tool_results: list[dict[str, Any]] = []

    def flush_tool_results():
        nonlocal pending_tool_results
        if pending_tool_results:
            converted.append({"role": "user", "content": pending_tool_results})
            pending_tool_results = []

    for m in messages:
        role = m.get("role")
        if role == "tool":
            pending_tool_results.append(
                {
                    "type": "tool_result",
                    "tool_use_id": m.get("tool_call_id") or "",
                    "content": m.get("content") or "",
                }
            )
            continue

        flush_tool_results()

        if role == "user":
            converted.append({"role": "user", "content": m.get("content") or ""})
            continue

        if role == "assistant":
            content_blocks: list[dict[str, Any]] = []
            text = m.get("content") or ""
            if text:
                content_blocks.append({"type": "text", "text": text})
            for tc in m.get("tool_calls") or []:
                fn = tc.get("function") or {}
                raw_args = fn.get("arguments") or "{}"
                try:
                    inp = json.loads(raw_args) if isinstance(raw_args, str) else (raw_args or {})
                except json.JSONDecodeError:
                    inp = {}
                content_blocks.append(
                    {
                        "type": "tool_use",
                        "id": tc.get("id") or "",
                        "name": fn.get("name") or "",
                        "input": inp,
                    }
                )
            if not content_blocks:
                content_blocks = [{"type": "text", "text": ""}]
            converted.append({"role": "assistant", "content": content_blocks})

    flush_tool_results()
    return converted


def _chat_openai(
    messages: list[dict[str, Any]],
    *,
    model: str,
    temperature: float,
    tools: Optional[list[dict]] = None,
    tool_choice: Optional[str] = None,
) -> NormalizedChatResult:
    import openai

    client = get_openai_client()
    kwargs: dict[str, Any] = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
    }
    if tools:
        kwargs["tools"] = tools
        if tool_choice:
            kwargs["tool_choice"] = tool_choice
    try:
        resp = client.chat.completions.create(**kwargs)
    except Exception as exc:
        if _is_quota_error(exc):
            raise LLMQuotaError(str(exc)) from exc
        raise

    choice = resp.choices[0].message
    tool_calls: list[NormalizedToolCall] = []
    for tc in getattr(choice, "tool_calls", None) or []:
        tool_calls.append(
            NormalizedToolCall(
                id=tc.id,
                name=tc.function.name,
                arguments=tc.function.arguments or "{}",
            )
        )
    return NormalizedChatResult(
        content=(choice.content or "").strip(),
        tool_calls=tool_calls,
        provider=PROVIDER_OPENAI,
        model=model,
    )


def _chat_anthropic(
    messages: list[dict[str, Any]],
    *,
    model: str,
    temperature: float,
    tools: Optional[list[dict]] = None,
) -> NormalizedChatResult:
    client = get_anthropic_client()
    system, rest = _split_system(messages)
    anth_messages = _openai_messages_to_anthropic(rest)
    if not anth_messages:
        anth_messages = [{"role": "user", "content": ""}]

    kwargs: dict[str, Any] = {
        "model": model,
        "messages": anth_messages,
        "temperature": temperature,
        "max_tokens": 2048,
    }
    if system:
        kwargs["system"] = system
    anth_tools = _openai_tools_to_anthropic(tools)
    if anth_tools:
        kwargs["tools"] = anth_tools

    try:
        resp = client.messages.create(**kwargs)
    except Exception as exc:
        if _is_quota_error(exc):
            raise LLMQuotaError(str(exc)) from exc
        raise

    text_parts: list[str] = []
    tool_calls: list[NormalizedToolCall] = []
    for block in resp.content or []:
        btype = getattr(block, "type", None)
        if btype == "text":
            text_parts.append(getattr(block, "text", "") or "")
        elif btype == "tool_use":
            tool_calls.append(
                NormalizedToolCall(
                    id=getattr(block, "id", "") or "",
                    name=getattr(block, "name", "") or "",
                    arguments=json.dumps(getattr(block, "input", None) or {}),
                )
            )

    return NormalizedChatResult(
        content="\n".join(p for p in text_parts if p).strip(),
        tool_calls=tool_calls,
        provider=PROVIDER_ANTHROPIC,
        model=model,
    )


def chat_completion(
    messages: list[dict[str, Any]],
    *,
    model: Optional[str] = None,
    temperature: float = 0.2,
    tools: Optional[list[dict]] = None,
    tool_choice: Optional[str] = None,
    purpose: str = "chat",
) -> NormalizedChatResult:
    """
    Provider-agnostic chat turn with optional failover.

    With CHATBOT_PROVIDER=both|auto and both keys set, tries primary then fallback.
    ``purpose="classify"`` uses cheaper per-provider classify models.
    """
    chain = provider_chain()
    errors: list[str] = []

    for provider in chain:
        if model and len(chain) == 1:
            resolved_model = model
        elif purpose == "classify":
            resolved_model = classify_model(provider)
        else:
            resolved_model = chat_model(provider)
        try:
            if provider == PROVIDER_ANTHROPIC:
                return _chat_anthropic(
                    messages,
                    model=resolved_model,
                    temperature=temperature,
                    tools=tools,
                )
            return _chat_openai(
                messages,
                model=resolved_model,
                temperature=temperature,
                tools=tools,
                tool_choice=tool_choice,
            )
        except Exception as exc:
            errors.append(f"{provider}/{resolved_model}: {exc}")
            continue

    joined = " | ".join(errors) if errors else "unknown error"
    if any(_is_quota_error(RuntimeError(e)) for e in errors):
        raise LLMQuotaError(f"All LLM providers failed: {joined}")
    raise RuntimeError(f"All LLM providers failed: {joined}")


def message_text(response: Any) -> str:
    """Accept NormalizedChatResult or legacy OpenAI response."""
    if isinstance(response, NormalizedChatResult):
        return response.content
    try:
        return (response.choices[0].message.content or "").strip()
    except Exception:
        return ""
