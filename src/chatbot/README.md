# Pyro AI Chatbot (hybrid RAG + CRM/ERP tools)

Tenant-scoped assistant that answers:

- **Help / how-to** via RAG over `chatbot/knowledge/*.md`
- **Live CRM + ERP data** via tools on `records` (`lead`, `support_ticket`, `inventory_item`, …)

## Endpoints

All require tenant auth (`IsTenantAuthenticated`).

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/chat/ask/` | One-shot ask (creates conversation if needed) |
| GET/POST | `/chat/conversations/` | List / create conversations |
| GET | `/chat/conversations/<uuid>/` | Conversation + messages |
| GET/POST | `/chat/conversations/<uuid>/messages/` | History / send message |

### Ask body

```json
{
  "message": "How many open leads do we have?",
  "conversation_id": null,
  "page_context": { "route": "/leads/queue", "entity_type": "lead" }
}
```

## LLM providers (OpenAI + Claude)

| Env | Purpose |
|-----|---------|
| `CHATBOT_PROVIDER` | `both` / `auto` (failover), or force `openai` / `anthropic` |
| `CHATBOT_PROVIDER_PRIMARY` | `anthropic` (default) or `openai` — who is tried first |
| `ANTHROPIC_API_KEY` | Claude API key from [console.anthropic.com](https://console.anthropic.com/) |
| `OPENAI_API_KEY` | OpenAI key (also used for embeddings when available) |
| `CHATBOT_MODEL_ANTHROPIC` | Claude chat model (default `claude-sonnet-4-5`) |
| `CHATBOT_MODEL_OPENAI` | OpenAI chat model (default `gpt-4.1-mini`) |

**Important:** Claude Pro / ChatGPT Plus are **not** API keys.

```bash
# Use BOTH providers (Claude first, OpenAI fallback)
CHATBOT_PROVIDER=both
CHATBOT_PROVIDER_PRIMARY=anthropic
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-proj-...
CHATBOT_MODEL_ANTHROPIC=claude-sonnet-4-5
CHATBOT_MODEL_OPENAI=gpt-4.1-mini
```

## Setup

1. Set `ANTHROPIC_API_KEY` and/or `OPENAI_API_KEY`.
2. `pip install -r requirements.txt` (includes `anthropic`)
3. Migrate: `python manage.py migrate chatbot`
4. Ingest docs: `python manage.py ingest_chatbot_knowledge`  
   (use `--skip-embeddings` if OpenAI embeddings are unavailable; keyword RAG still works)

## Action tools (bob pages)

| Tool | What it does | Confirm? |
|------|----------------|----------|
| `get_billing_report` | Membership billing for a month | No (read-only) |
| `list_my_pages` / `create_page` | Pages under configured owner email | Create needs `confirm=true` |

Page ownership is **not** the requesting GM. Set per tenant:

- Django admin → **Tenant settings** → `chatbot_page_owner_email`
- or env fallback: `CHATBOT_PAGE_OWNER_EMAIL=owner@example.com`

That email must be an active membership with `user_id` in the tenant.

Example prompts:

- "Show billing for 2026-07"
- "Create a page named Ops Home"
- "List my pages"
- "List background job types"
- "Enqueue score_leads for this tenant" → bot asks confirm → call with `confirm=true`
- "List pyro jobs" / "Run dispatch_data_sync" (confirm required)

Dangerous jobs (purge, unassign, webhook, etc.) still need explicit user confirmation.
