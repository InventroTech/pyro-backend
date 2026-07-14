---
name: review-others-pr
description: >-
  Review someone else's GitHub pull request in this repo using Cursor.
  Use when the user asks to review a PR, review others' PRs, check out a PR,
  or paste a pull request URL/number.
---

# Review others' PRs (pyro-backend)

Follow [Cursor's reviewing-others-PRs flow](https://cursor.com/for/code-review#reviewing-others-prs).

## Steps

1. **Identify the PR** — number or URL (repo: `InventroTech/pyro-backend`).
2. **Check out locally** (prefer GitHub CLI):
   ```bash
   gh pr checkout <PR_NUMBER>
   ```
   If `gh` is missing, use:
   ```bash
   git fetch origin pull/<PR_NUMBER>/head:pr-<PR_NUMBER> && git checkout pr-<PR_NUMBER>
   ```
3. **Load context**
   - `gh pr view <PR_NUMBER>` (title, body, base branch)
   - `gh pr diff <PR_NUMBER>` or `git diff origin/<base>...HEAD`
   - Read `.cursor/BUGBOT.md` and apply those review rules
4. **Review for**
   - Tenant isolation / auth permissions
   - Soft-delete and model conventions
   - Missing or weak tests under `src/tests/`
   - Secrets, raw SQL, unmanaged-model mistakes
5. **Report findings** as a short review:
   - Summary (1–2 sentences)
   - Blocking issues (if any)
   - Non-blocking suggestions
   - Test gaps
6. **Optional GitHub comments** — only if the user asks:
   ```bash
   gh pr review <PR_NUMBER> --comment --body "..."
   # or --request-changes / --approve when they explicitly ask
   ```

## Do not

- Push, merge, or approve unless the user explicitly asks
- Switch away from the PR branch without saying so
- Rewrite unrelated files while reviewing
