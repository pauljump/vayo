# GitHub Integration Guide

## Why GitHub for Every Idea

GitHub isn't just for deployment - it's **external memory** that persists when conversation context expires.

### The Stack
- **`.claude/`** = Local conversation memory (insights, state, readiness)
- **Git commits** = Code history and decision trail
- **GitHub issues** = External thought inbox + blockers
- **GitHub PRs** = Experiment results + review process
- **GitHub Actions** = Automation (readiness updates, catalog sync)

## Setup (Done Automatically)

When you run `~/.claude/tools/init-idea-folder.sh`, it creates:

```
your-project/
  .git/                   # Git initialized
  .github/
    workflows/
      claude-sync.yml     # Auto-updates readiness, syncs catalog
  README.md               # Generated from idea card
  IDEA-CARD.md           # Reference copy of card
```

And creates GitHub repo: `github.com/pauljump/your-project`

## Using Issues as External Memory

### Scenario 1: You Think of Something While Away

Instead of losing the thought:
```bash
# From anywhere (terminal, phone with GitHub app):
gh issue create \
  --repo pauljump/your-project \
  --title "Try Hypothesis.is integration" \
  --body "Saw competitor using it. Check if better than our W3C approach." \
  --label signal
```

Next session, I'll read this issue and research it.

### Scenario 2: We Hit a Blocker

```
You: "How do we handle OAuth scopes for plugins?"
Me: "I don't know the best practice here. Let me create a blocker issue."

# I run:
gh issue create \
  --title "Research: OAuth scope best practices for plugin system" \
  --label blocked \
  --body "Need to figure out granular scopes for app→plugin access..."

# Then I can research across sessions
```

### Scenario 3: Open Questions from Idea Card

When scaffolding a new idea, I automatically create issues from the card's "Open questions" section:

```
IDEA-149 open questions:
1. Will renters pay $4.99 for reports?
2. How to capture lease outcomes?
3. What's optimal comparable selection?

→ Creates 3 GitHub issues labeled "question"
```

## Using PRs for Experiments

### Workflow

```bash
# Starting experiment:
git checkout -b experiment/bayesian-ab-testing

# Make changes, commit:
git commit -m "feat(experiments): Bayesian A/B testing [exp:abc123]"

# Create PR:
gh pr create \
  --title "Experiment: Bayesian A/B Testing" \
  --body "
## Hypothesis
Bayesian stats converge faster than frequentist for low-traffic tests.

## Method
Implement Thompson sampling, compare to chi-square on simulated data.

## Results
[Will update after running]
"

# After testing, update PR description with results
# Merge if successful, close if failed
```

### Why PRs?
- **Review process:** You can review on phone, approve when ready
- **Experiment history:** Searchable forever
- **Clean main branch:** Failed experiments don't pollute history
- **Discussion:** Comment on specific code lines

## GitHub Actions (Automation)

### `.github/workflows/claude-sync.yml`

Runs on every push to main:

```yaml
name: Claude Sync

on:
  push:
    branches: [main]

jobs:
  update-readiness:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Update readiness score
        run: |
          # Parse .claude/insights/all.json
          # Count decisions, approaches, resolved blockers
          # Calculate new readiness score
          # Update .claude/state/readiness.json
          # Commit if changed

      - name: Sync to master catalog
        run: |
          # Update idea_factory/collections/concepts/catalog.csv
          # Update idea card if status/readiness changed
          # Commit to idea_factory repo
```

This keeps the master catalog in sync with each idea's progress.

## Cross-Project Intelligence

### Checking Related Ideas

When working on IDEA-050 (PowerCredit), idea card says:
```
related: [IDEA-007, IDEA-149]
```

I can:
```bash
# Clone related repos temporarily:
git clone https://github.com/pauljump/citycell /tmp/citycell
git clone https://github.com/pauljump/stuyscrape /tmp/stuyscrape

# Read their insights:
cat /tmp/citycell/.claude/insights/all.json
cat /tmp/stuyscrape/.claude/insights/all.json

# Extract patterns:
"IDEA-007 used NYC Open Data API with 50K batch imports - copy that pattern"
"IDEA-149 has event-sourcing architecture - reuse for credit tracking"

# Create issues in PowerCredit:
gh issue create --title "Implement event-sourcing (see IDEA-149 RentIntel)" \
  --body "Reference: github.com/pauljump/stuyscrape/blob/main/architecture.md"
```

## Repo Naming Convention

- Idea has `directory` field in card → use that name
- No directory field → kebab-case from title
- Examples:
  - IDEA-001 directory=appstore → `github.com/pauljump/appstore`
  - IDEA-019 directory=menupilot → `github.com/pauljump/menupilot`
  - IDEA-050 directory=powercredit → `github.com/pauljump/powercredit`

## GitHub CLI Cheat Sheet

```bash
# Issues
gh issue list                          # See all issues
gh issue list --label blocked          # Just blockers
gh issue create --title "..." --body "..." --label signal
gh issue close 15 --comment "Resolved by testing X"

# PRs
gh pr list
gh pr create --title "..." --body "..."
gh pr view 12
gh pr merge 12

# Repos
gh repo view                           # View current repo
gh repo create your-project --public   # Create new repo

# Branches
git checkout -b feature/description
git push -u origin feature/description
```

## When GitHub Fails You

If GitHub is down or you're offline:
- **Everything still works locally** (`.claude/` is local-first)
- Issues can be created as `.claude/signals/inbox.md` entries
- PRs can be branches without remote push
- When GitHub returns, sync everything

---

**Remember:** GitHub extends memory beyond conversation context limits. Use it liberally.
