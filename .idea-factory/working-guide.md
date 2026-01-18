# How We Work Together

## The Goal
**Pursuit of the perfect prompt**: Accumulate context through conversation until building becomes obvious and trivial. We don't start coding until readiness ≥ 70%.

## Session Startup Protocol

When you say **"continue"** in this project:

1. **Read state:**
   - `.claude/state/next.md` - what's queued locally
   - `.claude/state/readiness.json` - how ready to build
   - `.claude/insights/all.json` - what we've decided
   - `.claude/signals/inbox.md` - your thoughts while away
   - `.idea-factory/questions-tracker.json` - conversation progress

2. **Check GitHub:**
   - `gh issue list` - blockers, signals, open questions
   - `gh pr list` - pending experiments
   - `git log --since="last session"` - what changed

3. **Greet you with context:**
   ```
   Welcome back to [Project Name] (IDEA-XXX).

   Last session: X days ago
   Readiness: XX% (status)

   Next steps: [from next.md]

   GitHub: [issues/PRs if any]
   Inbox: [signals if any]

   What should we work on?
   ```

4. **If readiness < 100%:**
   - Start/resume structured questioning (see `conversation-protocol.md`)
   - Ask one question at a time
   - Track answers in questions-tracker.json
   - Update readiness after each answer
   - Show progress every 5 questions

5. **If readiness = 100%:**
   - Present "The Perfect Prompt"
   - Wait for approval
   - Build on approval

## Conversation Style

### Brevity by Default
- **Simple questions:** 1-3 sentences max
- **Complex decisions:** Ultrathink first, then answer
- **No preamble/postamble** unless you ask for it
- Brief confirmations after completing tasks

### When to Elaborate
- You ask "ultrathink through this"
- Decision has significant implications
- Multiple viable approaches exist
- I'm proposing architecture changes

### Tool Usage Rules
- **TodoWrite:** Multi-step tasks (3+ steps), track progress
- **Bash:** Git operations, system commands (NOT file reading)
- **Read:** Always before Edit or Write to existing files
- **Glob/Grep:** Finding files or searching code
- **Never use bash for:** cat, echo, grep (use dedicated tools)

## Git & GitHub Workflow

### Committing Changes

**When you ask me to commit:**
1. Run `git status` + `git diff` to see changes
2. Draft commit message following style:
   ```
   type(scope): brief description

   Detailed explanation if needed.

   🤖 Generated with Claude Code

   Co-Authored-By: Claude <noreply@anthropic.com>
   ```
3. Add relevant files: `git add <files>`
4. Commit with message
5. Verify with `git status`

**Commit message types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Maintenance

**Auto-close issues:**
- Include in commit: "fixes #7" or "closes #12"

### Branches for Experiments

When running experiments:
```bash
git checkout -b experiment/description
# ... make changes ...
git commit -m "feat(experiments): description [exp:abc123]"
gh pr create --title "Experiment: Description" --body "..."
```

### Pull Requests

I'll create PRs for:
- Completed experiments (with results)
- Significant features (if you want review)
- Cross-project patterns (for documentation)

## Insights & Learning

### What I Track Automatically

In `.claude/insights/all.json`:
- **Decisions:** "Let's use X" (definitive choices)
- **Approaches:** "We'll implement with Y" (how we'll do it)
- **Assumptions:** "Probably Z" (needs validation)
- **Blockers:** "Don't know how to..." (stuck points)
- **Signals:** "I tried X and..." (real-world observations)

### Trigger Phrases I Listen For

**Decisions:**
- "Let's use..."
- "We should..."
- "We'll go with..."
- "Decision: ..."

**Assumptions:**
- "Probably..."
- "I assume..."
- "Should be..."
- "Hypothesis: ..."

**Blockers:**
- "Don't know how to..."
- "Not sure about..."
- "Stuck on..."
- "Need to figure out..."

**Signals:**
- "I tried..."
- "I tested..."
- "I saw..."
- "I found..."

### Readiness Calculation

Score = weighted average:
- **Core value clarity (30%):** Why this matters, who it's for
- **MVP scope (25%):** What we're building, clear boundaries
- **Technical approach (25%):** How we'll build it
- **Assumptions validated (10%):** Tested hypotheses
- **Blockers resolved (10%):** Nothing stuck

**Thresholds:**
- 0-40%: **Exploring** (keep talking, ask questions)
- 40-70%: **Designing** (getting clearer, almost ready)
- 70-85%: **Ready to build** (start coding)
- 85-100%: **Ship it!** (clear execution path)

## Experiments & Validation

### When to Run Experiments

When we have an assumption that needs validation:
1. Create experiment record (if using database)
2. Create branch: `experiment/description`
3. Implement test
4. Document results
5. Update insights with findings
6. PR if successful, close branch if failed

### Experiment Template

See `experiment-template.md` for structure.

## GitHub Issues as External Memory

### When to Create Issues

- **Blockers:** "Need to figure out OAuth scopes" → Issue
- **Signals:** Your thoughts while away → Issue with `signal` label
- **Open questions:** From idea card → Issues on init
- **Future features:** "Eventually add X" → Issue with `future` label

### Issue Labels

- `blocked`: Can't proceed without resolving
- `signal`: Observation/idea from outside Claude
- `experiment`: Hypothesis to test
- `future`: Post-MVP feature
- `bug`: Something broken
- `question`: Needs research/decision

## Related Ideas

This idea is related to:
- [Populated from idea card "related" field]

When working on similar problems, I'll:
1. Check related idea repos for patterns
2. Read their `.claude/insights/all.json`
3. Suggest reusing approaches that worked
4. Reference their implementations

## Update This Guide

This guide is synced from master templates at:
`~/idea_factory/templates/.idea-factory/working-guide.md`

If we improve our methodology:
1. Update master template
2. Run `~/.claude/tools/sync-templates.sh`
3. All 147+ ideas get the update

---

**Last synced:** 2026-01-10
**Version:** 1.0
**Master location:** `~/idea_factory/templates/.idea-factory/`
