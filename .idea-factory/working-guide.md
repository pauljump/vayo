# How We Work Together

## The Goal
**Pursuit of the perfect prompt**: Accumulate context through conversation until building becomes obvious and trivial. We don't start coding until readiness ≥ 70%.

## Session Startup Protocol

When you say **"continue"** in this project:

1. **Read current state FIRST:**
   - `.claude/current_state.md` - Rolling summary (1-2 pages, always current)
   - This is the compressed view optimized for quick context restoration

2. **Read additional state if needed:**
   - `.claude/state/next.md` - what's queued locally
   - `.claude/state/readiness.json` - detailed readiness breakdown
   - `.claude/signals/inbox.md` - your thoughts while away
   - `.idea-factory/questions-tracker.json` - conversation progress
   - `.claude/insights/_index.json` - compiled insights (or individual files if specific lookup needed)

3. **Check GitHub:**
   - `gh issue list` - blockers, signals, open questions
   - `gh pr list` - pending experiments
   - `git log --since="last session"` - what changed

4. **Greet you with context:**
   ```
   Welcome back to [Project Name] (IDEA-XXX).

   Last session: X days ago
   Readiness: XX% (status)

   Recent decisions: [last 3-5 from current_state.md]

   Open blockers: [from current_state.md]

   Next steps: [from current_state.md]

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

### Committing Changes and Pushing to GitHub

**When you ask me to commit:**
1. Run `git status` + `git diff` to see changes
2. Draft commit message following style:
   ```
   type(scope): brief description

   Detailed explanation if needed.
   ```
3. Add relevant files: `git add <files>`
4. Commit with message
5. **ALWAYS push to GitHub immediately: `git push`**
6. Verify with `git status` after push

**Commit message types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `refactor`: Code refactoring
- `test`: Tests
- `chore`: Maintenance

**Auto-close issues:**
- Include in commit: "fixes #7" or "closes #12"

**CRITICAL:** Push after every commit. Work should land in GitHub immediately so it's backed up and visible across machines.

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

In `.claude/insights/` (one file per insight - append-only):
- **Decisions:** "Let's use X" (definitive choices) → `decisions/YYYY-MM-DDTHH-MM-SS_slug.json`
- **Approaches:** "We'll implement with Y" (how we'll do it) → `approaches/...`
- **Assumptions:** "Probably Z" (needs validation) → `assumptions/...`
- **Blockers:** "Don't know how to..." (stuck points) → `blockers/...`
- **Signals:** "I tried X and..." (real-world observations) → `signals/...`

The compiled view is in `.claude/insights/_index.json` (generated, never edit directly)

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

**Checklist-based system** (score = % of items checked):

**Required checklist items:**
- [ ] **Core value defined** - What problem does this solve? For whom?
- [ ] **Target user identified** - Who specifically will use this?
- [ ] **MVP scope defined** - What's the smallest useful version?
- [ ] **Technical approach decided** - How will we build this?
- [ ] **Critical assumptions listed** - What could invalidate this idea?
- [ ] **Blockers identified** - What's preventing progress?
- [ ] **First experiment run** - Have we tested anything yet?

**Score = checked items / total items × 100**

Example: 4/7 items checked = 57% readiness

**Thresholds:**
- 0-40%: **Exploring** (keep talking, ask questions)
- 40-70%: **Designing** (getting clearer, almost ready)
- 70-85%: **Ready to build** (start coding)
- 85-100%: **Ship it!** (clear execution path)

Each item is binary (done or not). Add notes to items for context.

**Updating readiness:**
When a checklist item is complete, Claude should:
1. Update `.claude/state/readiness.json` with `jq` to mark item as checked
2. Add notes to the item explaining what was decided
3. Recalculate score: `(checked count / total count) * 100`
4. Update status based on new score

Example:
```bash
jq '.checklist[0].checked = true |
    .checklist[0].notes = "Help NYC renters find rent-stabilized apartments" |
    .score = 14' .claude/state/readiness.json > tmp && mv tmp .claude/state/readiness.json
```

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

## Advanced Techniques (Claude Code Team)

### 1. Parallel Sessions for 10x Productivity

**The single biggest productivity unlock:**

Work on 3-5 ideas simultaneously using git worktrees:
```bash
# Create worktrees for parallel work
cd ~/Desktop/projects/idea-1
git worktree add ../idea-1-feature-a
git worktree add ../idea-1-feature-b

# Each gets its own Claude session
# Context switching is instant, no merge conflicts
```

**Alternative:** Use separate git clones if you prefer (simpler but takes more disk space)

**Why this works:**
- Each Claude maintains separate context
- Work proceeds in parallel on different branches
- No context thrashing between tasks
- Ship multiple features/ideas per day instead of per week

### 2. Plan Mode is Your Secret Weapon

**Start every complex task in plan mode. Pour energy into the plan.**

The better your plan, the more likely Claude will 1-shot the implementation.

**Advanced technique from team:**
- First Claude writes the plan
- Second Claude reviews it as a staff engineer
- Iterate until plan is bulletproof
- Then implement (usually succeeds first try)

**When something goes sideways:**
- STOP coding immediately
- Switch back to plan mode
- Fix the plan
- Resume with corrected approach

**Rule:** If Claude is making mistakes, the plan is incomplete.

### 3. Invest Ruthlessly in CLAUDE.md

**After every correction, end with:**
> "Update CLAUDE.md so you don't make that mistake again."

Claude is eerily good at writing rules for itself.

**Over time:**
- **Ruthlessly edit** - Delete rules that don't matter
- **Consolidate** - Merge similar guidelines
- **Prioritize** - Put critical rules at top
- **Test** - Verify rules actually prevent mistakes

**CLAUDE.md compounds.** Every session makes Claude better at your project.

### 4. Build Your Personal Skill Library

**If you do something more than once a day, turn it into a skill or command.**

```bash
# Example: Create /techdebt command
vim ~/.claude/commands/techdebt.md
```

Content:
```markdown
Find duplicated code and suggest refactors. Look for:
- Copy-pasted functions
- Similar patterns across files
- Opportunities to DRY up code
Present as actionable list with file:line references.
```

**Run at end of every session:**
```
/techdebt
```

**Team tips:**
- Build skills library in `~/.claude/skills/`
- Commit to git (reuse across machines)
- Share with team (everyone benefits)
- Skills compound across all projects

### 5. Let Claude Fix Bugs Autonomously

**Most bugs Claude can fix without micromanagement.**

**Patterns that work:**
```bash
# From Slack/GitHub
"Fix: [paste entire bug thread]"

# From CI
"Go fix the failing CI tests"

# From logs
"Fix: [paste docker logs]"
```

**Key insight:** Don't micromanage HOW to fix it. Claude will:
1. Read the error
2. Find relevant code
3. Understand root cause
4. Implement fix
5. Verify it works

**Your job:** Point at the problem, verify the fix.

**Works especially well with:**
- Integration with Slack MCP (paste threads directly)
- CI/CD logs (point Claude at failing tests)
- Production monitoring (paste error traces)

### 6. Combine Techniques for Maximum Impact

**Power combo from team:**

1. **Morning:** Start 3-5 parallel sessions in plan mode
2. **Plan phase:** Each Claude develops bulletproof plan
3. **Invest:** Update each project's CLAUDE.md with corrections
4. **Build:** Claude 1-shots implementations (plans are solid)
5. **Evening:** Run `/techdebt` on all projects
6. **Bug fixes:** Autonomous (paste logs, say "fix")

**Result:** 10x shipping velocity with same time investment.

## Related Ideas

This idea is related to:
- [Populated from idea card "related" field]

When working on similar problems, I'll:
1. Check related idea repos for patterns
2. Read their `.claude/insights/_index.json`
3. Suggest reusing approaches that worked
4. Reference their implementations

## Archive Old Sessions

**Every 10 sessions, archive to prevent context bloat:**

```bash
~/.claude/tools/archive-sessions.sh
```

**What it does:**
- Compresses sessions 1-10, 11-20, etc. into `.claude/archives/`
- Moves resolved blockers and validated assumptions to archive
- Updates `current_state.md` with latest state
- Keeps active decisions, open blockers, unvalidated assumptions current

**Files:**
- `.claude/current_state.md` - Always read this first (1-2 pages)
- `.claude/archives/sessions-001-010.md` - Historical sessions
- `.claude/archives/decisions-archive.json` - Resolved items

**Rule:** Keep `current_state.md` under 2 pages. If it grows beyond that, archive more aggressively.

## Session End Protocol

**At the end of EVERY session:**

1. **Check for uncommitted work:**
   ```bash
   git status
   ```

2. **If there are changes, commit them:**
   ```bash
   git add -A
   git commit -m "type(scope): description of session work"
   git push
   ```

3. **Update current_state.md if significant progress:**
   - Summarize what changed
   - Update readiness if it moved
   - Note next steps

4. **Confirm push succeeded:**
   ```bash
   git status  # Should show "up to date with origin/main"
   ```

**Why this matters:**
- Work is backed up immediately
- Visible across machines
- No stale local changes
- GitHub is always current

**Never end a session with uncommitted or unpushed work.**

## Update This Guide

This guide is synced from master templates at:
`~/idea_factory/templates/.idea-factory/working-guide.md`

If we improve our methodology:
1. Update master template
2. Run `~/.claude/tools/sync-templates.sh`
3. All 147+ ideas get the update

---

**Last synced:** 2026-02-01
**Version:** 1.3 (checklist-based readiness)
**Master location:** `~/idea_factory/templates/.idea-factory/`
