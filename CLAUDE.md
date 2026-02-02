# Claude Code Instructions for This Project

**This is an Idea Factory project.**

## When I Start a Session

1. **If the user says "continue":**
   - Read `.claude/current_state.md` FIRST (rolling summary, 1-2 pages)
   - Read `.idea-factory/working-guide.md` for full protocol
   - Check GitHub issues: `gh issue list`
   - Greet the user with current status from current_state.md
   - ONLY dig into `.claude/insights/all.json` if user asks specific historical questions

2. **If the user gives a task directly:**
   - Just do the task
   - Follow the working guide for tool usage, git workflow, and conversation style

## When I End a Session

**ALWAYS before ending:**
1. Check for uncommitted work: `git status`
2. If there are changes:
   - `git add -A`
   - `git commit -m "type(scope): description"`
   - `git push`
3. Confirm: `git status` should show "up to date with origin"

**Never end a session with uncommitted or unpushed work.**

## Core Principles

**From `.idea-factory/working-guide.md`:**
- Don't code until readiness ≥ 70%
- Use readiness-driven development (exploring → designing → building)
- Track insights automatically (decisions, assumptions, blockers)
- Be brief by default, elaborate when asked
- Use TodoWrite for multi-step tasks
- Update readiness after major conversations
- **Commit and push at end of every session**

## The Working Guide

All methodology details are in `.idea-factory/working-guide.md`. Read it when:
- User says "continue"
- You need clarification on process
- You're about to commit, create PR, or run experiments

## Quick Reference

**Key files (read in this order):**
- `.claude/current_state.md` - **READ FIRST** (rolling summary, always current)
- `.idea-factory/working-guide.md` - How we work together
- `.idea-factory/idea-context.json` - Project metadata
- `.claude/state/readiness.json` - Detailed readiness breakdown
- `.claude/insights/all.json` - Full history (only if user asks)
- `.claude/archives/` - Historical sessions (only if needed)

**Commands I understand:**
- "continue" - Load full context and resume where we left off
- "save-session" - Capture current session (if you want me to log insights)
- Check `.claude/commands/` for custom slash commands

---

**Last synced from:** `~/Desktop/projects/idea_factory/templates/CLAUDE.md`
