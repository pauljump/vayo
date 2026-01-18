# Conversation Protocol: 0% → 100% Readiness

## The Goal
Ask customized questions, one at a time, until we reach **100% readiness** = one perfect prompt to build production app.

## How It Works

When you say **"continue"** after project setup, I:
1. Read the idea card to understand the project type and scope
2. Generate custom questions based on what's needed for production
3. Ask questions one at a time through 5 phases
4. Track answers in `.idea-factory/questions-tracker.json`
5. Update readiness score after each answer
6. At 100%, present "The Perfect Prompt"
7. You approve → I build

## Question Framework (Adaptive)

### Phase 1: Core Value (0-30%)
**Goal:** Understand the problem and user

**Question areas (customize based on project):**
- Who is this for? (primary user persona)
- What's the pain? (specific problem moment)
- What's the fix? (how this solves it)
- Why now? (timing/opportunity)
- What's the competition? (alternatives today)
- Why will this win? (differentiation)

**Triggers 30% readiness when:**
- Clear user persona
- Specific pain point (not generic)
- Simple solution statement
- Differentiation from competition

### Phase 2: MVP Scope (30-55%)
**Goal:** Define exactly what we're building

**Question areas (customize based on project type):**
- Primary user action? (the core workflow)
- Where does it happen? (interface/context)
- What's the minimum? (simplest version)
- What's out of scope? (NOT building in v1)
- How do they discover it? (distribution)
- Onboarding flow? (steps to first success)

**Adapt to project type:**
- **Mobile:** Widget placement, notifications, shortcuts
- **API:** Endpoints, request/response formats, auth flow
- **Web app:** Pages, navigation, responsive breakpoints
- **Data pipeline:** Data sources, transformation steps, output format

**Triggers 55% readiness when:**
- Crystal clear primary action/workflow
- Defined scope boundaries
- Simple onboarding/setup (≤3 steps)

### Phase 3: Technical Approach (55-75%)
**Goal:** Understand how to build it

**Question areas (customize based on project type):**
- Platform/runtime? (where does it run?)
- Tech stack? (languages, frameworks, libraries)
- Key APIs/services? (what integrations needed?)
- Backend requirements? (serverless, database, hosting)
- Third-party services? (payments, auth, analytics)
- Deployment? (how does it ship?)

**Adapt to project type:**
- **Mobile:** iOS/SwiftUI, Apple APIs, App Store
- **API:** Node/Python, database, hosting, rate limits
- **Web app:** React/Next.js, auth, Vercel/Railway
- **Data pipeline:** Python/SQL, scheduling, monitoring
- **Chrome extension:** Manifest v3, permissions, storage

**Triggers 75% readiness when:**
- Platform decisions made
- API feasibility confirmed
- Tech stack chosen
- Deployment path clear

### Phase 4: Risk Mitigation (75-90%)
**Goal:** Validate assumptions and blockers

**Question areas (customize based on project):**
- Riskiest assumption? (what could kill this?)
- How to validate? (quickest test)
- Platform/approval risk? (Apple/Chrome/AWS policies)
- User permissions? (what access needed?)
- Monetization timing? (when/how to charge)
- Pricing? (exact amount and model)

**Adapt to project type:**
- **Mobile:** App Store review, privacy permissions
- **API:** Rate limits, abuse prevention, scaling costs
- **Web app:** SEO, performance, browser support
- **Data pipeline:** Data quality, API rate limits, costs
- **Chrome extension:** Chrome Web Store policies, CSP

**Triggers 90% readiness when:**
- Top 3 risks identified
- Validation plan for each
- Platform approval precedent found
- Pricing/monetization decided

### Phase 5: The Perfect Prompt (90-100%)
**Goal:** Synthesize everything into build instructions

**Step 1: Review synthesis**
Present a complete summary customized to project type:
- User: [persona]
- Problem: [pain point]
- Solution: [how it fixes it]
- MVP: [exact scope]
- Stack: [tech choices]
- Risks: [top 3 + mitigation]
- Pricing/monetization: [model]

**Step 2: User edits**
User corrects anything wrong

**Step 3: Generate perfect prompt**
Synthesize into production build instructions adapted to project type:

```
Build a [type] that [core value] for [user].

Core Feature:
- [Primary workflow in detail]

Scope:
- [In scope items]
- NOT: [Out of scope]

Tech:
- [Stack/platform details]
- [Key APIs/integrations]
- [Backend/hosting if needed]

Setup/Onboarding:
1. [Step 1]
2. [Step 2]
3. [Step 3]

Risks:
- [Risk 1]: [Mitigation]
- [Risk 2]: [Mitigation]

[Pricing/Distribution section if applicable]

Build this as production-ready code with proper error handling,
[accessibility for mobile/security for API/performance for web/etc].
```

**Step 4: User approval**
"yes" → Start building

**Triggers 100% readiness when:**
- User approves the perfect prompt

## Conversation Style

### One Question at a Time
- Ask ONE question
- Wait for your answer
- Acknowledge briefly (1 line)
- Ask next question

### Example Flow
```
Me: Who is this for?

You: Parents with kids who have iPads and won't turn them off

Me: Got it - parents managing screen time. What's the pain?

You: I have to go through 7 taps in Settings → Screen Time
to block their iPad when they don't listen

Me: Clear - buried controls during discipline moments. What's the fix?

You: One tap on a widget to instantly block everything
```

### No Explaining
- Don't explain why I'm asking (unless you ask)
- Don't preview next questions
- Just ask, listen, move on

### Track Progress
After every 5 questions, show:
```
Readiness: 42% (designing)
Next: Define MVP scope
```

## Dynamic Question Generation

**Read the idea card first**, then generate questions based on:
1. **Project type** (mobile, API, web, data, extension, CLI, etc)
2. **Complexity** (simple CRUD vs complex integration)
3. **User type** (consumer, developer, enterprise)
4. **Platform** (iOS, web, serverless, etc)

**Examples:**

*For a mobile app:*
- Ask about widgets, notifications, App Store
- Skip backend questions if it's purely client-side
- Focus on Apple APIs and permissions

*For an API:*
- Ask about endpoints, auth, rate limits
- Skip UI/UX questions
- Focus on scalability and documentation

*For a data pipeline:*
- Ask about data sources, transforms, scheduling
- Skip user onboarding questions
- Focus on data quality and monitoring

**Adaptive behavior:**
- If user answers multiple things at once → acknowledge all, skip ahead
- If user is uncertain → offer 2-3 options based on research
- If user says "I don't know" → research or suggest quick validation
- Always one question at a time

## Completion

When readiness hits 100%:
1. Show "The Perfect Prompt"
2. Wait for approval
3. Build the entire app
4. Deploy to TestFlight
5. Share build link

---

**Last updated:** 2026-01-11
**Version:** 2.0
