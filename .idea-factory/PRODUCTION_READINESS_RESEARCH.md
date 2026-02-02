# Production Readiness Framework - Research Foundation

**Source:** Deep research analysis on production readiness requirements across project types
**Date:** 2026-01-18
**Purpose:** Reference document for generating custom questions during questioning protocol

---

## Part 1: Universal Questions (All Projects)

Every project must answer these fundamental questions:

### 1. What problem are we solving, and for whom?
- Clearly identify the single primary pain point
- Specific target user or customer who experiences that pain
- If you can't state the user's core problem in 1-2 sentences, not ready
- Understand how users handle the problem now and why it's important to solve
- **Red flag:** Vague ambition ("build a social network") without tangible problem

### 2. Why does this problem need solving now (and what's the value)?
- Articulate unique value proposition or benefit
- Why will target users care?
- What job-to-be-done does your product accomplish that alternatives fail at?
- Compelling reason (unmet need, inefficiency, cost savings) not just "it would be cool"
- This is the value hypothesis (Lean Startup)
- Validate through customer interviews/surveys before building

### 3. Who are the stakeholders and what are their goals?
- End users, customers (if different), and the business
- Success criteria for each
- For users: what outcome or improvement are they seeking?
- For business: metrics (revenue, growth, engagement) that define success
- How product aligns with organizational objectives
- If you cannot define who benefits and how, concept isn't grounded enough

### 4. What are the top assumptions that must be true for this to succeed?
- List critical assumptions - if wrong, would kill the project
- User behavior assumptions ("people will use this weekly")
- Market size ("at least 5% of target users will adopt")
- Technical feasibility ("the AI API can handle our use case")
- Business viability ("we can acquire users at $X cost")
- Identify riskiest assumptions and prioritize testing them first
- **Example:** "build it and they will come" is often fatal unvalidated assumption

### 5. What does a simple version (MVP) look like?
- Define minimum feature set needed to deliver core value
- "If we stripped this down to solve just the primary problem, what would that look like?"
- Prevents scope bloat and forces clarity on what truly matters
- **Red flag:** Can't decide what's core vs. nice-to-have = don't understand core value
- By defining MVP, ensure you can test idea with minimal resources and gather feedback

### 6. How will we measure success and know if we're on track?
- Establish key metrics or criteria before building
- Usage metric (DAU, retention rate), performance metric, customer satisfaction
- Tie metrics to the problem (e.g. "reduce time to do X by 50%")
- Decide on feedback loops: user testing, surveys, analytics
- Ensures you're prepared to iterate based on evidence, not guesses

---

## Part 2: Project-Type Decision Tree

### Root Decision: Project Category

**Q1: What type of project is this?**
- Mobile App (iOS/Android)
- Web Application (SaaS/consumer web)
- API/Backend Service
- Data Pipeline/ETL
- Browser Extension
- CLI Tool
- Desktop Application

### Branch Logic by Type

#### If Mobile App:
1. **Platforms** → iOS, Android, or both? Native or cross-platform?
   - If native iOS/Android: resources for two codebases, store requirements
   - If cross-platform: verify framework can access specialized features
2. **Online or Offline** → Does it need a backend/server?
   - If backend needed: also plan API service (see API path below)
   - If no backend: device storage, future online feature planning
3. **App Store Compliance** → Does anything violate App Store/Google Play policies?
   - Money transactions: must use in-app purchase on iOS for digital goods
   - User-generated content: moderation plan to meet content guidelines
   - If potential policy issue: research guidelines, find precedents

#### If Web Application:
1. **Nature** → Content-oriented vs. interactive?
   - If content-heavy: static site or CMS for MVP, focus on SEO
   - If interactive web app: SPA or multi-page, backend requirements
2. **Hosting** → Serverless, static front-end, or traditional server?
   - Static/JAMstack: Vercel/Netlify, backend services via APIs
   - Traditional server: cloud provider, scalability planning
   - Serverless: constraints (execution limits), event-driven architecture
3. **User Management** → How will users authenticate?
   - Quick onboarding: passwordless or social login
   - Enterprise/B2B: plan SSO integration
   - Email verification, password resets, 2FA affect architecture
4. **Data & Compliance** → What data stored, compliance needs?
   - Personal data: GDPR compliance (consent, deletion)
   - Sensitive data: additional security and legal review

#### If API/Backend Service:
1. **Public or Internal?** → Who are the consumers?
   - Public developer tool: usage plans, authentication, documentation, rate limiting
   - Internal microservice: simplified auth, resilience focus
2. **Data Persistence** → Database and architecture needs?
   - Choose database for access patterns (SQL vs NoSQL)
   - High throughput: horizontal scalability planning
   - Caching strategy (Redis) to avoid performance bottlenecks
3. **Critical Integrations** → Dependencies on third-party APIs?
   - Usage limits or terms of service that could block
   - Fallback behavior for API failures
4. **Versioning** → How to handle changes without breaking clients?
   - Version API from start (/v1/ /v2/ routes)
   - Error handling conventions (meaningful error codes/messages)

#### If Data Pipeline/ETL:
1. **Frequency** → Batch or Real-Time?
   - Real-time/streaming: Kafka, event-driven, complexity (out-of-order events)
   - Batch: schedule determination, simpler but latency consideration
2. **Data Sources and Sinks** → Inputs and outputs?
   - Access confirmation (credentials, permissions)
   - Source reliability, format/schema expectations
   - Schema drift handling plan
3. **Data Volume** → Thousands or billions?
   - Large scale: distributed processing (Spark), scaling strategy
   - Moderate scale: simple script or managed ETL tool
4. **Failure and Monitoring** → What happens if pipeline fails?
   - Error catching and alerting
   - Retries for transient errors
   - Data validation at each stage

#### If Browser Extension:
1. **Target Browsers** → Chrome only or multiple?
   - Chrome only: stick with MV3
   - Multiple browsers: WebExtensions standard, test in each
2. **Permissions Scope** → What permissions truly needed?
   - Least privilege principle
   - Optional permissions for runtime requests
   - Privacy policy if handling user data
3. **Technical Constraints (MV3)** → Work within MV3 limits?
   - Service workers not persistent background scripts
   - DeclarativeNetRequest for network filtering
   - No remote code execution
4. **Data Storage** → How much data to save?
   - chrome.storage quotas (sync ~100KB)
   - External backend if significant data
   - User account needs for cross-device sync

#### If CLI Tool:
1. **Target OS** → Which operating systems?
   - Linux/macOS only: leverage Bash/Python
   - Windows support: cross-platform language (Go, Rust, Python)
2. **Distribution** → How will users install?
   - Package manager (pip, npm, gem): meet ecosystem requirements
   - Direct download: precompiled binaries, CI setup
3. **Usage and UX** → Command structure and options?
   - Essential subcommands and flags
   - Configuration handling (config files, env variables)
   - CLI conventions (help text, version flag)
4. **Dependencies** → What does CLI rely on?
   - Bundled or documented prerequisites
   - Environment checks and user guidance

#### If Desktop Application:
1. **Platform Choice** → Windows, macOS, Linux?
   - Cross-platform: Electron, Qt, Flutter (trade-offs: app size, memory)
   - Single OS: native approach for better UX
2. **Distribution** → App stores or direct download?
   - App stores: comply with store rules, sandboxing
   - Direct download: code signing to avoid security warnings
3. **Auto-Update** → Update strategy?
   - Integrate auto-updater early
   - Store's updating vs. custom mechanism
4. **Native Integrations** → Deep OS integration needed?
   - File system access, permission prompts
   - Background running, startup configuration
   - OS-specific APIs and sandboxing constraints

---

## Part 3: Phase-Specific Question Banks

### Phase 1: Core Value (0-30%)

**Goal:** Understand the problem and user

**Required Questions:**
- What user problem are we solving? (must define before anything else)
- How do users solve this today? (indicates competition and improvement needed)
- How intense or frequent is this problem? (daily annoyance vs rare issue)
- Who exactly is the target user/customer? (ideal user profile)
- Who is the first user we will target? (ideal early adopter segment)
- What is our value proposition / unique solution?
- What's the elevator pitch of the product?
- What is the impact of solving this problem? (quantify or qualify benefit)
- How will we know if users are getting value? (1-2 key indicators)
- What assumptions are we making about users or market?
- Which assumption, if wrong, will cause this to fail? (riskiest assumption)

**Optional Questions:**
- Are there different segments of users?
- What differentiator or innovation do we have? (if entering crowded space)

**Triggers 30% readiness when:**
- Clear user persona
- Specific pain point (not generic)
- Simple solution statement
- Differentiation from competition

### Phase 2: MVP Scope (30-55%)

**Goal:** Define exactly what we're building

**Required Questions:**
- What is the Minimum Viable Product (MVP)?
- Which features are core vs. nice-to-have?
- What can we deliberately exclude from MVP?
- What technical decisions are mandatory before we build?
- Minimum tech stack needed to demo value?

**Platform-Specific MVP Elements (Required):**
- **Mobile:** Core screens and flows, user accounts needed?, permissions, privacy policy
- **Web:** Essential pages, responsive design needs, documentation
- **API:** Minimal endpoints, documentation as part of MVP
- **Data Pipeline:** Minimal data sources & outputs, dashboard/report needs
- **Browser Extension:** Core use-case on limited sites, settings UI
- **CLI:** Basic command functionality, help output and usage examples
- **Desktop:** Core feature on one OS, minimal UI, data storage

**Optional Questions:**
- Does the MVP require a backend or can it be simulated?

**UX and Policy Requirements:**
- App Store guideline items to comply with in MVP
- Payment flow and legal pages if accepting payments
- Privacy policy if collecting user data (GDPR)

**Triggers 55% readiness when:**
- Crystal clear primary action/workflow
- Defined scope boundaries
- Simple onboarding/setup (≤3 steps)

### Phase 3: Technical Feasibility (55-75%)

**Goal:** Understand how to build it

**Required Questions:**
- What is the overall architecture?
- Do we have expertise to build each component?
- Are there any unknowns in this architecture?
- Technology stack specifics (Frontend, Backend, Database, External Services)
- Have we checked platform-specific guidelines and constraints?
- Security and data protection basics (auth/authorization, sensitive data handling)
- Technical unknowns and research plan

**Platform-Specific Technical Checks:**
- **iOS apps:** Confirm functionality doesn't hit forbidden areas, background functionality limits
- **Android:** Minimum SDK, device fragmentation
- **Web:** Browser compatibility, SEO requirements
- **Browser extensions:** Manifest V3 constraints, Chrome-specific API usage
- **CLI/Desktop:** OS-level dependencies, WebView requirements

**Optional Questions:**
- Performance and scale considerations for MVP
- What's our plan for security testing?

**Triggers 75% readiness when:**
- Platform decisions made
- API feasibility confirmed
- Tech stack chosen
- Deployment path clear

### Phase 4: Risk Assessment (75-90%)

**Goal:** Validate assumptions and blockers

**Required Questions:**

**Market Risk & Validation:**
- Have we validated that users want this before building?
- What is our plan to acquire users/customers?

**Technical Feasibility Risk:**
- What could go wrong technically?
- Do we have any single points of failure?

**Platform/Governance Risk:**
- Is there a risk of not getting approval (App Store, Chrome Web Store, etc.)?
- Are there legal or compliance risks?

**User Experience/Adoption Risk:**
- What if users don't behave as expected?
- Are there any red flags in our concept from a user perspective?

**Project Management Risk:**
- Timeline and Resource – is our plan realistic?
- Budget – have we budgeted properly?
- Team – do we have any skill gaps or bandwidth risks?

**Risk Mitigation:**
- For each significant risk, what's our mitigation or contingency plan?

**Platform-Specific Risks:**
- **Mobile:** App Store review, privacy permissions
- **API:** Rate limits, abuse prevention, scaling costs
- **Web:** SEO, performance, browser support
- **Data Pipeline:** Data quality, API rate limits, costs
- **Browser Extension:** Chrome Web Store policies, CSP

**Optional Questions:**
- Worst-case regulatory scenario?

**Triggers 90% readiness when:**
- Top 3 risks identified
- Validation plan for each
- Platform approval precedent found
- Pricing/monetization decided

### Phase 5: Synthesis & Planning (90-100%)

**Goal:** Ensure all previous questions form a coherent strategy

**Required Questions:**
- Given all answers, is the project still a good idea to pursue now?
- What does our timeline and development plan look like?
- What are the immediate next steps before full build?
- What questions remain unanswered?
- Did we address all Red Flags?
- How will we measure success/failure of the MVP?

**Optional Questions:**
- Future roadmap (post-MVP) priorities?

**Perfect Prompt Generation:**
1. **Review synthesis** - Present complete summary customized to project type
2. **User edits** - User corrects anything wrong
3. **Generate perfect prompt** - Synthesize into production build instructions
4. **User approval** - "yes" → Start building

**Triggers 100% readiness when:**
- User approves the perfect prompt

---

## Part 4: Red Flag Checklist

**Stop and address these before proceeding:**

1. **No clear user or target market** - Can't answer who and why specifically
2. **Solution in search of a problem** - "I want to use blockchain/AI" without concrete pain point
3. **"I want to build the next [Facebook/Uber/Amazon]"** - Overly broad/vague goals
4. **Competitive advantage is only "better UX/UI"** - Not defensible, users may not switch
5. **Overly broad scope for MVP** - Several months of work, multiple subsystems
6. **No MVP or plan to test assumptions** - Planning to build full product over a year then release
7. **Lack of market validation or user input** - Haven't talked to a single potential user
8. **Ignoring or unaware of competition/precedent** - Can't name existing solutions
9. **Unrealistic expectations or magical thinking** - "It will go viral on its own"
10. **Underestimating complexity or costs** - "This will be easy" when evidence suggests otherwise
11. **No go/no-go checkpoints or willingness to pivot** - "We're building this no matter what"

---

## Part 5: Platform Requirement Matrix

### Mobile App (iOS/Android)

**Critical Decisions:**
- iOS, Android, or both? Native vs. cross-platform framework
- Backend needed or standalone app?
- Monetization model (free, IAP, subscription)
- Key device features to use (GPS, camera, push notifications)

**Platform Requirements:**
- Follow OS design guidelines (Apple HIG)
- App Store policies: privacy, no private APIs, no forbidden content
- Google Play policies: content rating, privacy policy if collecting personal data
- Technical: 64-bit builds, proper icons and assets

**Approval/Deployment:**
- Apple: $99/yr, app review (days), provisioning profiles, code signing
- Google: $25 one-time, review (hours to a day)
- Both require compliance; Apple notably strict (1.9M rejections in 2024)

**Common Pitfalls:**
- App rejection (crashes, privacy violations, not using IAP)
- Underestimating multi-OS/device support
- Missing push notification setup
- Overloading features (performance issues)
- Not planning for app updates

### Web Application

**Critical Decisions:**
- SPA vs Multi-Page (SSR) - affects tech stack and SEO
- Hosting: server vs serverless vs static
- Tech stack: language/framework, database
- Auth strategy: email/password, OAuth, passwordless

**Platform Requirements:**
- GDPR cookie consent if in EU
- Accessibility (WCAG) if broad audience
- PCI DSS if handling payments (or use Stripe)
- SSL (HTTPS) mandatory

**Approval/Deployment:**
- Set up domain, DNS, hosting, CI/CD pipeline
- No formal approval, but third-party API compliance
- Monitor uptime, security updates

**Common Pitfalls:**
- Security misconfigurations (debug mode, improper database security)
- Neglecting responsive design
- Ignoring SEO until late
- Using too many frameworks (bloat, maintenance headaches)
- Database inefficiencies (not indexing)

### API/Backend Service

**Critical Decisions:**
- REST vs GraphQL vs gRPC
- Authentication: API keys, OAuth 2.0, JWT
- Rate limiting strategy
- Data storage: relational, NoSQL, in-memory
- Versioning approach (/v1/)

**Platform Requirements:**
- Documentation (OpenAPI spec, developer portal)
- Terms of use, possibly SLA
- CORS configuration
- HTTPS, auth on endpoints

**Approval/Deployment:**
- Deploy on cloud (AWS, GCP)
- CI/CD, containerization for scale
- Launch with logging and monitoring

**Common Pitfalls:**
- Lack of good documentation & onboarding
- Uncontrolled usage (no rate limits)
- Breaking changes without notice
- Underestimating support needs
- Bottlenecks (not cached, not load-tested)

### Data Pipeline/ETL

**Critical Decisions:**
- Batch vs Streaming
- Tools: workflow manager vs simple scripts
- Data quality checks location
- Frequency and scheduling
- Error handling strategy

**Platform Requirements:**
- Data protection compliance if personal info
- Monitoring/alerting on failure or anomalies
- Managed services best practices and limits

**Approval/Deployment:**
- Configure scheduled jobs or orchestrator
- Access to sources and targets (credentials, network)
- Gradual rollout (parallel with existing process)

**Common Pitfalls:**
- Schema drift breaking pipeline
- No data validation (garbage in, garbage out)
- Maintenance burden (linear scalability issue)
- Lack of logging/observability (silent failures)
- High cloud costs (inefficient queries)

### Browser Extension

**Critical Decisions:**
- Chrome-only vs cross-browser
- Manifest V2 vs V3 (must use V3 for Chrome)
- Permissions: minimize to reduce review friction
- Extension architecture: background vs content vs popup scripts
- Distribution: Chrome Web Store vs self-host

**Platform Requirements:**
- CWS policies: single purpose, minimal permissions, privacy policy
- Manifest V3 rules: no remote code, service workers, declarativeNetRequest
- Chrome sync storage quotas (~100KB)

**Approval/Deployment:**
- CWS: developer account ($5), submission, review (days)
- Firefox AMO: similar process
- Users install via store link

**Common Pitfalls:**
- Over-broad permissions (scary to users and reviewers)
- Not updating to MV3
- Performance issues (poorly written content scripts)
- Storage limitations (hitting quota)
- UX pitfalls (no onboarding, breaking site functionality)

### CLI Tool

**Critical Decisions:**
- Implementation language (affects distribution)
- Target OSes: plan Windows and UNIX support
- Interface design: subcommands vs flags
- Packaging: package manager vs GitHub releases
- Dependencies: minimize or bundle

**Platform Requirements:**
- No app store, but OS policies (Windows SmartScreen for unsigned .exe)
- Package registry guidelines (PyPI metadata, npm naming)
- Licensing for open-source
- Disclose if collecting analytics

**Approval/Deployment:**
- GitHub release page or package manager
- Homebrew formula submission for Mac
- No auto-update unless you build it

**Common Pitfalls:**
- Cross-platform quirks (newline handling, path separators)
- Poor documentation/help
- Naming collisions
- Backward compatibility (breaking user scripts)
- Not handling errors gracefully

### Desktop Application

**Critical Decisions:**
- Cross-platform framework vs native per OS
- Programming language/framework
- UI/UX: OS conventions or custom uniform design
- Update mechanism: auto-updater or manual
- Data storage & sync: local only or cloud

**Platform Requirements:**
- Windows: code signing certificate to prevent SmartScreen warnings
- macOS: notarization, Developer ID certificate, sandboxing for Mac App Store
- Linux: packaging for distros (AppImage, Flatpak)
- User permissions: OS prompts for file/camera access

**Approval/Deployment:**
- App stores: submission, review (sandboxing requirements)
- Direct distribution: installer/package, uninstall instructions
- Updates: store auto-update or custom implementation

**Common Pitfalls:**
- Lack of code signing & notarization
- Not sandboxing/tested for least privilege
- Platform-specific bugs (case sensitivity, DPI issues)
- High resource usage (especially Electron)
- No crash reporting
- Ignoring OS UX conventions

---

## Part 6: Question Sequencing Principles

### Dependency Logic

1. **Core Value questions MUST come first** - can't define MVP without knowing problem/user
2. **MVP Scope depends on Core Value** - can't pick features without knowing value to deliver
3. **Technical depends on MVP** - once you know what's in MVP, determine technical needs
4. **Risk uses all prior answers** - identify assumptions from earlier phases
5. **Synthesis compiles everything** - final coherent strategy

### Adaptive Branching

- **If user answers multiple things at once** → acknowledge all, skip already-answered questions
- **If user is uncertain** → offer 2-3 options based on research
- **If user says "I don't know"** → research, suggest validation, or provide industry defaults
- **If answer reveals red flag** → address immediately before proceeding

### Project-Type Filtering

- **Mobile app** → skip backend questions if purely client-side
- **API** → skip UI/UX questions
- **Data pipeline** → skip user onboarding questions
- **Simple CRUD** → skip complex scalability questions

### Validation Checkpoints

**Pre-build validation:**
- Market validation (interviews, landing pages)
- Technical feasibility (API availability, rate limits)
- Platform approval precedent
- Cost/pricing validation

**Post-build validation:**
- User testing
- Performance benchmarks
- Security audit

---

## Usage Guidelines for Question Generation

1. **Start with project type identification** - determines which questions apply
2. **Follow phase sequence** - Core → MVP → Technical → Risk → Synthesis
3. **Ask one question at a time** - wait for answer before proceeding
4. **Use answers to filter future questions** - skip irrelevant paths
5. **Flag red flags immediately** - don't proceed if critical issue found
6. **Validate assumptions before building** - especially riskiest ones
7. **Ensure all platform requirements covered** - check matrix for project type
8. **Generate perfect prompt only at 100%** - when all critical questions answered

---

**Last Updated:** 2026-01-18
**Version:** 1.0
