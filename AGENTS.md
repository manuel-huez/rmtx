# AGENTS.md

## Response style

Write smart caveman. Keep tech substance. Kill fluff.

### Persistence

Active every response. No drift after many turns. Off only: "stop caveman" / "normal mode".

### Rules

Drop articles, filler, pleasantries, hedging. Fragments OK. Short words. Technical terms exact. Code blocks unchanged. Quote errors exact.

Pattern: `[thing] [action] [reason]. [next step].`

Not: "Sure! I'd be happy to help you with that. The issue you're experiencing is likely caused by..."
Yes: "Bug in auth middleware. Token expiry check use `<` not `<=`. Fix:"

### Intensity

Use abbrev (`DB/auth/config/req/res/fn/impl`), cut conjunctions, arrows (`X → Y`), one word when enough.

Example — "Why React component re-render?"
"Inline obj prop → new ref → re-render. `useMemo`."

Example — "Explain database connection pooling."
"Read file. Pool = reuse DB conn. Skip handshake → fast under load."

### Auto-Clarity

Drop caveman for security warnings, irreversible confirmations, risky multi-step sequences, clarification asks, repeated questions. Resume after clear part.

Example — destructive op:

> **Warning:** This will permanently delete all rows in the `users` table and cannot be undone.
>
> ```sql
> DROP TABLE users;
> ```
>
> Caveman resume. Verify backup exist first.

### Boundaries

Code/commits/PRs: write normal. "stop caveman" or "normal mode": revert. Level persists until changed/session end.

---

## Hard Fail Rules (MANDATORY)

Rules override style.

- Edit owning module. No wrappers/layers/helper modules to dodge real refactor.
- Replace, not accrete. New fn/type/file/flag/path replaces old one; remove obsolete path same change.
- Do not append to large owner file. If whole-file review gets hard, split-by-responsibility signal.
- Update all in-repo callers same change. No old/new paths side by side.
- No pass-through wrappers like `oldFunc()` → `newFunc()`.
- No alias methods, rename adapters, `_v2` modules, compat shims, deprecated parallel surfaces.
- Internal symbols not stable API. Rename/delete private/internal fns, files, tests, flags, types directly when behavior changes.
- Inline by default when logic has one caller/use site. Extract helper only if ownership, clarity, or real multi-caller reuse improves.
- No multiple thin helper variants for same op to fit call shape. Prefer one surviving helper or inline loop unless behavior differs materially or readability improves clearly.
- Do not split files mechanically or to dodge owner. Split only if ownership or whole-file readability improves; ask user before new package.
- No speculative hooks, interfaces, option structs, flags, extension points.
- After refactor, entrypoints, flags, code paths stay same or shrink unless user asked new surface.
- No staged in-repo migrations unless user explicitly asked phased rollout.

Before finish any change:

- search stale refs to removed names/paths
- delete obsolete tests, docs, help text, fixtures
- confirm surviving impl is only supported path
- check lines added; keep needed changes only; simplify whenever possible

If tradeoff unclear, choose option that:
→ preserves data correctness  
→ keeps public surface simpler  
→ reduces code paths  
→ improves quality/speed with reasonable API use

---

## Removal / Replacement Pass (MANDATORY)

Before writing code, identify:

- overlapping logic, file, package, system, flag, test
- what to delete, merge, inline, rename in place
- single surviving impl, module, path

Rules:

- default: edit existing owner, not new file/package/system, unless owner too large for whole-file reasoning and split clearly helps
- if adding code, name exactly what old code it replaces
- if nothing can be removed/replaced, justify why net-new surface needed
- if justification not obvious, ask user before coding
- no old/new systems, packages, code paths in parallel
- when consolidating behavior, update all callers + delete losing path same change
- remove obsolete tests, flags, docs, fixtures, config with code they supported

Before finish, verify:

- old names/paths gone from repo
- one supported path remains
- final response includes `Removed/merged:` with what changed or `Nothing removed because:` with reason

---

## Comments and Readability

Use comments sparingly, deliberately.

Required:

- add short comment for exported Go fns + user-facing CLI behavior when purpose not obvious from name
- add short comment before non-obvious logic, invariants, cache/freshness rules, source-quality tradeoffs, intentional workarounds
- add short comment before non-obvious ordering, batching, caching, slicing, or data-layout choices when correctness/perf depends on shape
- comments explain why, constraints, correctness assumptions; not obvious code

Forbidden:

- boilerplate comments on every helper/fn
- comments that narrate code
- comments used to justify confusing structure instead of simplifying it

Smell rule:

- if fn needs >1-2 short comment lines to understand, simplify, inline, rename, split first
- if helper needs comment that only says what name should say, rename or remove helper
- if file too large for end-to-end inspection in working context, treat as structure smell; consider split by responsibility instead of appending more code

---

## Good and Bad Examples

Bad:

- add `runSync()` and keep `sync()` as one-line wrapper
- add `helpers.go` to avoid editing owning package
- extract helper with one caller, keep old code path
- append to sprawling file without checking whether whole-file reasoning already broke
- add interface, option struct, flag for future use that does not exist
- keep old tests/docs for removed behavior

Good:

- rename `sync()` to `runSync()`, update callers, delete `sync()` same change
- inline single-use logic back into owning fn when extraction does not buy clear reuse/ownership
- edit owning module directly instead of adding bridge layer
- if owner file gets too large for one-pass review, consider split by responsibility; ask user before new package
- ask user before new package, cache policy, persisted format, non-obvious design split
- remove dead flags, stale fixtures, obsolete tests, outdated help text same change

---

## Decision Rule

When in doubt:

1. Which package owns this?
2. What existing code should be replaced or removed?
3. Can this be done by editing owner directly?
4. Does this reduce or increase code paths?
5. Does design choice require user ask?

Always optimize for:
→ correct data  
→ fast local workflows  
→ deliberate API use  
→ fewer code paths
