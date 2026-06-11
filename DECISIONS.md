# Decisions

Running log of design choices made during implementation where the plan/design notes left a question open. Newest at the bottom.

## 000 — Missing design notes
`plan.md` names `hose-design-notes.md` as the semantic authority, but the file is not in the repository. The plan's inline semantics (§-references included) are detailed enough to implement against; this log records every open-question resolution the notes would otherwise have arbitrated.

## 001 — Build stack versions
JDK toolchain 25 targeting **JVM 21** (plan requirement), Kotlin **2.2.0** via `kotlin-maven-plugin`, kotlinx-coroutines **1.10.2**, JUnit **5.12.2**, Turbine **1.2.0**. Mixed Kotlin/Java compilation ordered Kotlin-first in the parent `pluginManagement` so the SPI stays plain Java while core modules are Kotlin.

## 002 — SPI nullability annotations
`@Nullable` in SPI signatures comes from **JSpecify 1.0** (`org.jspecify.annotations.Nullable`) — the modern, tooling-recognized standard; Kotlin 2.x reads it for null-safety across the boundary.

## 003 — SPI threading + feed shape (plan-mandated, recorded per Step 04)
SPI methods are **blocking**; the core wraps calls in `Dispatchers.IO`. The change feed is **listener-based** (`FeedListener`); the core bridges it to a channel. `FeedListener.onResync()` (default no-op) is included from day one as the lossy-feed healing hook Steps 14/15 need.

## 004 — `Link` is a typed reference
`Link(type, key)` — logical type name + encoded key; `follow` is a batched dereference. The richer "edge with relation name" shape can be layered later without breaking this (an edge resolves *to* links). Simplest thing that supports record links and relations at M2.

## 005 — `StoreQuery` semantics live in one place
`StoreQueries.matches(query, payload)` in `hose-store-spi` is the reference evaluator (reflective field access; numbers compare by value, `5L == 5`). The in-memory adapter calls it; native-language adapters must match it; the core reuses it for live-set maintenance. Avoids three divergent definitions of what a query means.

## 006 — SPI `delete` version is `@Nullable`
`Mutation.Delete.version` is nullable in domain form (feeds can't always supply one); forcing non-null at the SPI would push a lie across the boundary. Adapters may use it for feed events or conflict checks and are free to ignore it.

## 007 — Persistence ordering: route-then-persist, persists serialized in spine order (DECIDED)
Per the plan's Step 09 gate (the §11 leaning, now decided): LOCAL writes route optimistically on the spine first; persistence happens off-loop on `Dispatchers.IO`. Persists flow through **one ordered queue** drained by a single coroutine, so writes reach the store in spine order — no per-key reorder hazard, at the (M1-acceptable) cost of globally serialized store writes. On persist failure: `Revert` (carrying the pre-write value captured at apply time) re-enters the spine, and a `WriteFailure` surfaces on the public flow. *(The design-notes §11 flip cannot be performed — the notes file is not in the repo; this entry is the record.)*

## 008 — Stale LOCAL writes are dropped, not persisted
A LOCAL write the version guard rejects (older than current state) is not sent to the store — persisting it could clobber newer store state. It surfaces as `WriteFailure(StaleLocalWriteException)`. An absorbed-echo LOCAL write (identical value) **is** persisted for durability, with nothing to revert.

## 009 — One-cast invariant is file-scoped
All wildcard↔typed narrowing (`EntityHandle` narrowing and `EntityType` token erasure) lives in `IdentityMap.kt`; collaborators (live sets, write path) obtain the erased token via `IdentityMap.erasedToken()`. The Step 11 scan enforces "no unchecked casts outside IdentityMap.kt".

## 010 — Live-set snapshot merge: deltas win
A live set registers its spine router first, then runs the one-shot store query; any key the router touched while the snapshot was loading is skipped during the merge (the spine is fresher than the store read). Snapshot entities seed shared handles through the identity map.

## 011 — Contract kit specifics
Revert semantics are tested adapter-agnostically via a kit-internal `FailingStore` decorator that injects one persist failure — no adapter needs a failure mode of its own. The one-cast scan splits in two: hose-core's own suite asserts `UNCHECKED_CAST` appears only in `IdentityMap.kt`; the kit's scan asserts adapter modules contain none at all. Step 10's core-level integration test was deleted as a duplicate once the kit's `HoseFlowContract` covered it (it also created a reactor cycle core→memory→kit→core). Step 06's conflation and `Versions.none()` behaviors stay unit-level in hose-core: they are kotlinx/type-class semantics no adapter can influence.
