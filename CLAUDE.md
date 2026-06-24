# CLAUDE.md — Hose Developer Guide

## Project overview

Hose is a **reactive entity streaming framework** over pluggable stores. It gives callers
`StateFlow`-backed handles for individual entities and `Flow<SetEvent<K>>`-backed live sets
for queries, backed by any store that implements the SPI. The domain model is untouched
(no marker interfaces, no annotations); capabilities live in a type-class (`EntityType`).

---

## Module layout

```
hose/                         Maven multi-module, parent pom at root
├── hose-store-spi/           Plain Java SPI — no Kotlin-only constructs in public API
├── hose-core/                Runtime: spine, identity map, handles, live sets, write path
├── hose-contract-tests/      Reusable test base classes (in src/main, not src/test)
├── hose-store-memory/        In-memory adapter (both capability tiers)
├── hose-store-postgres/      Postgres adapter (JDBC + triggers + LISTEN/NOTIFY)
├── hose-store-surreal/       SurrealDB adapter (WebSocket RPC + LIVE SELECT)
└── hose-vaadin-demo/         Vaadin 24 proof-of-concept app (karibu-dsl, vaadin-boot)
```

Package root: `org.antoined.*`

---

## Technology stack

| Concern | Choice |
|---|---|
| Language | Kotlin 2.2.0 (JVM 21); SPI layer in plain Java |
| Build | Maven 3.9.x |
| Async | kotlinx-coroutines-core 1.10.2 |
| Testing | JUnit 5.12.2 + Turbine 1.2.0 (flow assertions) |
| Nullability | JSpecify 1.0 |
| Postgres | JDBC + HikariCP 6.3.0 |
| SurrealDB | WebSocket via `java.net.http.WebSocket` (not the JVM SDK — it lacks LIVE SELECT) |
| JSON | Jackson 2.19.0 (core + kotlin module + JSR-310) |
| UI (demo) | Vaadin 24.7.4 + karibu-dsl-v23 2.3.2 + vaadin-boot 13.3 |
| Containers in tests | `docker run` / `docker rm -f` via Bash (not Testcontainers — Docker Desktop masks the API socket on this machine) |

---

## Build commands

```bash
mvn clean verify          # full build + all tests
mvn clean compile         # compile only
mvn test -f hose-core/pom.xml   # single module tests
```

All tests must pass with `mvn -q verify` before a step is considered done. Commits are
per-step with the message format `step NN: <title>`.

---

## Core architecture invariants

### 1. Type-class pattern — no marker interface

Entities are arbitrary classes (data classes, records, generated DTOs). Hose capabilities
live beside them in an `EntityType<E, K, V>` instance:

```kotlin
val TODO = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}
```

- `name` (logical type name) is a **persisted artifact**. Renaming it orphans stored data.
- `encodeKey(K): String` must be **stable and injective**.
- Version codec (encode + decode) is **all-or-nothing** — builder fails at build time if incomplete.

### 2. One-cast invariant (enforced by test)

The wildcard-to-typed narrowing of `EntityType<*, *, *>` happens in **exactly one place**:
`IdentityMap.kt`. No unchecked casts may appear anywhere else in `hose-core`. The file
`OneCastInvariantTest` enforces this with a source scan. Adapter modules must contain zero
unchecked casts.

### 3. Total order via the spine

A single bounded channel (default capacity 1000) drained by one coroutine gives every
mutation a global total order:

1. **Apply phase** — resolve handle via identity map, apply mutation under version guard
2. **Route phase** — run registered routers (live-set maintenance) in loop order
3. **Tap emission** — shared flow mirror of applied mutations

Rejected-stale and absorbed-echo mutations are invisible to taps.

### 4. Route-then-persist write path

1. LOCAL write enqueued on spine, applies instantly (optimistic)
2. Caller resumes immediately — collectors see the write before the store does
3. Off-loop persist on `Dispatchers.IO` through an ordered queue (spine order preserved)
4. On store failure → enqueue `Mutation.Revert` carrying pre-write value, emit `WriteFailure`
5. Stale LOCAL writes are dropped (not persisted — would clobber newer store state)
6. Absorbed-echo LOCAL writes **are** persisted

### 5. Version guard (comparator-based)

On upsert:
- Strictly older → dropped and logged
- Strictly newer → applied
- Tie → equality check: equal = absorb (echo), unequal = apply (arrival order arbitrates)

`Versions.none()` (constant comparator) degrades to equality-only echo absorption with no
special-case code.

### 6. SPI boundary law

Everything that crosses the SPI crosses as neutral envelopes:

| Crossing | Type |
|---|---|
| Entity data | `StoredEntity` (type name, key string, version string, payload bytes) |
| Queries | `StoreQuery` (conjunctive field comparisons) |
| Feed events | `FeedListener.onUpsert / onDelete / onResync` |

The core runtime works generically with `EntityType<*, *, *>` tokens. Adapters traffic
exclusively in strings and opaque payloads.

### 7. Feed bridge

Adapter-side change feeds are listener-based (plain Java callbacks):

1. Adapter threads enqueue into an unbounded ordered channel (non-blocking)
2. One forwarder coroutine drains into the spine (preserves feed order)
3. Echo absorption and stale rejection are handled by the version guard
4. Feed deletes arrive by encoded key; resolve back to domain pk via identity map's key index
5. On reconnect: adapter calls `onResync()` → core snapshot-refreshes all live handles/sets

---

## Persistence layout

### PostgreSQL

One table per entity type:

```sql
CREATE TABLE hose_<logical_name> (
    key          TEXT PRIMARY KEY,
    version      TEXT NOT NULL,
    payload      JSONB,
    payload_class TEXT
);
```

Physical name: `hose_` + lowercase logical name with `[^a-z0-9] → _`.

Change feed: trigger on each table → `pg_notify` on channel `hose_changes` with JSON
`{table, op, key, version, payload?}`. Payloads > ~7.5 KB are omitted; re-selected on demand.
Feed listener polls via `PGConnection.getNotifications(250ms)`.

### SurrealDB

One table per entity type, columns: `key`, `version`, `payload`, `payloadClass`.
Physical name: lowercase logical name with `[^a-z0-9] → _`.

Feed: `LIVE SELECT * FROM type::table(...)` over WebSocket RPC. Query filtering is done
in-process with the reference evaluator (matches `StoreQueries` by construction).
Single-flight CAS guard on reconnect; outstanding LIVE subscriptions are `KILL`ed before
re-issuing.

---

## Contract test kit

Base classes live in `hose-contract-tests/src/main/kotlin` (not `src/test`).

| Class | What it covers |
|---|---|
| `HoseFlowContract` | Version guards, total order, snapshot-then-deltas, reverts, echo absorption |
| `EntityStoreContract` | CRUD + query semantics |
| `ObservableStoreContract` | Feed ordering + per-key ordering guarantees |

Adapter test: subclass + implement `fixture()`:

```kotlin
class PostgresContractTest : HoseFlowContract() {
    override fun fixture() = PostgresFixture()
    override fun hoseConfig() = HoseConfig(graceMillis = 600)
}
```

Container fixtures spin up via `docker run` (random host port, shutdown-hook cleanup).
Never use Testcontainers — the Docker Desktop API socket is masked on this machine.

### Turbine pattern

```kotlin
turbineScope {
    val c1 = hose.entity(TODO, 1L).testIn(this)
    assertEquals(null, c1.awaitItem())
    hose.upsert(TODO, todo)
    assertEquals(todo, c1.awaitItem())
}
```

---

## Public API reference

```kotlin
// Type registration
val TODO = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}

// Instantiation
val hose = Hose(store, setOf(TODO))

// Entity handle (StateFlow<E?>)
val flow: StateFlow<Todo?> = hose.entity(TODO, 42L)

// Live set (Flow<SetEvent<K>>)
val active: Flow<SetEvent<Long>> = hose.liveSet(TODO, StoreQuery.all("todo"))

// Writes (suspending, optimistic)
hose.upsert(TODO, updatedTodo)
hose.delete(TODO, 42L, version)

// Mutation tap — spine order, every applied mutation
hose.subscribe(setOf(TODO)).collect { mutation -> ... }

// Persist failures
hose.writeFailures.collect { failure -> ... }
```

`Versions` built-ins: `long()`, `instant()`, `comparable()`, `lexicographic()`, `none()`.

---

## Vaadin demo conventions

The demo uses **karibu-dsl** exclusively — never Java-style imperative component building.
Trigger the `karibu` skill for any task touching a `@Route`, Vaadin component, `KComposite`,
or `Binder`. Key demo files:

| File | Role |
|---|---|
| `Main.kt` | Boot entry (vaadin-boot, embedded Jetty, Spring-free) |
| `Backend.kt` | In-memory store + entity type registration |
| `TodoView.kt` | Route("") — karibu-dsl grid + form |
| `TodoViewModel.kt` | StateFlow/Flow view model, mutation calls |
| `FlowBinding.kt` | `UiFlowScope` — 50-line Flow→Vaadin bridge (attach/detach scoped, `UI.access`) |

`UiFlowScope` is purpose-built (decision #012 — no `vaadin-stateflow` library exists).

---

## Scope guards — never implement these

The following are explicitly out of scope for any step:

- Incremental view maintenance (joins, aggregates in live queries)
- Cross-entity transactions
- Wire protocol or browser client
- Distributed identity maps
- CRDT machinery
- Caching layers in adapters
- Performance optimizations not demanded by a failing test

If a task description sounds like one of these, stop and confirm with the user.

---

## Design decisions log

All architectural choices are recorded in `DECISIONS.md` (18 entries as of M1). Consult it
before adding anything that touches the spine, version guard, write path, feed bridge,
or entity type system. Key decisions:

| # | Summary |
|---|---|
| #006 | `Mutation.Delete.version` is nullable (feeds often can't supply one) |
| #007 | Route-then-persist; persist queue preserves spine order |
| #008 | Stale LOCAL writes dropped; absorbed-echo LOCAL writes persisted |
| #009 | One-cast invariant — narrowing only in `IdentityMap.kt` |
| #012 | Demo carries its own `UiFlowScope`; no external flow-vaadin library |
| #013 | SurrealDB adapter speaks WebSocket RPC directly (JVM SDK lacks LIVE SELECT) |
| #014 | SurrealDB in-process query filtering (matches reference evaluator by construction) |
| #015 | Containers via docker CLI (Testcontainers masked on this machine) |
| #016 | LIVE SELECT downgrade + single-flight CAS reconnect recovery |
| #017 | Postgres payload-in-NOTIFY with bounded size fallback |
| #018 | One table per entity type; relations via `follow`; edge table is future work |

---

## Entity equality is load-bearing

Echo suppression compares a locally written entity against its change-feed echo using `==`.
Data classes and Java records give structural equality for free. Hand-written classes must
implement `equals` consistently — without it, every local write emits a false tap.

---

## Predicate purity contract

Live-set predicates are **pure and cheap**, run synchronously on the spine thread. A slow or
side-effecting predicate stalls or corrupts the entire runtime. KDoc on `LiveSets.kt`
enforces this as a documented contract.
