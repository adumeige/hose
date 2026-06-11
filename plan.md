# Hose — Implementation Plan for Claude Code

*Companion to `hose-design-notes.md`. Read that document first; it is the authority on semantics. This plan is the authority on order of work. Where they conflict, the design notes win and this plan should be amended.*

## How to work this plan

- Execute steps **in order**. Each step ends with a "Done when" gate — do not start the next step until the gate is green (`mvn -q verify` passes, listed tests pass).
- **Tests before implementation** within each step where tests are listed: write the failing test, then make it pass.
- Commit at every gate, message format: `step NN: <step title>`.
- **Scope guards — never implement, even if it seems helpful:** incremental view maintenance (joins/aggregates in live queries), cross-entity transactions, a wire protocol / browser client, distributed identity maps, CRDT machinery, caching layers, performance optimizations not demanded by a failing test.
- When a design question arises that the design notes leave open (§11 of the notes), pick the *leaning* recorded there; if no leaning exists, choose the simplest option, record the choice in `DECISIONS.md` at repo root, and continue.

---

## Phase 0 — Repository bootstrap

### Step 01 — Maven multi-module skeleton
Create a Maven multi-module project (Maven chosen over Gradle: plays better with corporate proxies — `settings.xml` mirrors are the known, boring path). JVM 21, Kotlin 2.x via `kotlin-maven-plugin`:

```
hose/
  pom.xml                     # parent: pluginManagement, dependencyManagement, kotlin + surefire config
  hose-core/                  # handles, spine, identity map, live sets, public API
  hose-store-spi/             # the adapter SPI — plain-Java-friendly, zero Kotlin-isms in signatures
  hose-store-memory/          # in-memory adapter (both capability tiers)
  hose-contract-tests/        # the reusable adapter test kit (base classes in main sources — see Step 11)
  hose-vaadin-demo/           # M1 proof application
  DECISIONS.md
```

Dependencies (managed in the parent): `kotlinx-coroutines-core`; test stack: JUnit 5, `kotlinx-coroutines-test`, Turbine (flow assertions). `hose-store-spi` must compile with **no Kotlin-only constructs in its public signatures** (no suspend, no Flow) — see Step 04.

**Done when:** `mvn -q verify` is green with empty modules; `DECISIONS.md` exists.

---

## Phase 1 — Core types and SPI

### Step 02 — EntityType registry (`hose-core`)
**No marker interface.** Entities are arbitrary classes (data classes, Java records, generated DTOs); hose's required capabilities — key, version, equality — live *beside* the type in a type-class, registered at `Hose` construction:

```kotlin
interface EntityType<E : Any, K : Any, V : Any> {
    val name: String                    // default: E::class.qualifiedName (reified factory)
    fun pk(e: E): K
    fun version(e: E): V
    val versionOrder: Comparator<V>
    fun encodeKey(k: K): String         // default: toString(); must be STABLE + INJECTIVE
    fun encodeVersion(v: V): String     // NO default — paired with decode, see below
    fun decodeVersion(s: String): V     // NO default — toString is not invertible
}
```

- **Version codec has no defaults at all** — half an automatism is worse than none. Encode and decode are supplied *together*, either by a `Versions` strategy or explicitly; the builder rejects an `EntityType` with one half missing, at build time.
- Builder DSL: `entityType<Todo, Long, Instant>("todo") { pk { it.id }; version({ it.updatedAt }, Versions.instant()) }` — `name` and `encodeKey` default as above; `Versions` strategy supplies comparator + full codec together.
- `Versions` shelf: `long()`, `instant()`, `comparable<T>()` (comparator only — codec still required), `lexicographic()` (KDoc warning: `"9" > "10"`), `none()` — the constant comparator; degrades the guard to equality-only echo absorption with zero special-case code.
- Registry: `EntityKey(typeName: String, pk: Any)` internally; **fail-fast on duplicate names** at registration (near-impossible with FQN defaults, but cheap and catches deliberate overrides colliding). KDoc caveats: `name` and toString-based key encodings are *persisted artifacts* — with FQN defaults, **package moves now orphan data too**, not just class renames; override `name` before refactoring any long-lived entity; composite keys should declare encoding explicitly. Adapters map the logical name (dotted FQN) to physical identifiers (table names etc.) — that mapping is adapter-side and must itself be stable.
- `Predicate<E>` = pure function `(E) -> Boolean`. KDoc must state the §4.4 contract: **pure and cheap — runs on the spine**.

**Done when:** registry compiles; a plain `data class Todo(...)` (no hose imports in the class) registers and round-trips key/version extraction; duplicate-name registration throws; equality round-trip test passes (equality is load-bearing — echo suppression depends on it, §4.3); an `EntityType` missing either half of the version codec fails at build time, not first use.

### Step 03 — Mutation envelope (`hose-core`)
Sealed hierarchy — mutations travel the spine in *domain* form (token + decoded values); encoding happens only at SPI/feed boundaries:

```kotlin
sealed interface Mutation {
    data class Upsert(val type: EntityType<*, *, *>, val entity: Any, val origin: Origin) : Mutation
    data class Delete(val type: EntityType<*, *, *>, val pk: Any, val version: Any?, val origin: Origin) : Mutation
    data class Revert(val type: EntityType<*, *, *>, val pk: Any, val toEntity: Any?, val cause: Throwable) : Mutation
}
enum class Origin { LOCAL, FEED }
```

`Revert` exists for the route-then-persist failure path (§4.4 lean). The wildcard-to-typed narrowing happens once, inside the identity map (see one-cast invariant, Step 06/11).

**Done when:** compiles; exhaustive `when` over `Mutation` in a placeholder router compiles without `else`.

### Step 04 — Store SPI (`hose-store-spi`)
Per design notes §5/§6: the SPI is **expressible from plain Java** — and with the type-class in place, **domain classes never cross the SPI**. Adapters traffic exclusively in a neutral envelope; the boundary law: *generic above the token, neutral below the SPI.* Decision (record it): SPI methods are **blocking**; the core wraps calls in `Dispatchers.IO`. The feed is **listener-based**; the core bridges it to a channel.

```java
// Neutral envelope — no generics, Java-trivial
record StoredEntity(String type, String key, String version, Object payload) {}
// payload is opaque to the SPI: the domain instance itself for M1/in-memory;
// serialized form once the codec lands in EntityType at M2 (toStored/fromStored).

// Required tier
interface EntityStore {
    @Nullable StoredEntity get(String type, String key);
    Set<StoredEntity> query(StoreQuery query);            // one-shot; liveness is the core's job
    Map<Link, StoredEntity> follow(Set<Link> links);
    StoredEntity upsert(StoredEntity entity);
    void delete(String type, String key, String version);
}
// Optional tier
interface ObservableStore extends EntityStore {
    FeedSubscription changeFeed(Set<String> types, FeedListener listener);
}
```

`StoreQuery` is a data structure (field comparisons only — per-entity-evaluable, §4.2); the Kotlin DSL over it is a later, separate concern (§11). Keys and versions cross this boundary **only** in encoded-string form (`EntityType.encodeKey`/`encodeVersion`); decoding back to `K`/`V` happens core-side before the comparator or identity map ever sees them. Capability detection: `store is ObservableStore`.

**Done when:** SPI module compiles **as Java-consumable** (add one trivial Java test class in the module that implements `EntityStore` to prove it) — note this is now easy precisely because the envelope killed the generics.

### Step 05 — In-memory adapter (`hose-store-memory`)
`ConcurrentHashMap`-backed implementation of **both tiers**, with a constructor flag `observable: Boolean` — when false, it presents as `EntityStore` only. This single adapter exercises both capability tiers and the topology-axis behavior (notes §6.3) in tests.

**Done when:** basic CRUD unit tests pass in both modes; feed listener receives events for writes when observable.

---

## Phase 2 — The reactive core

### Step 06 — EntityHandle + identity map (`hose-core`)
Per notes §4.3:
- `EntityHandle<E>` wraps `MutableStateFlow<E>`; exposes read-only `StateFlow<E>`; `apply(mutation)` with the **comparator-based version guard**: incoming strictly older (`versionOrder`) → drop + log; strictly newer → apply; **tie → equality check; equal absorbs as echo, unequal applies** (arrival order arbitrates, well-defined under the spine's total order).
- `IdentityMap`: `ConcurrentHashMap<EntityKey, EntityHandle<*>>`; handle creation is `computeIfAbsent`. **The one-cast invariant:** narrowing `EntityHandle<*>` to `EntityHandle<E>` happens in exactly one place, `@Suppress`-annotated with a comment citing the registry's `name ↔ EntityType` bijection. No cast escapes the identity map.
- Eviction: watch `subscriptionCount`; on zero, start grace timer (configurable, default 30s); on expiry with still-zero, evict and release any upstream resources. Resubscription after eviction triggers fresh adapter `get`.

Tests (Turbine):
1. Two collectors on one key receive the same instance's emissions (one handle, X collectors).
2. Echo absorption: applying an equal entity emits nothing.
3. Stale rejection: applying a comparator-older version emits nothing.
4. **Tie semantics:** same version, unequal value → applies; same version, equal value → absorbed. Use `Versions.instant()` with two writes in the same millisecond.
5. Conflation: slow collector observes only the latest of a burst.
6. Eviction: zero subscribers + grace expiry evicts; re-get after eviction hits the store again (verify with a counting store stub); re-collect *before* expiry replays current value and cancels the timer.
7. `Versions.none()`: guard degrades to equality-only echo absorption; no stale rejection occurs.

**Done when:** all five pass. These are contract-kit candidates — write them in a reusable style.

### Step 07 — The spine (`hose-core`)
Per notes §4.4: bounded `Channel<Mutation>` (default capacity 4096, configurable) drained by **one** coroutine. Verbs: *apply* (resolve handle, version-guarded apply), *route* (Step 08), *evict* (housekeeping ticks). Reads never touch the spine. Expose an internal tap: `fun mutations(): Flow<Mutation>` — a `SharedFlow` mirror of applied mutations (this *is* `subscribe(type)`, notes §4.4).

Tests:
1. Total order: enqueue interleaved mutations for keys A and B from multiple coroutines; assert all observers see one consistent global sequence (use the tap).
2. LOCAL and FEED mutations for the same key merge sequentially; version guard decides deterministically.
3. Bounded channel: filling beyond capacity suspends the producer rather than OOMing (capacity-1 channel in test).
4. A throwing predicate/handler does not kill the loop (supervision: log, continue).

**Done when:** all pass; loop provably single-threaded (assert thread/coroutine identity inside the loop in a test).

### Step 08 — Live sets (`hose-core`)
Per notes §4.2/§4.3: a live set = `StoreQuery` (predicate-shaped) + materialized membership.
- Registration: initial one-shot `store.query()` snapshot, then maintenance on the spine: for each applied mutation of a matching type, evaluate the predicate — present & matching: no-op; present & no longer matching: remove; newly matching: add. (The original pipes-notes routing rule, finally implemented where it belongs.)
- Emission shape (§11 leaning): **delta flow with replayed snapshot** — `Flow<SetEvent>` where `SetEvent = Snapshot(keys) | Added(key) | Removed(key)`; late subscriber gets `Snapshot` first. Members resolve to shared handles via the identity map (no duplication, §4.3).
- Live sets refcount and evict like handles (same grace mechanism).

Tests: snapshot-then-deltas ordering for a late subscriber; add/remove/no-op transitions on mutation; entity mutation that doesn't change membership emits on the entity handle but not the set; set + handle fed by a single spine assignment (order asserted via tap).

**Done when:** all pass.

### Step 09 — Write path: route-then-persist with revert (`hose-core`)
Per §4.4 lean and §8: `upsert`/`delete` enqueue LOCAL mutation (instant optimistic routing), then persist via adapter **off-loop** (`Dispatchers.IO`); on adapter failure, enqueue `Revert` carrying the previous entity (capture it during apply). Surface failures: a `SharedFlow<WriteFailure>` on the public API.

Tests: optimistic emission precedes store write completion (gate the stub store with a latch); failing store yields a `Revert` that restores prior value in order; `WriteFailure` is observable.

**Done when:** all pass. Record persistence-ordering as **decided** in `DECISIONS.md` and flip §11 in the design notes.

### Step 10 — Feed bridge + public API facade (`hose-core`)
- Feed bridge: if the store `is ObservableStore`, subscribe `changeFeed` for types in use; listener enqueues FEED mutations onto the spine. Echo absorption (equality) and stale rejection (version) come free from Steps 06–07 — add an integration test proving a LOCAL write followed by its FEED echo emits exactly once.
- Public facade, Kotlin-first (notes §5), **token-anchored throughout** — the `EntityType` token is the generic anchor; no casts, no string type names in application code:

```kotlin
class Hose(store: EntityStore, types: Set<EntityType<*, *, *>>, config: HoseConfig = HoseConfig()) {
    fun <E : Any, K : Any> entity(type: EntityType<E, K, *>, pk: K): StateFlow<E?>
    fun <E : Any> liveSet(type: EntityType<E, *, *>, query: StoreQuery): Flow<SetEvent<E>>
    fun subscribe(types: Set<EntityType<*, *, *>>): Flow<Mutation>     // the spine tap
    suspend fun <E : Any> upsert(type: EntityType<E, *, *>, entity: E)
    suspend fun <E : Any, K : Any, V : Any> delete(type: EntityType<E, K, V>, pk: K, version: V)
    val writeFailures: SharedFlow<WriteFailure>
    fun close()
}
```

**Done when:** an end-to-end core test (memory store, observable mode) drives the full loop: upsert → entity flow + live set + tap all update in total order; same test in non-observable mode still passes for own-writes (topology tier 1, notes §6.3).

---

## Phase 3 — The contract kit (the real M1 deliverable)

### Step 11 — Extract the adapter contract suite (`hose-contract-tests`)
Abstract JUnit 5 base classes parameterized by an adapter factory. Maven packaging: the base classes live in the module's **main sources** with JUnit as a compile-scope dependency (Maven has no `java-test-fixtures` equivalent worth fighting for); adapter modules consume `hose-contract-tests` as a test-scoped dependency. Coverage:
- SPI semantics: CRUD, query correctness, follow, version handling on delete.
- Type-class invariants: **one-cast invariant** (no unchecked casts outside the identity map — enforce via a source-scan test or ArchUnit rule); **key-encoding stability** (same key encodes identically across calls) and **injectivity** (a property test the adapter/application feeds sample keys to — distinct keys must never encode identically, or two entities silently merge into one handle); **tie semantics** of the version guard per Step 06.
- Feed semantics (only when `ObservableStore`): events for every write, ordering per key, listener resubscribe.
- Flow semantics through a `Hose` instance over the adapter: the seven behaviors from Step 06, total order from Step 07, snapshot-then-deltas from Step 08, revert from Step 09, echo-once from Step 10.
- Topology tiers: own-writes liveness without feed; external-writes liveness with feed (simulate an external writer by writing through a second store handle/connection).

Re-point the memory adapter's tests at the kit; delete duplicated tests.

**Done when:** memory adapter passes the full kit in both modes; the kit is consumable from another module as a test-scoped dependency.

### Step 12 — Vaadin demo (`hose-vaadin-demo`)
Spring-free Vaadin + Karibu + vaadin-stateflow: a single-view todo/CRUD app over the memory store (observable mode). Two browser sessions must show live propagation: edit in one, observe in the other. Bind a live set to a Grid via the delta flow; bind one entity detail to a `StateFlow`.

**Done when:** `mvn -pl hose-vaadin-demo -am verify` is green and the module's run target (embedded Jetty main class via `exec:java`, or the Vaadin plugin's dev mode) serves the app; a manual two-tab check shows live updates; a Karibu-Testing UI test asserts a programmatic upsert appears in the Grid.

**🏁 M1 complete.** Tag `v0.1.0-m1`.

---

## Phase 4 — M2: SurrealDB adapter (`hose-store-surreal`)

### Step 13 — Driver + triad mapping
Add module; use the SurrealDB JVM SDK (verify current artifact name/version at implementation time — the JVM driver ecosystem moves; record the choice). Map: entities → tables, edges → `RELATE`, relations → record links. Implement `EntityStore` over it. Integration tests run against SurrealDB via Testcontainers (or the in-memory embedded mode if the chosen driver supports it — prefer Testcontainers for fidelity).

**Done when:** the SPI-semantics portion of the contract kit passes against a real SurrealDB instance.

### Step 14 — LIVE SELECT feed + re-query fallback
Implement `ObservableStore.changeFeed` via `LIVE SELECT`. Per notes §7: live queries may reject clause shapes regular queries accept — implement the **re-query fallback**: when a live registration fails, fall back to type-level live feed + core-side predicate evaluation (which the spine does anyway), and log the downgrade. Reconnect: on connection loss, resubscribe and emit a resync signal the core answers with snapshot refresh (snapshot-on-reconnect, §7 — implement the core hook here if Step 10 didn't).

**Done when:** full contract kit (both tiers, topology tests with a second connection as external writer) passes against SurrealDB. Tag `v0.2.0-m2`.

---

## Phase 5 — M3: Postgres adapter (`hose-store-postgres`)

### Step 15 — JDBC store + NOTIFY feed
Module over plain JDBC (HikariCP). Schema strategy: one table per entity type (jsonb payload + pk + version columns) — simplest honest mapping; record it. Edges via a link table. Feed: triggers writing `pg_notify` on insert/update/delete; a dedicated connection polls `PGConnection.getNotifications(timeout)` (pgjdbc has no push — this *is* the mechanism; document the polling interval as config). NOTIFY is lossy across disconnects: on reconnect, signal resync → core snapshot refresh (same hook as Step 14).

**Done when:** full contract kit passes against Postgres via Testcontainers, including a kill-and-reconnect test proving snapshot-on-reconnect heals missed events. Tag `v0.3.0-m3`.

---

## Backlog (explicitly not in this plan)
Neo4j adapter (tier-one only until a context forces it); Kotlin query DSL over `StoreQuery`; entity codec/schema evolution; Debezium variant of the Postgres feed; poll-refresh crutch for tier-one adapters; browser gateway (§10 of the notes); the pipes-store adapter, someday, when the sibling grows up.