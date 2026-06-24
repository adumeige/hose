# hose

Reactive entity streaming over pluggable stores.

Hose gives you `StateFlow`-backed handles for individual entities and `Flow<SetEvent<K>>`-backed live sets for queries — backed by any store that implements the SPI. Your domain classes are untouched: no marker interfaces, no annotations, no code generation.

## How it works

Entities are arbitrary classes. Hose capabilities live beside them in an `EntityType` type-class that you declare once:

```kotlin
val TODO = entityType<Todo, Long, Instant>("todo") {
    pk { it.id }
    version({ it.updatedAt }, Versions.instant())
}
```

You then hand a store and your types to `Hose` and get back reactive handles:

```kotlin
val hose = Hose(store, setOf(TODO))

// StateFlow<Todo?> — always holds the latest value, null until the entity exists
val todo: StateFlow<Todo?> = hose.entity(TODO, 42L)

// Flow<SetEvent<Long>> — snapshot on first collect, then deltas
val activeTodos: Flow<SetEvent<Long>> = hose.liveSet(TODO, StoreQuery.all("todo"))

// Optimistic writes: routing happens before the store confirms
hose.upsert(TODO, updatedTodo)
hose.delete(TODO, 42L, version)
```

Writes are **optimistic**: collectors see the change before the store round-trip completes. If the store rejects the write, a revert is applied and the failure surfaces on `hose.writeFailures`.

## Core concepts

### EntityType

The type-class that binds a domain class `E` to its primary key type `K` and version type `V`:

| Field | Purpose |
|---|---|
| `name` | Logical type name — **persisted**, renaming it orphans stored data |
| `pk` | Extract the primary key from an entity instance |
| `encodeKey` | Stable, injective `K → String` (default: `toString()`) |
| `version` | Extract version from entity + encode/decode pair (all-or-nothing) |

Built-in version strategies: `Versions.instant()`, `Versions.long()`, `Versions.comparable()`, `Versions.lexicographic()`, `Versions.none()`.

### Entity handles

`hose.entity(type, pk)` returns a `StateFlow<E?>` shared across all callers for the same (type, pk) pair. The runtime keeps exactly one handle alive per key — multiple collectors share a single upstream subscription. Handles are evicted after a configurable grace period once all collectors have gone away.

### Live sets

`hose.liveSet(type, query)` returns a `Flow<SetEvent<K>>` that emits:

- `SetEvent.Snapshot(keys)` on first collect — the current matching set
- `SetEvent.Added(key)` / `SetEvent.Removed(key)` as the set changes

Membership is maintained in-process by routing each applied mutation through registered predicates. Predicates run on the spine and must be **pure and cheap**.

### Mutation tap

`hose.subscribe(types)` emits every applied mutation in spine order. Rejected-stale and absorbed-echo mutations are invisible.

## Store adapters

| Module | When to use |
|---|---|
| `hose-store-memory` | Tests, local development, ephemeral state |
| `hose-store-postgres` | Production — durable, LISTEN/NOTIFY change feed |
| `hose-store-surreal` | Production — SurrealDB, LIVE SELECT change feed |

All adapters are tested against the shared contract suite in `hose-contract-tests`.

### Postgres

One table per entity type (`hose_<name>`), a trigger on each table, and a `LISTEN` connection polling `hose_changes`. Large payloads (> ~7.5 KB) are omitted from the notify and re-fetched on demand.

```kotlin
val store = PostgresStore(dataSource, setOf(TODO))
```

### SurrealDB

Speaks WebSocket RPC directly (not the JVM SDK, which lacks LIVE SELECT). One table per entity type. Query filtering is done in-process.

```kotlin
val store = SurrealStore("ws://localhost:8000/rpc", "root", "root", "test", "test", setOf(TODO))
```

## Adding to your project

Packages are published to GitHub Packages. Add the repository and dependency to your `pom.xml`:

```xml
<repositories>
    <repository>
        <id>github-hose</id>
        <url>https://maven.pkg.github.com/adumeige/hose</url>
    </repository>
</repositories>

<dependency>
    <groupId>org.antoined</groupId>
    <artifactId>hose-core</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
<!-- add hose-store-spi, hose-store-postgres, etc. as needed -->
```

GitHub Packages requires authentication even for public packages. Add your token to `~/.m2/settings.xml`:

```xml
<server>
    <id>github-hose</id>
    <username>YOUR_GITHUB_USERNAME</username>
    <password>YOUR_GITHUB_TOKEN</password>
</server>
```

## Building locally

Java 21 and Maven 3.9+ required.

```bash
mvn clean verify          # full build + all tests
mvn clean compile         # compile only
mvn test -f hose-core/pom.xml   # single module
```

Integration tests for the Postgres and SurrealDB adapters spin up Docker containers via the `docker` CLI. Docker must be running.

## Writing a new adapter

1. Implement `EntityStore` (required) and optionally `ObservableStore` from `hose-store-spi`
2. Extend `EntityStoreContract` (and `ObservableStoreContract` if applicable) from `hose-contract-tests`
3. Provide a `StoreAdapterFixture` that starts and stops your infrastructure
4. Contain zero unchecked casts — the contract test suite enforces this

## Module layout

```
hose-store-spi/        Plain Java SPI — EntityStore, ObservableStore, StoredEntity, StoreQuery
hose-core/             Runtime — spine, identity map, handles, live sets, write path
hose-contract-tests/   Shared test base classes for adapter authors
hose-store-memory/     In-memory adapter
hose-store-postgres/   Postgres adapter
hose-store-surreal/    SurrealDB adapter
hose-vaadin-demo/      Proof-of-concept Vaadin 24 todo app
```
