package org.antoined.store.spi;

import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;

/**
 * The required adapter tier: blocking CRUD plus one-shot query over a store.
 *
 * <p><strong>Threading contract:</strong> all methods are <em>blocking</em>; the core
 * invokes them on {@code Dispatchers.IO}, never on the spine. Implementations must be
 * safe for concurrent calls.
 *
 * <p>Keys and versions cross this boundary <em>only</em> in encoded string form
 * (produced by the core's {@code EntityType} codec); domain key/version types never
 * appear below the SPI.
 *
 * <p>Liveness is the core's job: {@link #query(StoreQuery)} is a one-shot snapshot.
 * Stores that can push changes additionally implement {@link ObservableStore};
 * capability detection is a plain {@code instanceof} check.
 */
public interface EntityStore {

    /** The entity stored under ({@code type}, {@code key}), or null if absent. */
    @Nullable
    StoredEntity get(String type, String key);

    /** One-shot evaluation of {@code query}; snapshot semantics, no liveness. */
    Set<StoredEntity> query(StoreQuery query);

    /**
     * Batch-resolves {@code links} to their targets. Links whose target does not
     * exist are absent from the result map.
     */
    Map<Link, StoredEntity> follow(Set<Link> links);

    /** Creates or replaces the entity; returns the stored form. */
    StoredEntity upsert(StoredEntity entity);

    /**
     * Deletes the entity stored under ({@code type}, {@code key}). {@code version}
     * is the encoded version the caller believes it is deleting, or null when the
     * origin cannot supply one; adapters may use it for feed events or conflict
     * checks and are free to ignore it.
     */
    void delete(String type, String key, @Nullable String version);
}
