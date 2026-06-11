package io.hose.store.spi;

import org.jspecify.annotations.Nullable;

/**
 * Receives change-feed events from an {@link ObservableStore}. Implemented by the
 * core; called from adapter threads. Callbacks must be fast and non-blocking.
 */
public interface FeedListener {

    /** An entity was created or updated. */
    void onUpsert(StoredEntity entity);

    /** An entity was deleted; {@code version} when the store can supply it. */
    void onDelete(String type, String key, @Nullable String version);

    /**
     * The feed lost continuity (reconnect after a connection drop, missed events):
     * the subscriber must refresh from snapshots. Default no-op so simple adapters
     * that never lose continuity need not care.
     */
    default void onResync() {
    }
}
