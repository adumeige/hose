package io.hose.store.spi;

import java.util.Set;

/**
 * The optional adapter tier: a store that can push changes. The feed is
 * listener-based and plain-Java; the core bridges it onto its mutation channel.
 */
public interface ObservableStore extends EntityStore {

    /**
     * Subscribes {@code listener} to changes of the given logical {@code types}.
     * Events must be delivered for every write that reaches the store — including
     * this process's own writes (the core absorbs echoes) — and in per-key order.
     *
     * <p>Listener callbacks may be invoked from adapter-owned threads; they must
     * return quickly (the core only enqueues).
     */
    FeedSubscription changeFeed(Set<String> types, FeedListener listener);
}
