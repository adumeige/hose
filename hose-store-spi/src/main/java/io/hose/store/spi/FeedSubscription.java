package io.hose.store.spi;

/** Handle on an active change-feed subscription; close to unsubscribe. Idempotent. */
public interface FeedSubscription extends AutoCloseable {

    @Override
    void close();
}
