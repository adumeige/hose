package org.antoined.store.spi;

/**
 * A typed reference to one entity — logical type name plus encoded key.
 *
 * <p>{@link EntityStore#follow(java.util.Set)} resolves a batch of links to their
 * target entities; this is how the core dereferences record links / relations without
 * the domain model crossing the SPI.
 */
public record Link(String type, String key) {
}
