package org.antoined.store.spi;

/**
 * The neutral envelope in which entities cross the store SPI — no generics, no domain
 * classes. The boundary law: <em>generic above the token, neutral below the SPI.</em>
 *
 * <p>{@code key} and {@code version} are the <em>encoded</em> string forms produced by
 * the core's {@code EntityType} codec; adapters never see decoded keys or versions.
 *
 * <p>{@code payload} is opaque to the SPI: for the in-memory adapter (M1) it is the
 * domain instance itself; once an entity codec lands in {@code EntityType} (M2) it is
 * the serialized form.
 */
public record StoredEntity(String type, String key, String version, Object payload) {
}
