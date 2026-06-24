package org.antoined.store.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * The Step 04 gate: the SPI is implementable from plain Java with no Kotlin-isms —
 * this trivial map-backed implementation is the proof.
 */
class JavaConsumabilityTest {

    /** Plain-Java EntityStore implementation; compiling it is most of the test. */
    static final class MapStore implements EntityStore {
        private final Map<String, StoredEntity> data = new LinkedHashMap<>();

        private static String at(String type, String key) {
            return type + "/" + key;
        }

        @Override
        @Nullable
        public StoredEntity get(String type, String key) {
            return data.get(at(type, key));
        }

        @Override
        public Set<StoredEntity> query(StoreQuery query) {
            return data.values().stream()
                    .filter(e -> e.type().equals(query.type()))
                    .filter(e -> StoreQueries.matches(query, e.payload()))
                    .collect(Collectors.toSet());
        }

        @Override
        public Map<Link, StoredEntity> follow(Set<Link> links) {
            Map<Link, StoredEntity> result = new HashMap<>();
            for (Link link : links) {
                StoredEntity found = get(link.type(), link.key());
                if (found != null) {
                    result.put(link, found);
                }
            }
            return result;
        }

        @Override
        public StoredEntity upsert(StoredEntity entity) {
            data.put(at(entity.type(), entity.key()), entity);
            return entity;
        }

        @Override
        public void delete(String type, String key, @Nullable String version) {
            data.remove(at(type, key));
        }
    }

    public record Person(String name, int age) {
    }

    @Test
    void crudRoundTrip() {
        MapStore store = new MapStore();
        StoredEntity alice = new StoredEntity("person", "1", "v1", new Person("alice", 34));
        store.upsert(alice);
        assertEquals(alice, store.get("person", "1"));
        store.delete("person", "1", "v1");
        assertNull(store.get("person", "1"));
    }

    @Test
    void queryEvaluatesFieldComparisons() {
        MapStore store = new MapStore();
        store.upsert(new StoredEntity("person", "1", "v1", new Person("alice", 34)));
        store.upsert(new StoredEntity("person", "2", "v1", new Person("bob", 19)));

        StoreQuery adults = new StoreQuery("person",
                List.of(new StoreQuery.FieldComparison("age", StoreQuery.Op.GTE, 21)));
        Set<StoredEntity> result = store.query(adults);
        assertEquals(1, result.size());
        assertEquals("1", result.iterator().next().key());
    }

    @Test
    void followResolvesExistingLinksOnly() {
        MapStore store = new MapStore();
        StoredEntity alice = new StoredEntity("person", "1", "v1", new Person("alice", 34));
        store.upsert(alice);

        Map<Link, StoredEntity> resolved =
                store.follow(Set.of(new Link("person", "1"), new Link("person", "404")));
        assertEquals(Map.of(new Link("person", "1"), alice), resolved);
    }

    @Test
    void queryEvaluatorSemantics() {
        Person p = new Person("alice", 34);
        assertTrue(StoreQueries.matches(StoreQuery.all("person"), p));
        assertTrue(StoreQueries.matches(new StoreQuery("person",
                List.of(new StoreQuery.FieldComparison("name", StoreQuery.Op.EQ, "alice"))), p));
        assertFalse(StoreQueries.matches(new StoreQuery("person",
                List.of(new StoreQuery.FieldComparison("age", StoreQuery.Op.LT, 21))), p));
        // numeric widening: long literal vs int field
        assertTrue(StoreQueries.matches(new StoreQuery("person",
                List.of(new StoreQuery.FieldComparison("age", StoreQuery.Op.EQ, 34L))), p));
    }
}
