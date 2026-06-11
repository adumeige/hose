package io.hose.store.spi;

import java.util.List;

/**
 * A declarative, per-entity-evaluable query: the conjunction (AND) of simple field
 * comparisons over one entity type. Deliberately a plain data structure — adapters
 * translate it to their native query language; the core re-evaluates the same shape
 * per entity to maintain live sets. Any richer query DSL is layered above this, in
 * Kotlin, later.
 *
 * <p>An empty comparison list selects every entity of the type.
 */
public record StoreQuery(String type, List<FieldComparison> comparisons) {

    /** Selects all entities of {@code type}. */
    public static StoreQuery all(String type) {
        return new StoreQuery(type, List.of());
    }

    /** One {@code field op value} clause. {@code value} is a literal in payload terms. */
    public record FieldComparison(String field, Op op, Object value) {
    }

    /** Comparison operators — the per-entity-evaluable minimum. */
    public enum Op {
        EQ, NEQ, LT, LTE, GT, GTE
    }
}
