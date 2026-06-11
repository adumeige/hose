package io.hose.store.spi;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import org.jspecify.annotations.Nullable;

/**
 * Reference evaluator for {@link StoreQuery} against a payload object — the single
 * definition of what a query <em>means</em>. Adapters without a native query engine
 * (in-memory) call it directly; adapters that translate to a native language must
 * match its semantics; the core uses it to maintain live sets per entity.
 *
 * <p>Field access is reflective, in order: record component accessor / JavaBean-style
 * getter ({@code getX()}, {@code isX()}, or Kotlin-property {@code x()}), then public
 * field. Numbers compare by value ({@code 5L == 5}); other ordered comparisons require
 * {@link Comparable} field values.
 */
public final class StoreQueries {

    private StoreQueries() {
    }

    /** True when {@code payload} satisfies every comparison of {@code query}. */
    public static boolean matches(StoreQuery query, Object payload) {
        for (StoreQuery.FieldComparison c : query.comparisons()) {
            if (!matches(c, payload)) {
                return false;
            }
        }
        return true;
    }

    private static boolean matches(StoreQuery.FieldComparison c, Object payload) {
        Object actual = fieldValue(payload, c.field());
        Object expected = c.value();
        return switch (c.op()) {
            case EQ -> eq(actual, expected);
            case NEQ -> !eq(actual, expected);
            case LT -> compare(actual, expected) < 0;
            case LTE -> compare(actual, expected) <= 0;
            case GT -> compare(actual, expected) > 0;
            case GTE -> compare(actual, expected) >= 0;
        };
    }

    private static boolean eq(@Nullable Object a, @Nullable Object b) {
        if (a instanceof Number na && b instanceof Number nb) {
            return toBig(na).compareTo(toBig(nb)) == 0;
        }
        return java.util.Objects.equals(a, b);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static int compare(@Nullable Object a, @Nullable Object b) {
        if (a == null || b == null) {
            throw new IllegalArgumentException("Cannot order-compare null field values");
        }
        if (a instanceof Number na && b instanceof Number nb) {
            return toBig(na).compareTo(toBig(nb));
        }
        if (a instanceof Comparable ca && a.getClass().isInstance(b)) {
            return ca.compareTo(b);
        }
        throw new IllegalArgumentException(
                "Cannot order-compare " + a.getClass().getName() + " with " + b.getClass().getName());
    }

    private static BigDecimal toBig(Number n) {
        if (n instanceof BigDecimal bd) {
            return bd;
        }
        if (n instanceof Double || n instanceof Float) {
            return BigDecimal.valueOf(n.doubleValue());
        }
        return BigDecimal.valueOf(n.longValue());
    }

    /** Reads {@code field} from {@code payload} reflectively; see class doc for the lookup order. */
    @Nullable
    public static Object fieldValue(Object payload, String field) {
        Class<?> cls = payload.getClass();
        String cap = Character.toUpperCase(field.charAt(0)) + field.substring(1);
        for (String name : new String[] {field, "get" + cap, "is" + cap}) {
            try {
                Method m = cls.getMethod(name);
                if (m.getParameterCount() == 0 && m.getReturnType() != void.class) {
                    return m.invoke(payload);
                }
            } catch (NoSuchMethodException ignored) {
                // try the next accessor shape
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Failed reading field '" + field + "' of " + cls.getName(), e);
            }
        }
        try {
            Field f = cls.getField(field);
            return f.get(payload);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("No readable field '" + field + "' on " + cls.getName());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Failed reading field '" + field + "' of " + cls.getName(), e);
        }
    }
}
