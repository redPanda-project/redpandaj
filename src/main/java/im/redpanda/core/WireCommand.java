package im.redpanda.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Positive marker for a top-level wire command byte in {@link Command}.
 *
 * <p>{@link WireRegistry} renders exactly the annotated constants into the wire registry, and
 * {@code WireRegistryTest} fails as soon as {@link Command} declares a {@code public static final
 * byte} without this marker. A new command therefore cannot be forgotten in the registry, and a
 * byte constant that is not a command cannot slip into it unnoticed (TD092).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface WireCommand {}
