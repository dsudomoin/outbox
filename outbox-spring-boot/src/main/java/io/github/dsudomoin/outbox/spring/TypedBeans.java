package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxSerializer;
import jakarta.annotation.Nullable;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.springframework.aop.framework.AopInfrastructureBean;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.InjectionPoint;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.DependencyDescriptor;
import org.springframework.core.ResolvableType;
import org.springframework.util.ReflectionUtils;

/** What the generic prototype beans of typed payloads read from the injection point they serve. */
final class TypedBeans {

    private TypedBeans() {}

    /**
     * The JSON codec {@code outbox.json.mapper} names, the start failing when it is absent; for auto Jackson 2's, else
     * Jackson 3's, else null. Eager as a named mapper fails the start with no typed injection at all.
     */
    record Json(@Nullable JsonCodec codec) {

        static Json select(OutboxProperties.Json.Mapper mapper, List<JsonCodec> codecs) {
            var present = codecs.stream().filter(JsonCodec::hasMapper).toList();
            return switch (mapper) {
                case AUTO -> present.stream()
                    .filter(c -> c.major() == OutboxProperties.Json.Mapper.JACKSON2).findAny()
                    .or(() -> present.stream().findAny()).map(Json::new).orElse(new Json(null));
                case JACKSON2 -> named(mapper, present, "com.fasterxml.jackson.databind.ObjectMapper");
                case JACKSON3 -> named(mapper, present, "tools.jackson.databind.json.JsonMapper");
            };
        }

        private static Json named(OutboxProperties.Json.Mapper mapper, List<JsonCodec> present, String bean) {
            return new Json(present.stream().filter(c -> c.major() == mapper).findAny().orElseThrow(() ->
                new IllegalStateException("outbox.json.mapper=" + mapper.name().toLowerCase(Locale.ROOT)
                    + " names a mapper that is absent: there is no " + bean + " bean")));
        }
    }

    /**
     * The injection point being served, or null when Spring creates the bean with none (a scan by type). A factory
     * method cannot declare it as a parameter: Spring would refuse before the method runs, failing the whole scan. The
     * helper is created without a bean definition, so a scan never meets it, and its constructor reads the current
     * injection point, the one whose resolution is running this factory method.
     */
    @Nullable
    static DependencyDescriptor injectionPoint(AutowireCapableBeanFactory factory) {
        try {
            return (DependencyDescriptor) factory.createBean(InjectionPointHolder.class).injectionPoint;
        } catch (BeanCreationException e) {
            if (e.getMostSpecificCause() instanceof IllegalStateException) { // "No current InjectionPoint available"
                return null;
            }
            throw e;
        }
    }

    /** Infrastructure, as proxying it would lose its field, or fail the final class, under a broad aspect. */
    static final class InjectionPointHolder implements AopInfrastructureBean {

        final InjectionPoint injectionPoint;

        InjectionPointHolder(InjectionPoint injectionPoint) {
            this.injectionPoint = injectionPoint;
        }
    }

    /**
     * {@code T} of the {@code expected<T>} injected at {@code injectionPoint}, through nested generics and subclasses;
     * null, meaning no bean, when there is no injection point or it is of another type: a collection, array, map or
     * stream, which collects the app's explicit beans, or a scan under another bean's injection. A raw type, {@code ?}
     * and {@code Object} say nothing to serve it by: the JSON would decode to the mapper's maps, and no contract could
     * be read.
     */
    @Nullable
    static ResolvableType typeArgument(@Nullable DependencyDescriptor injectionPoint, Class<?> expected) {
        if (injectionPoint == null) {
            return null;
        }
        var declared = injectionPoint.getResolvableType().as(expected);
        if (declared == ResolvableType.NONE) {
            return null;
        }
        var argument = declared.getGeneric(0);
        if (argument.resolve() == null || argument.resolve() == Object.class) {
            throw new IllegalStateException(expected.getSimpleName() + " at " + injectionPoint + " of "
                + injectionPoint.getMember() + " has no concrete type argument to serve it by: declare it as "
                + expected.getSimpleName() + "<YourType>, or give it a bean of its own");
        }
        return argument;
    }

    /**
     * {@code type} with every variable resolved, as Jackson takes it; generics that stay unresolved (a raw class, a
     * bare wildcard) leave the raw class, which Jackson binds to its bounds.
     */
    static Type jsonType(ResolvableType type) {
        var generics = type.getGenerics();
        if (generics.length == 0 || type.hasUnresolvableGenerics()) {
            return type.toClass();
        }
        return ResolvableType.forClassWithGenerics(type.toClass(), Arrays.stream(generics)
            .map(generic -> ResolvableType.forType(jsonType(generic))).toArray(ResolvableType[]::new)).getType();
    }

    /**
     * The {@code OutboxSerializer<T>} an injection point of that type would get; null, when not {@code required}, if
     * there is none. A provider lookup by {@code ResolvableType} would not do: it skips the fallback pass, the only one
     * the generic beans match in.
     */
    @Nullable
    @SuppressWarnings("unchecked")
    static <T> OutboxSerializer<T> serializer(AutowireCapableBeanFactory factory, ResolvableType type,
            boolean required) {
        var serializer = ResolvableType.forClassWithGenerics(OutboxSerializer.class, type);
        return (OutboxSerializer<T>) factory.resolveDependency(new SerializerSlot(serializer, required, false), null);
    }

    /** An injection point of an {@code OutboxSerializer} of a type known only at runtime. */
    private static final class SerializerSlot extends DependencyDescriptor {

        private static OutboxSerializer<?> slot; // only its declared type counts

        private final ResolvableType type;
        private final boolean fallback;

        SerializerSlot(ResolvableType type, boolean required, boolean fallback) {
            super(ReflectionUtils.findField(SerializerSlot.class, "slot"), required);
            this.type = type;
            this.fallback = fallback;
        }

        @Override
        public ResolvableType getResolvableType() {
            return type;
        }

        @Override
        public boolean fallbackMatchAllowed() {
            return fallback;
        }

        /** The default copy would drop this class's {@code type}. */
        @Override
        public DependencyDescriptor forFallbackMatch() {
            return new SerializerSlot(type, isRequired(), true);
        }
    }
}
