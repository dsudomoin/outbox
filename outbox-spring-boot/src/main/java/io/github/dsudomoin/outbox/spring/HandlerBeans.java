package io.github.dsudomoin.outbox.spring;

import io.github.dsudomoin.outbox.OutboxHandler;
import io.github.dsudomoin.outbox.OutboxRecord;
import io.github.dsudomoin.outbox.OutboxTopicHandler;
import io.github.dsudomoin.outbox.Outcome;
import io.github.dsudomoin.outbox.TypedOutboxTopicHandler;
import jakarta.annotation.Nullable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.Advisor;
import org.springframework.aop.IntroductionAdvisor;
import org.springframework.aop.PointcutAdvisor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.interceptor.ExposeInvocationInterceptor;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.ClassUtils;

/**
 * The handler beans as the runtime takes them: the {@link OutboxHandler} bean, if any, for the topics no route claims,
 * and every {@link OutboxTopicHandler} bean as a route, checked before the runtime is built so that every failure
 * names the bean. A bean behind a Spring AOP proxy is called through the proxy, its advice running per record (spec
 * §5.4): its identity (topics, contract version) comes from the proxy's ultimate target, and every advisor that
 * matches a method on the relay's path must be one this proxy can intercept, else the start fails. Unproxied beans go
 * in as they are.
 */
final class HandlerBeans {

    /** The Kafka fallback bean of {@code outbox-kafka-spring-boot}, an {@code OutboxHandler}. */
    static final String KAFKA_FALLBACK = "outboxKafkaFallbackHandler";

    private static final Logger log = LoggerFactory.getLogger(HandlerBeans.class);
    private static final Method HANDLE = method(OutboxTopicHandler.class, "handle", OutboxRecord.class);
    private static final Method TYPED_HANDLE = method(TypedOutboxTopicHandler.class, "handle", OutboxRecord.class);
    private static final Method HOOK = method(TypedOutboxTopicHandler.class, "handle", Object.class,
        OutboxRecord.class);
    private static final Method TOMBSTONE = method(TypedOutboxTopicHandler.class, "handleTombstone",
        OutboxRecord.class);

    /**
     * A handler bean: its name, the bean (a proxy or not), its ultimate target, and the methods the relay's calls reach
     * on it, as the target's class declares them (bridges resolved): for a plain handler {@code handle}, for a typed
     * one the hook {@code handle(T, OutboxRecord)}, {@code handleTombstone} and the final {@code handle(OutboxRecord)},
     * and {@code handleBatch} when the class overrides it.
     */
    record Handler(String name, Object bean, Object target, List<Method> relayPath) {

        Class<?> targetClass() {
            return target.getClass();
        }

        boolean overridesBatch() {
            return relayPath.stream().anyMatch(m -> m.getName().equals("handleBatch"));
        }

        @Override
        public String toString() {
            return name + " (" + targetClass().getName() + ")";
        }
    }

    @Nullable
    private final OutboxHandler fallback;
    private final List<OutboxTopicHandler> routes;
    private final List<Handler> handlers;
    private final String routesLine;

    private HandlerBeans(@Nullable OutboxHandler fallback, List<OutboxTopicHandler> routes, List<Handler> handlers,
            String routesLine) {
        this.fallback = fallback;
        this.routes = routes;
        this.handlers = handlers;
        this.routesLine = routesLine;
    }

    /**
     * Collects and checks the handler beans: no bean of both kinds, at most one {@code OutboxHandler}, every route
     * claims topics and no two claim one, proxies with a singleton target whose advice on the relay's path is
     * reachable.
     */
    static HandlerBeans collect(ListableBeanFactory beans, ObjectProvider<OutboxHandler> handlerBeans,
            ObjectProvider<OutboxTopicHandler> routeBeans) {
        var fallbacks = handlerBeans.orderedStream().toList();
        var routeList = routeBeans.orderedStream().toList();
        var names = new IdentityHashMap<Object, String>(); // singletons only: never a scoped target
        beans.getBeansOfType(OutboxHandler.class, false, true).forEach((name, bean) -> names.put(bean, name));
        beans.getBeansOfType(OutboxTopicHandler.class, false, true).forEach((name, bean) -> names.put(bean, name));

        for (var bean : fallbacks) {
            if (bean instanceof OutboxTopicHandler) {
                throw new IllegalArgumentException("bean " + describe(names, bean) + " is both an OutboxHandler and an"
                    + " OutboxTopicHandler: implement OutboxTopicHandler only, for its own topics, or OutboxHandler"
                    + " only, for every topic no route claims");
            }
        }
        if (fallbacks.size() > 1) {
            var all = fallbacks.stream().map(bean -> describe(names, bean)).collect(Collectors.joining(", "));
            boolean kafka = fallbacks.stream().anyMatch(bean -> KAFKA_FALLBACK.equals(names.get(bean)));
            throw new IllegalArgumentException("several OutboxHandler beans: " + all + "; the outbox takes one, for"
                + " the topics no OutboxTopicHandler claims" + (kafka ? "; outbox-kafka-spring-boot provides "
                + KAFKA_FALLBACK + " while outbox.kafka.fallback=true: set outbox.kafka.fallback=false to let your own"
                + " handler take them" : ": keep one"));
        }

        var handlers = new ArrayList<Handler>();
        OutboxHandler fallback = null;
        String fallbackName = "refused (no OutboxHandler)";
        if (!fallbacks.isEmpty()) {
            var handler = inspect(name(names, fallbacks.get(0)), fallbacks.get(0));
            handlers.add(handler);
            fallback = (OutboxHandler) relayed(handler, Set.of());
            fallbackName = handler.name();
        }
        var routes = new ArrayList<OutboxTopicHandler>();
        var claims = new TreeMap<String, Handler>();
        for (var bean : routeList) {
            var handler = inspect(name(names, bean), bean);
            handlers.add(handler);
            var topics = ((OutboxTopicHandler) handler.target()).topics();
            if (topics.isEmpty()) {
                throw new IllegalArgumentException("bean " + handler + " claims no topics");
            }
            for (var topic : topics) {
                var other = claims.putIfAbsent(topic, handler);
                if (other != null) {
                    throw new IllegalArgumentException("outbox topic " + topic + " is claimed by both beans " + other
                        + " and " + handler);
                }
            }
            routes.add((OutboxTopicHandler) relayed(handler, topics));
        }
        var line = claims.isEmpty() ? "none" : claims.entrySet().stream()
            .map(e -> e.getKey() + " -> " + e.getValue().name()).collect(Collectors.joining(", "));
        return new HandlerBeans(fallback, List.copyOf(routes), List.copyOf(handlers),
            "outbox routes: " + line + "; other topics -> " + fallbackName);
    }

    /** The {@code OutboxHandler} for the topics no route claims, as the runtime takes it; null without one. */
    @Nullable
    OutboxHandler fallback() {
        return fallback;
    }

    /** The routes, as the runtime takes them. */
    List<OutboxTopicHandler> routes() {
        return routes;
    }

    /** Every handler bean, the fallback first: for checks of what runs on the relay's path. */
    List<Handler> handlers() {
        return handlers;
    }

    /** Logs each topic's route bean and where every other topic goes, at INFO. */
    void logRoutes() {
        log.info("{}", routesLine);
    }

    /** Unwraps the proxies to the ultimate target and checks that each can intercept what its advisors match. */
    private static Handler inspect(String name, Object bean) {
        var proxies = new ArrayList<Advised>();
        var target = bean;
        while (AopUtils.isAopProxy(target)) {
            var next = AopProxyUtils.getSingletonTarget(target);
            if (next == null) {
                throw new IllegalArgumentException("outbox handler bean " + name + " ("
                    + AopProxyUtils.ultimateTargetClass(bean).getName() + ") is a proxy without a singleton target"
                    + " (a scoped or refreshable bean, or another dynamic TargetSource): the outbox reads its topics"
                    + " once and calls one object for every record; make it a plain singleton");
            }
            proxies.add((Advised) target);
            target = next;
        }
        var targetClass = target.getClass();
        var path = new ArrayList<Method>();
        for (var m : target instanceof TypedOutboxTopicHandler<?> ? List.of(HOOK, TOMBSTONE, TYPED_HANDLE)
                : List.of(HANDLE)) {
            path.add(AopUtils.getMostSpecificMethod(m, targetClass));
        }
        var batch = method(targetClass, "handleBatch", List.class);
        if (overrides(batch)) {
            path.add(batch);
        }
        var handler = new Handler(name, bean, target, List.copyOf(path));
        for (var proxy : proxies) {
            for (var advisor : proxy.getAdvisors()) {
                for (var m : handler.relayPath()) {
                    if (matches(proxy, advisor, m, targetClass) && !reachable(proxy, m)) {
                        throw unreachable(handler, proxy, advisor, m);
                    }
                }
            }
        }
        return handler;
    }

    /** What the runtime calls: the bean itself when unproxied, else a wrapper calling through the proxy. */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object relayed(Handler handler, Set<String> topics) {
        var bean = handler.bean();
        if (bean == handler.target()) {
            return bean;
        }
        if (handler.target() instanceof TypedOutboxTopicHandler typed) {
            // decode on the target; hooks on a class-based proxy, or through an interface-based one's handle
            return TypedOutboxTopicHandler.calledThrough(typed,
                bean instanceof TypedOutboxTopicHandler proxy ? proxy : new ViaInterface((OutboxTopicHandler) bean));
        }
        var batch = handler.overridesBatch();
        if (bean instanceof OutboxHandler proxy) {
            return new ProxiedHandler(handler.toString(), topics, proxy::handle, batch ? proxy::handleBatch : null);
        }
        var proxy = (OutboxTopicHandler) bean;
        return new ProxiedHandler(handler.toString(), topics, proxy::handle, batch ? proxy::handleBatch : null);
    }

    /**
     * The hooks of a typed handler behind an interface-based proxy, which only proxies its interface methods: each hook
     * calls the proxy's {@code handle(OutboxRecord)}, so that advice on it runs; the target decodes again and calls its
     * own hook, which no advice matches ({@link #inspect}).
     */
    // ponytail: such a payload is decoded twice (once here for the contract and decode checks, once by the target)
    private static final class ViaInterface extends TypedOutboxTopicHandler<Object> {

        private final OutboxTopicHandler proxy;

        ViaInterface(OutboxTopicHandler proxy) {
            super(Set.of(), (topic, payload) -> payload, Object.class); // unused: calledThrough keeps the target's
            this.proxy = proxy;
        }

        @Override
        protected void handle(Object value, OutboxRecord record) throws Exception {
            proxy.handle(record);
        }

        @Override
        protected void handleTombstone(OutboxRecord record) throws Exception {
            proxy.handle(record);
        }

        @Override
        public List<Outcome> handleBatch(List<OutboxRecord> records) {
            return proxy.handleBatch(records);
        }
    }

    /**
     * As the proxy applies it when called (DefaultAdvisorChainFactory): every advisor but introductions and the
     * invocation exposer, which matches all; the class filter only when the auto-proxy creator did not apply it already
     * (a {@code bean(...)} pointcut matches no class outside the proxy's creation).
     */
    private static boolean matches(Advised proxy, Advisor advisor, Method method, Class<?> targetClass) {
        if (advisor.getAdvice() instanceof ExposeInvocationInterceptor || advisor instanceof IntroductionAdvisor) {
            return false;
        }
        if (advisor instanceof PointcutAdvisor pointcut) {
            return (proxy.isPreFiltered() || pointcut.getPointcut().getClassFilter().matches(targetClass))
                && pointcut.getPointcut().getMethodMatcher().matches(method, targetClass);
        }
        return true;
    }

    /** An interface-based proxy intercepts the methods of its interfaces, a class-based one those not final. */
    private static boolean reachable(Advised proxy, Method method) {
        if (AopUtils.isJdkDynamicProxy(proxy)) {
            return Arrays.stream(proxy.getProxiedInterfaces())
                .anyMatch(i -> ClassUtils.hasMethod(i, method.getName(), method.getParameterTypes()));
        }
        return !Modifier.isFinal(method.getModifiers());
    }

    private static IllegalArgumentException unreachable(Handler handler, Advised proxy, Advisor advisor,
            Method method) {
        var signature = method.getName() + Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName)
            .collect(Collectors.joining(", ", "(", ")"));
        String why;
        String fix;
        if (AopUtils.isJdkDynamicProxy(proxy)) {
            why = "an interface-based (JDK) proxy, which intercepts only the methods of the interfaces it implements";
            fix = "set spring.aop.proxy-target-class=true (class-based proxies, Spring Boot's default), or advise"
                + " handle(OutboxRecord) instead";
        } else {
            why = "a class-based proxy, which cannot intercept a final method";
            fix = method.getDeclaringClass() == TypedOutboxTopicHandler.class
                ? "advise the hook handle(T, OutboxRecord) or handleTombstone instead of the final handle(OutboxRecord)"
                : "make the method not final";
        }
        return new IllegalArgumentException("outbox handler bean " + handler + ": advisor " + advisor + " matches "
            + signature + ", which the relay calls but its proxy never runs the advice on: it is " + why + "; " + fix);
    }

    /** True when {@code handleBatch}, as the class has it, is not the default of the core's interfaces. */
    private static boolean overrides(Method batch) {
        return batch.getDeclaringClass() != OutboxTopicHandler.class && batch.getDeclaringClass() != OutboxHandler.class;
    }

    private static String name(Map<Object, String> names, Object bean) {
        return names.getOrDefault(bean, ClassUtils.getUserClass(bean).getName());
    }

    private static String describe(Map<Object, String> names, Object bean) {
        return name(names, bean) + " (" + AopProxyUtils.ultimateTargetClass(bean).getName() + ")";
    }

    private static Method method(Class<?> type, String name, Class<?>... parameters) {
        var m = ClassUtils.getMethodIfAvailable(type, name, parameters);
        if (m == null) { // a protected one
            try {
                m = type.getDeclaredMethod(name, parameters);
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        }
        return m;
    }
}
