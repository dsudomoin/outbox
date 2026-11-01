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
import java.util.Objects;
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
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.ClassUtils;

/**
 * The handler beans as the runtime takes them: the {@link OutboxHandler} bean, if any, for the topics no route claims,
 * and every {@link OutboxTopicHandler} bean as a route, checked before the runtime is built so that every failure
 * names the bean. A bean behind a Spring AOP proxy is called through the proxy, its advice running per record: its
 * identity (topics, contract version) comes from the proxy's ultimate target, and an advisor that matches a method on
 * the relay's path but none of those the relay really calls on this proxy fails the start. Unproxied beans go in as
 * they are. A handler's transaction must not commit a record the relay treats as failed
 * ({@link TransactionalHandlerCheck}).
 */
final class HandlerBeans {

    /** The Kafka fallback bean of {@code outbox-kafka-spring-boot}, an {@code OutboxHandler}. */
    static final String KAFKA_FALLBACK = "outboxKafkaFallbackHandler";

    private static final Logger log = LoggerFactory.getLogger(HandlerBeans.class);
    private static final Method ROUTE_HANDLE = method(OutboxTopicHandler.class, "handle", OutboxRecord.class);
    private static final Method FALLBACK_HANDLE = method(OutboxHandler.class, "handle", OutboxRecord.class);
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
     * reachable, transactions on that path that roll back whenever the record fails.
     */
    static HandlerBeans collect(ListableBeanFactory beans, ObjectProvider<OutboxHandler> handlerBeans,
            ObjectProvider<OutboxTopicHandler> routeBeans) {
        var fallbacks = handlerBeans.orderedStream().toList();
        var routeList = routeBeans.orderedStream().toList();
        var names = new IdentityHashMap<Object, String>(); // singletons only: never a scoped target
        for (var type : List.of(OutboxHandler.class, OutboxTopicHandler.class)) { // what orderedStream() sees
            BeanFactoryUtils.beansOfTypeIncludingAncestors(beans, type, false, true)
                .forEach((name, bean) -> names.put(bean, name));
        }

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
        handlers.forEach(handler -> TransactionalHandlerCheck.check(handler, beans));
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

    /**
     * Unwraps the proxies to the ultimate target and checks them against the methods the wrapper ({@link #relayed})
     * really calls on the outermost one, each level passing the call on: an overridden {@code handleBatch} alone, as
     * {@code handle} and the hooks then run only through its own calls, past the proxy; else {@code handle}, for a
     * typed target the hooks on a class-based proxy or {@code handle(OutboxRecord)} on an interface-based one. A
     * class-based proxy must not have one of them final: it would run on the proxy's own instance, whose fields are
     * unset. An advisor must match one of them, unless it matches no method of the relay's path at all, an inherited
     * {@code handleBatch} included; advice on another method too, such as an advisor for the whole class, runs on
     * those it matches. As a relay method counts its namesake on any interface of the target, where an advisor can
     * match when the class's own method it does not; as a called method, on an interface-based proxy only, its namesake
     * on a proxied interface, which is what that proxy dispatches; a class-based one dispatches the class's method.
     */
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
                : List.of(target instanceof OutboxHandler ? FALLBACK_HANDLE : ROUTE_HANDLE)) {
            path.add(AopUtils.getMostSpecificMethod(m, targetClass));
        }
        var batch = method(targetClass, "handleBatch", List.class);
        var relayMethods = new ArrayList<>(path);
        relayMethods.add(batch);
        List<Method> called;
        if (overrides(batch)) {
            called = List.of(batch);
            path.add(batch);
        } else if (!(target instanceof TypedOutboxTopicHandler<?>)) {
            called = List.of(path.get(0));
        } else if (bean instanceof TypedOutboxTopicHandler<?>) {
            called = List.copyOf(path.subList(0, 2)); // the hooks
        } else {
            called = List.of(path.get(2)); // handle(OutboxRecord)
        }
        var handler = new Handler(name, bean, target, List.copyOf(path));
        var interfaces = ClassUtils.getAllInterfacesForClass(targetClass);
        for (var proxy : proxies) {
            var dispatched = AopUtils.isJdkDynamicProxy(proxy) ? proxy.getProxiedInterfaces() : new Class<?>[0];
            if (AopUtils.isCglibProxy(proxy)) {
                for (var m : called) {
                    if (Modifier.isFinal(m.getModifiers())) {
                        throw finalCalled(handler, m);
                    }
                }
            }
            for (var advisor : proxy.getAdvisors()) {
                var matched = relayMethods.stream()
                    .filter(m -> matchesWith(interfaces, proxy, advisor, m, targetClass)).toList();
                if (!matched.isEmpty()
                        && called.stream().noneMatch(m -> matchesWith(dispatched, proxy, advisor, m, targetClass))) {
                    throw unreachable(handler, proxy, advisor, matched.get(0));
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
     * The hooks of a typed handler behind an interface-based proxy: each hook calls the proxy's
     * {@code handle(OutboxRecord)}, so that advice on it runs; the target decodes again and calls its own hook, on
     * itself. Advice on the hooks alone cannot run, whatever interfaces declare them, and fails the start
     * ({@link #inspect}).
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
    static boolean matches(Advised proxy, Advisor advisor, Method method, Class<?> targetClass) {
        if (advisor.getAdvice() instanceof ExposeInvocationInterceptor || advisor instanceof IntroductionAdvisor) {
            return false;
        }
        if (advisor instanceof PointcutAdvisor pointcut) {
            return (proxy.isPreFiltered() || pointcut.getPointcut().getClassFilter().matches(targetClass))
                && pointcut.getPointcut().getMethodMatcher().matches(method, targetClass);
        }
        return true;
    }

    private static boolean matchesWith(Class<?>[] interfaces, Advised proxy, Advisor advisor, Method method,
            Class<?> targetClass) {
        return matches(proxy, advisor, method, targetClass) || namesakes(interfaces, method).stream()
            .anyMatch(namesake -> matches(proxy, advisor, namesake, targetClass));
    }

    /** The methods of {@code interfaces} with {@code method}'s signature. */
    static List<Method> namesakes(Class<?>[] interfaces, Method method) {
        return Arrays.stream(interfaces)
            .map(type -> ClassUtils.getMethodIfAvailable(type, method.getName(), method.getParameterTypes()))
            .filter(Objects::nonNull).toList();
    }

    private static IllegalArgumentException finalCalled(Handler handler, Method method) {
        var fix = method.getDeclaringClass() == TypedOutboxTopicHandler.class
            ? "give the bean class-based proxies only, or interface-based ones only" : "make it not final";
        return new IllegalArgumentException("outbox handler bean " + handler + ": " + signature(method) + " is final,"
            + " and the relay calls it on a class-based proxy, where it would run on the proxy's own instance, whose"
            + " fields are unset: " + fix);
    }

    private static IllegalArgumentException unreachable(Handler handler, Advised proxy, Advisor advisor,
            Method method) {
        String why;
        if (handler.overridesBatch()) {
            why = "the relay calls only the overridden handleBatch on the proxy, and its own calls of this method"
                + " bypass the proxy: advise handleBatch, or use a TransactionTemplate inside the override";
        } else if (method.getName().equals("handleBatch")) {
            why = "the relay runs the default handleBatch itself, over handle: override handleBatch or advise handle";
        } else if (AopUtils.isJdkDynamicProxy(proxy)) {
            why = "the relay calls only handle(OutboxRecord) on an interface-based (JDK) proxy, and the target then"
                + " calls its hooks on itself: set spring.aop.proxy-target-class=true (class-based proxies, Spring"
                + " Boot's default), or advise handle(OutboxRecord) instead";
        } else if (Modifier.isFinal(method.getModifiers())) {
            why = "a class-based proxy cannot intercept the final handle(OutboxRecord): advise the hook"
                + " handle(T, OutboxRecord) or handleTombstone instead";
        } else {
            why = "a class-based proxy calls the class's own method, which the advisor matches only through an"
                + " interface: advise the method where the class declares it, or set"
                + " spring.aop.proxy-target-class=false";
        }
        return new IllegalArgumentException("outbox handler bean " + handler + ": advisor " + advisor + " matches "
            + signature(method) + " but no method the relay calls on its proxy, so the advice would never run; " + why);
    }

    static String signature(Method method) {
        return method.getName() + Arrays.stream(method.getParameterTypes()).map(Class::getSimpleName)
            .collect(Collectors.joining(", ", "(", ")"));
    }

    /** True when {@code handleBatch}, as the class has it, is not the default of the core's interfaces. */
    private static boolean overrides(Method batch) {
        var declaring = batch.getDeclaringClass();
        return declaring != OutboxTopicHandler.class && declaring != OutboxHandler.class;
    }

    private static String name(Map<Object, String> names, Object bean) {
        return names.getOrDefault(bean, AopProxyUtils.ultimateTargetClass(bean).getName());
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
