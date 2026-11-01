package io.github.dsudomoin.outbox.spring;

import static io.github.dsudomoin.outbox.spring.HandlerBeans.signature;

import io.github.dsudomoin.outbox.spring.HandlerBeans.Handler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.interceptor.NoRollbackRuleAttribute;
import org.springframework.transaction.interceptor.RuleBasedTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttributeSource;

/**
 * Keeps a handler's transaction in line with what the relay records: an exception leaving a method on the relay's path
 * means the record failed, so the transaction must roll back on every exception, or its Inbox marker would commit and
 * skip the retry. An overridden {@code handleBatch} must not run in one transaction: that commits the records it
 * returns as not delivered too. Policies come from the transaction interceptors of the bean's proxies, global rollback
 * rules included; for a bean without one, as under AspectJ weaving, from the context's
 * {@link TransactionAttributeSource} beans. Transactions opened further down, by a service the handler calls, are out
 * of its sight.
 */
final class TransactionalHandlerCheck {

    /** The propagations that give a method a transaction; the relay's handler thread has none to join or suspend. */
    private static final Set<Integer> BEGINS_OR_JOINS = Set.of(TransactionDefinition.PROPAGATION_REQUIRED,
        TransactionDefinition.PROPAGATION_REQUIRES_NEW, TransactionDefinition.PROPAGATION_NESTED,
        TransactionDefinition.PROPAGATION_MANDATORY);

    private TransactionalHandlerCheck() {}

    /** Throws, naming the bean, the method and the fix, if the handler's transactions could commit a failed record. */
    static void check(Handler handler, ListableBeanFactory beans) {
        // first: refused whatever its policy, so a rollback hint for a class-level annotation would mislead
        for (var method : handler.relayPath()) {
            if (method.getName().equals("handleBatch") && !attributes(handler, method, beans).isEmpty()) {
                throw new IllegalArgumentException("outbox handler bean " + handler + ": " + signature(method)
                    + " runs in one transaction around the batch, which also commits the effects and Inbox markers of"
                    + " the records it returns as not delivered: put @Transactional(rollbackFor = Exception.class) on"
                    + " handle and keep the default handleBatch, or use a TransactionTemplate per record inside"
                    + " handleBatch, or one around the batch that throws on any failure");
            }
        }
        for (var method : handler.relayPath()) {
            if (!attributes(handler, method, beans).stream().allMatch(TransactionalHandlerCheck::rollsBackAlways)) {
                throw new IllegalArgumentException("outbox handler bean " + handler + ": " + signature(method)
                    + " runs in a transaction that commits on some exception, while the relay retries the record, and"
                    + " an Inbox marker committed with it would skip the retry and lose the work: use"
                    + " @Transactional(rollbackFor = Exception.class) without noRollbackFor, or"
                    + " @EnableTransactionManagement(rollbackOn = RollbackOn.ALL_EXCEPTIONS)");
            }
        }
    }

    private static boolean rollsBackAlways(TransactionAttribute attribute) {
        return attribute.rollbackOn(new Exception()) && attribute.rollbackOn(new Error())
            && !(attribute instanceof RuleBasedTransactionAttribute rules
                && rules.getRollbackRules().stream().anyMatch(NoRollbackRuleAttribute.class::isInstance));
    }

    /** The transactions {@code method} runs in: one per interceptor applying to it, nested proxies included. */
    private static List<TransactionAttribute> attributes(Handler handler, Method method, ListableBeanFactory beans) {
        var targetClass = handler.targetClass();
        var sources = new ArrayList<TransactionAttributeSource>();
        boolean intercepted = false;
        for (var bean = handler.bean(); AopUtils.isAopProxy(bean); bean = AopProxyUtils.getSingletonTarget(bean)) {
            var proxy = (Advised) bean;
            for (var advisor : proxy.getAdvisors()) {
                if (advisor.getAdvice() instanceof TransactionAspectSupport interceptor) {
                    intercepted = true;
                    if (HandlerBeans.matches(proxy, advisor, method, targetClass)) {
                        sources.add(interceptor.getTransactionAttributeSource());
                    }
                }
            }
        }
        if (!intercepted) {
            sources.addAll(BeanFactoryUtils.beansOfTypeIncludingAncestors(beans, TransactionAttributeSource.class,
                false, true).values());
        }
        return sources.stream().filter(Objects::nonNull)
            .map(source -> source.getTransactionAttribute(method, targetClass)).filter(Objects::nonNull)
            .filter(attribute -> BEGINS_OR_JOINS.contains(attribute.getPropagationBehavior())).toList();
    }
}
