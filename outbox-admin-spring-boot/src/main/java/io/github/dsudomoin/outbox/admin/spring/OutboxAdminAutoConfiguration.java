package io.github.dsudomoin.outbox.admin.spring;

import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.spring.OutboxAutoConfiguration;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.NoUniqueBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.util.function.SingletonSupplier;
import org.springframework.web.servlet.handler.MappedInterceptor;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

/**
 * The admin API on the app's Spring MVC, routes under {@code /kora-outbox/admin/v1}, over the {@link OutboxAdmin} bean:
 * the contract {@code kora-outbox-admin.yaml}, the same paths and JSON as the Kora adapter's. Its error handling and
 * its strict reading of request bodies apply to its own controller only. The app must provide an
 * {@link OutboxAdminAuthorizer} bean, or the start fails; {@code outbox.enabled=false} exposes nothing and needs none.
 */
@AutoConfiguration(after = OutboxAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(RequestMappingHandlerAdapter.class)
@ConditionalOnBean(OutboxAdmin.class)
@EnableConfigurationProperties(OutboxAdminProperties.class)
public final class OutboxAdminAutoConfiguration {

    @Bean
    OutboxAdminController outboxAdminController(OutboxAdmin admin) {
        return new OutboxAdminController(admin);
    }

    /** Never lazy: it fails the start without a Jackson, under {@code spring.main.lazy-initialization} too. */
    @Bean
    @Lazy(false)
    RequestReaders outboxAdminRequestReaders() {
        return new RequestReaders();
    }

    /**
     * Published as a {@link MappedInterceptor} because every handler mapping takes those beans by itself:
     * {@code WebMvcConfigurer.addInterceptors} is ignored for an app extending {@code WebMvcConfigurationSupport},
     * which still maps the controller, and that would leave the routes open. No path patterns: the guard goes by the
     * handler.
     *
     * <p>Needs nothing to be made but its properties, which need nothing themselves (not this configuration either),
     * and takes the authorizer at its first call: the mapping makes this bean during its own start and drops it,
     * silently, if making it comes back to a bean in creation, which an authorizer with the mapping or
     * {@code HandlerMappingIntrospector} among its dependencies would.
     */
    @Bean
    static MappedInterceptor outboxAdminGuard(ObjectProvider<OutboxAdminAuthorizer> authorizers,
        OutboxAdminProperties properties) {
        return new MappedInterceptor(null, new OutboxAdminGuard(SingletonSupplier.of(() -> authorizer(authorizers)),
            properties.checkOrigin()));
    }

    /**
     * Fails the start without an authorizer, once the mappings are up and before the server takes calls. Under
     * {@code spring.main.lazy-initialization} too: Boot keeps this type of bean out of it.
     */
    @Bean
    static SmartInitializingSingleton outboxAdminAuthorizerCheck(ObjectProvider<OutboxAdminAuthorizer> authorizers) {
        return () -> authorizer(authorizers);
    }

    /** No cause on these: with one, Boot's failure analyzer reports the cause in place of this advice. */
    private static OutboxAdminAuthorizer authorizer(ObjectProvider<OutboxAdminAuthorizer> authorizers) {
        try {
            return authorizers.getObject();
        } catch (NoUniqueBeanDefinitionException e) {
            throw new IllegalStateException("several OutboxAdminAuthorizer beans: " + e.getBeanNamesFound()
                + "; the admin API takes one: keep one or mark one @Primary");
        } catch (NoSuchBeanDefinitionException e) {
            throw new IllegalStateException("no OutboxAdminAuthorizer bean: the admin API has no other protection, so"
                + " it does not start without one; define a bean that checks the caller's credentials, or remove"
                + " outbox-admin-spring-boot from the app");
        }
    }
}
