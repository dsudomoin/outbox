package io.github.dsudomoin.outbox.admin.spring;

import io.github.dsudomoin.outbox.OutboxAdmin;
import io.github.dsudomoin.outbox.spring.OutboxAutoConfiguration;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;

/**
 * The admin API on the app's Spring MVC, routes under {@code /kora-outbox/admin/v1}, over the {@link OutboxAdmin} bean:
 * the contract {@code kora-outbox-admin.yaml}, the same paths and JSON as the Kora adapter's. Its error handling and
 * its strict reading of request bodies apply to its own controller only.
 */
@AutoConfiguration(after = OutboxAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnClass(RequestMappingHandlerAdapter.class)
@ConditionalOnBean(OutboxAdmin.class)
public final class OutboxAdminAutoConfiguration {

    @Bean
    OutboxAdminController outboxAdminController(OutboxAdmin admin) {
        return new OutboxAdminController(admin);
    }

    @Bean
    RequestReaders outboxAdminRequestReaders(ObjectProvider<RequestMappingHandlerAdapter> adapters) {
        return new RequestReaders(adapters);
    }
}
