package com.sangiya.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RouteConfig {

    @Bean
    public RouteLocator routes(
            RouteLocatorBuilder builder,
            @Value("${services.order-service.uri:http://localhost:8081}") String orderServiceUri) {
        return builder.routes()
                .route("order-service", r -> r.path("/api/orders/**").uri(orderServiceUri))
                .build();
    }
}
