package com.sangiya.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.gateway.route.RouteLocator;
import reactor.test.StepVerifier;

@SpringBootTest
class RouteConfigTest {

    @Autowired
    private RouteLocator routeLocator;

    @Test
    void definesAnOrderServiceRoute() {
        StepVerifier.create(routeLocator.getRoutes().filter(route -> route.getId().equals("order-service")))
                .expectNextMatches(route -> route.getUri().toString().equals("http://localhost:19191"))
                .verifyComplete();
    }
}
