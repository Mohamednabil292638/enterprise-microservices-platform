package com.sangiya.gateway.ratelimit;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
public class RateLimitGlobalFilter implements GlobalFilter, Ordered {

    private static final String CLIENT_ID_HEADER = "X-Client-Id";

    private final InMemoryRateLimiter rateLimiter;

    public RateLimitGlobalFilter(InMemoryRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String clientId = exchange.getRequest().getHeaders().getFirst(CLIENT_ID_HEADER);
        String key = clientId != null
                ? clientId
                : exchange.getRequest().getRemoteAddress() != null
                        ? exchange.getRequest().getRemoteAddress().getHostString()
                        : "unknown";

        if (!rateLimiter.tryConsume(key)) {
            exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
            return exchange.getResponse().setComplete();
        }
        return chain.filter(exchange);
    }

    @Override
    public int getOrder() {
        return -1; // run before routing
    }
}
