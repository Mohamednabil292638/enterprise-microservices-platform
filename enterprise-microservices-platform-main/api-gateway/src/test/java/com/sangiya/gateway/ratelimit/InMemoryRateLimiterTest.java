package com.sangiya.gateway.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class InMemoryRateLimiterTest {

    @Test
    void allowsRequestsUpToTheConfiguredCapacityWithinTheWindow() {
        InMemoryRateLimiter limiter = new InMemoryRateLimiter(3, Duration.ofSeconds(30));

        assertThat(limiter.tryConsume("client-a")).isTrue();
        assertThat(limiter.tryConsume("client-a")).isTrue();
        assertThat(limiter.tryConsume("client-a")).isTrue();
        assertThat(limiter.tryConsume("client-a")).isFalse();
    }

    @Test
    void tracksEachClientKeyIndependently() {
        InMemoryRateLimiter limiter = new InMemoryRateLimiter(1, Duration.ofSeconds(30));

        assertThat(limiter.tryConsume("client-a")).isTrue();
        assertThat(limiter.tryConsume("client-a")).isFalse();
        assertThat(limiter.tryConsume("client-b")).isTrue();
    }

    @Test
    void resetsTheBudgetOnceTheWindowElapses() throws InterruptedException {
        InMemoryRateLimiter limiter = new InMemoryRateLimiter(1, Duration.ofMillis(200));

        assertThat(limiter.tryConsume("client-a")).isTrue();
        assertThat(limiter.tryConsume("client-a")).isFalse();

        Thread.sleep(250);

        assertThat(limiter.tryConsume("client-a")).isTrue();
    }
}
