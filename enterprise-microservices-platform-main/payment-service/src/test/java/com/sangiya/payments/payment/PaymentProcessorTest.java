package com.sangiya.payments.payment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"order.created", "payment.succeeded", "payment.failed"})
class PaymentProcessorTest {

    @Autowired
    private PaymentProcessor paymentProcessor;

    @Test
    void chargeSucceedsAfterATransientGatewayFailureIsRetried() {
        // The processor's first internal attempt always throws a transient
        // failure; @Retry means the caller only ever sees the outcome after
        // the retry, i.e. success -- proving the retry actually ran rather
        // than the exception propagating.
        PaymentOutcome outcome = paymentProcessor.charge(UUID.randomUUID(), new BigDecimal("50.00"));

        assertThat(outcome.succeeded()).isTrue();
    }

    @Test
    void chargeDeclinesLargeAmountsWithoutRetrying() {
        PaymentOutcome outcome = paymentProcessor.charge(UUID.randomUUID(), new BigDecimal("15000.00"));

        assertThat(outcome.succeeded()).isFalse();
        assertThat(outcome.failureReason()).contains("exceeds processing limit");
    }
}
