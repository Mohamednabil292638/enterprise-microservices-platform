package com.sangiya.payments.payment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sangiya.common.events.OrderCreatedEvent;
import com.sangiya.common.events.PaymentFailedEvent;
import com.sangiya.common.events.PaymentSucceededEvent;
import com.sangiya.payments.outbox.OutboxEvent;
import com.sangiya.payments.outbox.OutboxEventRepository;
import com.sangiya.payments.outbox.ProcessedEvent;
import com.sangiya.payments.outbox.ProcessedEventRepository;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final PaymentProcessor paymentProcessor;
    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public OrderEventListener(
            PaymentProcessor paymentProcessor,
            PaymentRepository paymentRepository,
            OutboxEventRepository outboxEventRepository,
            ProcessedEventRepository processedEventRepository,
            ObjectMapper objectMapper) {
        this.paymentProcessor = paymentProcessor;
        this.paymentRepository = paymentRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "order.created", groupId = "payment-service")
    @Transactional
    public void onOrderCreated(String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Ignoring already-processed OrderCreated event {}", event.eventId());
            return;
        }

        PaymentOutcome outcome;
        try {
            outcome = paymentProcessor.charge(event.orderId(), event.amount());
        } catch (Exception ex) {
            // The gateway is unavailable even after retries/circuit-breaker
            // handling -- treat it the same as a declined payment so the
            // saga always reaches a terminal state rather than hanging.
            outcome = PaymentOutcome.declined("payment gateway unavailable: " + ex.getMessage());
        }

        UUID paymentId = UUID.randomUUID();
        Payment payment = new Payment(
                paymentId, event.orderId(), event.amount(), event.currency(),
                outcome.succeeded() ? PaymentStatus.SUCCEEDED : PaymentStatus.FAILED, outcome.failureReason());
        paymentRepository.save(payment);

        saveOutcomeEvent(event.orderId(), paymentId, outcome);
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
    }

    private void saveOutcomeEvent(UUID orderId, UUID paymentId, PaymentOutcome outcome) {
        UUID eventId = UUID.randomUUID();
        if (outcome.succeeded()) {
            PaymentSucceededEvent event = new PaymentSucceededEvent(eventId, orderId, paymentId, Instant.now());
            outboxEventRepository.save(
                    new OutboxEvent(eventId, "PaymentSucceeded", "payment.succeeded", writeJson(event)));
        } else {
            PaymentFailedEvent event = new PaymentFailedEvent(eventId, orderId, outcome.failureReason(), Instant.now());
            outboxEventRepository.save(
                    new OutboxEvent(eventId, "PaymentFailed", "payment.failed", writeJson(event)));
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize event", ex);
        }
    }
}
