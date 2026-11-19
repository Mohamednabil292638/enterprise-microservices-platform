package com.sangiya.orders.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sangiya.common.events.PaymentFailedEvent;
import com.sangiya.common.events.PaymentSucceededEvent;
import com.sangiya.orders.outbox.ProcessedEvent;
import com.sangiya.orders.outbox.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/** The saga's completion side: a payment outcome either confirms or cancels the order that triggered it. */
@Component
public class PaymentEventListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventListener.class);

    private final OrderService orderService;
    private final ProcessedEventRepository processedEventRepository;
    private final ObjectMapper objectMapper;

    public PaymentEventListener(
            OrderService orderService, ProcessedEventRepository processedEventRepository, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.processedEventRepository = processedEventRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "payment.succeeded", groupId = "order-service")
    @Transactional
    public void onPaymentSucceeded(String payload) throws Exception {
        PaymentSucceededEvent event = objectMapper.readValue(payload, PaymentSucceededEvent.class);
        if (alreadyProcessed(event.eventId())) {
            return;
        }
        orderService.confirmOrder(event.orderId());
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
        log.info("Order {} confirmed by payment {}", event.orderId(), event.paymentId());
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-service")
    @Transactional
    public void onPaymentFailed(String payload) throws Exception {
        PaymentFailedEvent event = objectMapper.readValue(payload, PaymentFailedEvent.class);
        if (alreadyProcessed(event.eventId())) {
            return;
        }
        orderService.cancelOrder(event.orderId());
        processedEventRepository.save(new ProcessedEvent(event.eventId()));
        log.info("Order {} cancelled: {}", event.orderId(), event.reason());
    }

    private boolean alreadyProcessed(java.util.UUID eventId) {
        boolean seen = processedEventRepository.existsById(eventId);
        if (seen) {
            log.info("Ignoring already-processed event {}", eventId);
        }
        return seen;
    }
}
