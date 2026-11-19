package com.sangiya.orders.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sangiya.common.events.OrderCreatedEvent;
import com.sangiya.orders.outbox.OutboxEvent;
import com.sangiya.orders.outbox.OutboxEventRepository;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private static final String ORDER_CREATED_TOPIC = "order.created";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(
            OrderRepository orderRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * The outbox guarantee lives entirely in this method being one
     * transaction: either both the order row and its outbox event commit, or
     * neither does. OutboxPublisher is the only thing that later reads this
     * row and actually talks to Kafka.
     */
    @Transactional
    public OrderResponse createOrder(String tenantId, CreateOrderRequest request) {
        Order order = new Order(UUID.randomUUID(), tenantId, request.amount(), request.currency());
        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId, order.getId(), tenantId, order.getAmount(), order.getCurrency(), Instant.now());
        outboxEventRepository.save(
                new OutboxEvent(eventId, "OrderCreated", ORDER_CREATED_TOPIC, writeJson(event)));

        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String tenantId, UUID id) {
        Order order = orderRepository.findByIdAndTenantId(id, tenantId).orElseThrow(() -> new OrderNotFoundException(id));
        return OrderResponse.from(order);
    }

    @Transactional
    public void confirmOrder(UUID id) {
        orderRepository.findById(id).ifPresent(Order::confirm);
    }

    @Transactional
    public void cancelOrder(UUID id) {
        orderRepository.findById(id).ifPresent(Order::cancel);
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to serialize event", ex);
        }
    }
}
