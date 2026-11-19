package com.sangiya.payments.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.sangiya.common.events.OrderCreatedEvent;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"order.created", "payment.succeeded", "payment.failed"})
// AFTER_EACH_TEST_METHOD, not the class-level default: each test method
// gets its own embedded broker. Without it, test methods share one broker
// instance, and a fresh consumer group's default earliest-offset reset can
// pick up a message left on the topic by an *earlier* test method instead
// of the one this test just produced -- exactly what happened before this
// was added (a real, reproduced flake, not a hypothetical one).
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class OrderEventListenerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private PaymentRepository paymentRepository;

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    private KafkaTemplate<String, String> orderEventsProducer() {
        Map<String, Object> props = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(pf);
    }

    private org.apache.kafka.clients.consumer.Consumer<String, String> paymentEventsConsumer(String topic) {
        Map<String, Object> props =
                KafkaTestUtils.consumerProps("test-consumer-" + UUID.randomUUID(), "true", embeddedKafkaBroker);
        props.put(
                org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringDeserializer.class);
        var consumer = new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer, topic);
        return consumer;
    }

    @Test
    void aNormalOrderResultsInAPaymentSucceededEventAndAStoredPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event =
                new OrderCreatedEvent(eventId, orderId, "tenant-a", new BigDecimal("75.00"), "USD", Instant.now());

        var consumer = paymentEventsConsumer("payment.succeeded");
        orderEventsProducer().send("order.created", eventId.toString(), objectMapper.writeValueAsString(event)).get();

        var record = org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord(
                consumer, "payment.succeeded", Duration.ofSeconds(10));
        assertThat(record.value()).contains(orderId.toString());

        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(paymentRepository.findByOrderId(orderId))
                        .isPresent()
                        .get()
                        .satisfies(payment -> assertThat(payment.getStatus()).isEqualTo(PaymentStatus.SUCCEEDED)));
    }

    @Test
    void aLargeOrderResultsInAPaymentFailedEvent() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event =
                new OrderCreatedEvent(eventId, orderId, "tenant-a", new BigDecimal("20000.00"), "USD", Instant.now());

        var consumer = paymentEventsConsumer("payment.failed");
        orderEventsProducer().send("order.created", eventId.toString(), objectMapper.writeValueAsString(event)).get();

        var record = org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord(
                consumer, "payment.failed", Duration.ofSeconds(10));
        assertThat(record.value()).contains("exceeds processing limit");
    }

    @Test
    void redeliveringTheSameOrderCreatedEventDoesNotCreateASecondPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        OrderCreatedEvent event =
                new OrderCreatedEvent(eventId, orderId, "tenant-a", new BigDecimal("30.00"), "USD", Instant.now());
        String payload = objectMapper.writeValueAsString(event);

        KafkaTemplate<String, String> producer = orderEventsProducer();
        producer.send("order.created", eventId.toString(), payload).get();
        await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(paymentRepository.findByOrderId(orderId)).isPresent());

        producer.send("order.created", eventId.toString(), payload).get();
        await().pollDelay(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            long count = paymentRepository.findAll().stream()
                    .filter(payment -> payment.getOrderId().equals(orderId))
                    .count();
            assertThat(count).isEqualTo(1);
        });
    }
}
