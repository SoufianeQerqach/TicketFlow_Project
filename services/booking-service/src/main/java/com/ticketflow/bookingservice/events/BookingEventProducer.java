package com.ticketflow.bookingservice.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketflow.bookingservice.entities.Booking;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BookingEventProducer {

    private static final Logger log = LoggerFactory.getLogger(BookingEventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public BookingEventProducer(
            @Autowired(required = false) KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${ticketflow.kafka.topics.booking-confirmed:ticketflow.booking.confirmed}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    public void sendBookingConfirmed(Booking booking) {
        BookingEvent event = new BookingEvent(
                booking.getId(),
                booking.getEventId(),
                booking.getEventTitle(),
                booking.getCustomerId(),
                booking.getQuantity(),
                booking.getTotalPrice(),
                booking.getStatus().name(),
                Instant.now()
        );

        String key = "booking-" + booking.getId();

        try {
            String payload = objectMapper.writeValueAsString(event);
            if (kafkaTemplate != null) {
                kafkaTemplate.send(topic, key, payload)
                        .whenComplete((result, ex) -> {
                            if (ex == null) {
                                log.info("Published booking event [topic={}, key={}, partition={}, offset={}]",
                                        topic, key,
                                        result.getRecordMetadata().partition(),
                                        result.getRecordMetadata().offset());
                            } else {
                                log.error("Failed to publish booking event for {}", key, ex);
                            }
                        });
            } else {
                log.info("KafkaTemplate unavailable, skipped sending booking event for {}", key);
            }
        } catch (JsonProcessingException e) {
            log.error("Could not serialize BookingEvent for booking {}", booking.getId(), e);
        }
    }
}
