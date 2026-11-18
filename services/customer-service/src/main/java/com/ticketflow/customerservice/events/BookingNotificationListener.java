package com.ticketflow.customerservice.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes booking events from Kafka asynchronously to handle customer notifications.
 */
@Component
public class BookingNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(BookingNotificationListener.class);

    @KafkaListener(
            topics = "${ticketflow.kafka.topics.booking-confirmed:ticketflow.booking.confirmed}",
            groupId = "${spring.kafka.consumer.group-id:customer-service}")
    public void onBookingConfirmed(
            @Payload String message,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(name = KafkaHeaders.RECEIVED_PARTITION, required = false) Integer partition,
            @Header(name = KafkaHeaders.OFFSET, required = false) Long offset) {

        log.info("📢 [NOTIFICATION / CUSTOMER-SERVICE] Reçu événement de réservation -> key: {}, partition: {}, offset: {}",
                key, partition, offset);
        log.info("📩 Notification traitée avec succès pour le message : {}", message);
    }
}
