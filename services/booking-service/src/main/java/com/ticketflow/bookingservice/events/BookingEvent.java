package com.ticketflow.bookingservice.events;

import java.math.BigDecimal;
import java.time.Instant;

public record BookingEvent(
        Long bookingId,
        Long eventId,
        String eventTitle,
        Long customerId,
        int quantity,
        BigDecimal totalPrice,
        String status,
        Instant timestamp
) {}
