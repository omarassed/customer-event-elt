package com.omarassed.customer.event.elt.model;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerEvent {
    @NotBlank
    private String eventId;
    @NotBlank
    private String customerId;
    @NotBlank
    private String interactionType;
    @Positive
    private double estimatedValue;
    private Instant eventTimestamp = Instant.now(); // ISO-8601 ready
}