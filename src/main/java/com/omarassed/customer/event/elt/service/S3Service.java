package com.omarassed.customer.event.elt.service;

import com.omarassed.customer.event.elt.model.CustomerEvent;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import tools.jackson.databind.ObjectMapper;

@Service
public class S3Service {
    private final S3Client s3Client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public S3Service(S3Client s3Client) { this.s3Client = s3Client; }

    public void uploadEvent(CustomerEvent event) throws Exception {
        String json = objectMapper.writeValueAsString(event);
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket("customer-event-elt-raw-dev-omarassed")
                        .key("raw-interactions/" + event.getEventId() + ".json")
                        .build(),
                RequestBody.fromString(json));
    }
}