package com.omarassed.customer.event.elt.controller;

import com.omarassed.customer.event.elt.model.CustomerEvent;
import com.omarassed.customer.event.elt.service.S3Service;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/events")
public class EventController {
    private final S3Service s3Service;
    public EventController(S3Service s3Service) { this.s3Service = s3Service; }

    @PostMapping("/simulate")
    public String simulate(@RequestBody CustomerEvent event) throws Exception {
        s3Service.uploadEvent(event);
        return "Event pushed to S3 Data Lake!";
    }
}
