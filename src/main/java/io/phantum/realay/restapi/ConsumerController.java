package io.phantum.realay.restapi;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api")
public class ConsumerController {

    @GetMapping(value="/consumers")
    public String getConsumers() {
        return "hello world";
    }
}
