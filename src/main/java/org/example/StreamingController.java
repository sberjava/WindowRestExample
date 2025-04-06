package org.example;

import lombok.AllArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@AllArgsConstructor
@RequestMapping("/entities")
public class StreamingController {

    private final ServiceExample service;

    @GetMapping
    @RequestMapping("/jdbc")
    public Flux<Dto> getAllEntitiesJdbc() {
        return service.findAllJdbc();
    }

    @GetMapping
    @RequestMapping("/batis")
    public Flux<Dto> getAllEntitiesBatis() {
        return service.findAllBatis();
    }
}
