package com.datalake.consumer;

import com.datalake.config.s3WriterService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class EventConsumer {

    private final s3WriterService s3WriterService;

    @KafkaListener(topics = "order-events", groupId = "datalake-consumer-group")
    public void consumeOrderEvents(String message){
        System.out.println("ORDER EVENT RECEIVED: "+ message);
        s3WriterService.write("order-events", message);
    }

    @KafkaListener(topics = "payment-events", groupId = "datalake-consumer-group")
    public void consumePaymentEvents(String message){
        System.out.println("PAYMENT EVENT RECEIVED: "+ message);
        s3WriterService.write("payment-events", message);
    }

    @KafkaListener(topics = "user-events", groupId = "datalake-consumer-group")
    public void consumeUserEvents(String message){
        System.out.println("USER EVENT RECEIVED: "+ message);
        s3WriterService.write("user-events", message);
    }
}
