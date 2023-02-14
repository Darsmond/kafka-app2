package com.kafkahomework.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkahomework.entity.VehicleDistance;
import com.kafkahomework.entity.VehicleSignal;
import com.kafkahomework.services.VehicleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private final VehicleService vehicleService;

    public KafkaConsumer(VehicleService vehicleService) {
        this.vehicleService = vehicleService;
    }

    @KafkaListener(topics = "${topics.input}", concurrency = "3")
    public void listen(List<VehicleSignal> recordBatch) {
        log.info("------------------------------------------");
        recordBatch.forEach(record -> {
            try {
                log.info("Consumer listened to: " + mapper.writeValueAsString(recordBatch));
                vehicleService.calculate(record);
            } catch (JsonProcessingException e) {
                log.error("Exception occurred during messages processing ", e);
            }
        });
        log.info("------------------------------------------");
    }

//    @KafkaListener(topics = "${topics.input}")
//    public void listen1(List<VehicleSignal> recordBatch) {
//        log.info("------------------------------------------");
//        recordBatch.forEach(record -> {
//            try {
//                log.info("First consumer listened to: " + mapper.writeValueAsString(recordBatch));
//                vehicleService.calculate(record);
//            } catch (JsonProcessingException e) {
//                log.error("Exception occurred during messages processing ", e);
//            }
//        });
//        log.info("------------------------------------------");
//    }
//
//    @KafkaListener(topics = "${topics.input}")
//    public void listen2(List<VehicleSignal> recordBatch) {
//        log.info("------------------------------------------");
//        recordBatch.forEach(record -> {
//            try {
//                log.info("Second consumer listened to: " + mapper.writeValueAsString(recordBatch));
//                vehicleService.calculate(record);
//            } catch (JsonProcessingException e) {
//                log.error("Exception occurred during messages processing ", e);
//            }
//        });
//        log.info("------------------------------------------");
//    }
//
//    @KafkaListener(topics = "${topics.input}")
//    public void listen3(List<VehicleSignal> recordBatch) {
//        log.info("------------------------------------------");
//        recordBatch.forEach(record -> {
//            try {
//                log.info("Third consumer listened to: " + mapper.writeValueAsString(recordBatch));
//                vehicleService.calculate(record);
//            } catch (JsonProcessingException e) {
//                log.error("Exception occurred during messages processing ", e);
//            }
//        });
//        log.info("------------------------------------------");
//    }

    @KafkaListener(topics = "${topics.output}")
    public void listen4(List<VehicleDistance> recordBatch) {
        log.info("------------------------------------------");
        recordBatch.forEach(record -> {
            try {
                log.info("Output consumer listened to: " + mapper.writeValueAsString(recordBatch));
            } catch (JsonProcessingException e) {
                log.error("Exception occurred during messages processing ", e);
            }
        });
        log.info("------------------------------------------");
    }
}
