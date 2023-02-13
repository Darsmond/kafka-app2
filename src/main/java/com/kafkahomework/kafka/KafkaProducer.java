package com.kafkahomework.kafka;

import com.kafkahomework.entity.VehicleDistance;
import com.kafkahomework.entity.VehicleSignal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private final KafkaTemplate<String, VehicleSignal> signalKafkaTemplate;
    private final KafkaTemplate<String, VehicleDistance> distanceKafkaTemplate;

    @Value("${topics.input}")
    private String inputTopic;

    @Value("${topics.output}")
    private String outputTopic;

    public KafkaProducer(KafkaTemplate<String, VehicleSignal> signalKafkaTemplate,
                         KafkaTemplate<String, VehicleDistance> distanceKafkaTemplate){
        this.signalKafkaTemplate = signalKafkaTemplate;
        this.distanceKafkaTemplate = distanceKafkaTemplate;
    }

    public void send(VehicleSignal signal) {
        log.info("Signal " + signal.getVehicleId() + " was sent to topic " + inputTopic);
        signalKafkaTemplate.send(inputTopic, signal.getVehicleId(), signal);
    }

    public void send(VehicleDistance signal) {
        log.info("Signal " + signal.getVehicleId() + " was sent to topic " + outputTopic);
        distanceKafkaTemplate.send(outputTopic, signal.getVehicleId(), signal);
    }

}
