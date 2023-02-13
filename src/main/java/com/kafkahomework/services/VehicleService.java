package com.kafkahomework.services;

import com.kafkahomework.entity.VehicleDistance;
import com.kafkahomework.entity.VehicleSignal;
import com.kafkahomework.kafka.KafkaProducer;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.HashMap;

@Service
public class VehicleService {

    private static final HashMap<String, VehicleSignal> vehicleToCoordinates = new HashMap<>();

    private final KafkaProducer kafkaProducer;

    public VehicleService(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    public void validate(VehicleSignal signal) {
        if (signal == null) {
            throw new RuntimeException("Signal is incorrect");
        }

        if (!StringUtils.isNumeric(signal.getVehicleId())
                || Integer.parseInt(signal.getVehicleId()) < 0) {
            throw new RuntimeException("VehicleId is incorrect");
        }

        if (!(StringUtils.isNumeric(signal.getxCoordinate())
                && StringUtils.isNumeric(signal.getyCoordinate()))) {
            throw new RuntimeException("Coordinates are incorrect");
        }

        kafkaProducer.send(signal);
    }

    public void calculate(VehicleSignal signal) {
        VehicleSignal oldCoordinates = vehicleToCoordinates.get(signal.getVehicleId());

        if (oldCoordinates == null) {
            vehicleToCoordinates.put(signal.getVehicleId(), signal);
            return;
        }

        int oldXCoordinate = Integer.parseInt(oldCoordinates.getxCoordinate());
        int oldYCoordinate = Integer.parseInt(oldCoordinates.getyCoordinate());
        int newXCoordinate = Integer.parseInt(signal.getxCoordinate());
        int newYCoordinate = Integer.parseInt(signal.getyCoordinate());
        int xDifference = newXCoordinate - oldXCoordinate;
        int yDiff = newYCoordinate - oldYCoordinate;

        double difference = Math.sqrt(xDifference * xDifference
                + yDiff * yDiff);
        kafkaProducer.send(new VehicleDistance(signal.getVehicleId(), String.valueOf(difference)));
    }
}
