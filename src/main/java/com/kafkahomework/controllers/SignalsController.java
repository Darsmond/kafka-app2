package com.kafkahomework.controllers;

import com.kafkahomework.services.VehicleService;
import com.kafkahomework.entity.VehicleSignal;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SignalsController {

    private final VehicleService vehicleService;

    public SignalsController(VehicleService vehicleService) {
        this.vehicleService = vehicleService;
    }

    @PostMapping("/vehicle-signal")
    public void acceptVehicleSignal(@RequestBody VehicleSignal vehicleSignal) {
        vehicleService.validate(vehicleSignal);
    }
}
