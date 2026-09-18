package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.IngestionRequestDto;
import com.amex.lumi.ingestion.dto.IngestionResponseDto;
import com.amex.lumi.ingestion.service.AirflowTriggerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.io.IOException;

@RestController
@RequestMapping("/api/ingestions")
public class IngestionController {

    private final AirflowTriggerService airflowTriggerService;

    public IngestionController(AirflowTriggerService airflowTriggerService) {
        this.airflowTriggerService = airflowTriggerService;
    }

    @PostMapping
    public ResponseEntity<IngestionResponseDto> trigger(@Valid @RequestBody IngestionRequestDto request) throws IOException {
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(airflowTriggerService.trigger(request));
    }
}
