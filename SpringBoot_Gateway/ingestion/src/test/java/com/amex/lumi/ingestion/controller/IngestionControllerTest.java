package com.amex.lumi.ingestion.controller;

import com.amex.lumi.ingestion.dto.IngestionRequestDto;
import com.amex.lumi.ingestion.dto.IngestionResponseDto;
import com.amex.lumi.ingestion.service.AirflowTriggerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IngestionControllerTest {

    @Mock
    private AirflowTriggerService airflowTriggerService;

    private IngestionController ingestionController;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        ingestionController = new IngestionController(airflowTriggerService);
    }

    @Test
    void triggerReturnsAcceptedResponseWhenServiceSucceeds() throws IOException {
        IngestionRequestDto request = new IngestionRequestDto("/tmp/input.csv", "/tmp/errors", "/tmp/control.txt");
        IngestionResponseDto expectedResponse = new IngestionResponseDto(
                "exec-123",
                "pipeline-trigger",
                "ingestion-exec-123",
                "TRIGGERED"
        );

        when(airflowTriggerService.trigger(request)).thenReturn(expectedResponse);

        ResponseEntity<IngestionResponseDto> response = ingestionController.trigger(request);

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertEquals(expectedResponse, response.getBody());
        verify(airflowTriggerService).trigger(request);
    }
}

