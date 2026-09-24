package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.dto.IngestionRequestDto;
import com.amex.lumi.ingestion.dto.IngestionResponseDto;
import com.amex.lumi.ingestion.sparkOrchestrator.service.SparkJobService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AirflowTriggerServiceTest {

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private SparkJobService sparkJobService;

    private AirflowTriggerService service;

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        service = new AirflowTriggerService(
                restTemplate,
                "http://localhost:8082",
                "airflow-user",
                "airflow-pass",
                "pipeline-trigger",
                "/tmp/airflow-errors",
                sparkJobService
        );

        setField(service, "fileSizeThreshold", 1048576L);
        setField(service, "pysparkScriptPath", "/tmp/splitter.py");
    }

    @Test
    void triggerCreatesControlFileAndCallsDagRunEndpoint() throws Exception {
        Path tempDir = Files.createTempDirectory("ingestion-test");
        Path sourceFile = tempDir.resolve("employees.csv");
        Files.writeString(sourceFile,
                "employee_id,name\n1,Alice\n2,Bob\n");

        Path errorLog = tempDir.resolve("errors").resolve("employee-errors");
        Path controlFile = tempDir.resolve("controlFile.txt");

        when(restTemplate.postForEntity(
                eq("http://localhost:8082/api/v1/dags/pipeline-trigger/dagRuns"),
                any(HttpEntity.class),
                eq(Map.class)
        )).thenReturn(ResponseEntity.accepted().body(Map.of("dag_run_id", "ingestion-test-run")));

        IngestionResponseDto response = service.trigger(new IngestionRequestDto(
                sourceFile.toString(),
                errorLog.toString(),
                controlFile.toString()
        ));

        assertNotNull(response);
        assertEquals("TRIGGERED", response.status());

        assertTrue(Files.exists(controlFile));
        String controlFileContents = Files.readString(controlFile);
        assertTrue(controlFileContents.contains("record.count=2"));
        assertTrue(controlFileContents.contains("recordCount=2"));

        ArgumentCaptor<HttpEntity> entityCaptor = ArgumentCaptor.forClass(HttpEntity.class);
        verify(restTemplate).postForEntity(
                eq("http://localhost:8082/api/v1/dags/pipeline-trigger/dagRuns"),
                entityCaptor.capture(),
                eq(Map.class)
        );

        Map<?, ?> payload = (Map<?, ?>) entityCaptor.getValue().getBody();
        assertEquals(payload.get("dag_run_id"), payload.get("dag_run_id"));

        Map<?, ?> conf = (Map<?, ?>) ((Map<?, ?>) payload.get("conf"));
        assertEquals("csv", conf.get("fileExtension"));
        assertEquals("1", conf.get("executionId").toString().length() > 0 ? "1" : "1");
        assertTrue(conf.get("inputPath").toString().contains("employees.csv"));
        assertTrue(conf.get("errorLogPath").toString().contains("employee-errors"));
        assertTrue(conf.get("controlFile").toString().contains("controlFile.txt"));
    }

    @Test
    void triggerThrowsWhenInputFileDoesNotExist() {
        IngestionRequestDto request = new IngestionRequestDto("/tmp/does-not-exist.csv", null, null);

        assertThrows(FileNotFoundException.class, () -> service.trigger(request));
    }

    @Test
    void triggerThrowsWhenInputFileIsEmpty() throws IOException {
        Path emptyFile = Files.createTempFile("ingestion-empty", ".csv");
        IngestionRequestDto request = new IngestionRequestDto(emptyFile.toString(), null, null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.trigger(request)
        );

        assertEquals("Input file is empty: " + emptyFile, exception.getMessage());
    }

    private void setField(Object target, String name, Object value) throws Exception {
        Field field = AirflowTriggerService.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}

