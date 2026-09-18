package com.amex.lumi.ingestion.service;

import com.amex.lumi.ingestion.dto.IngestionRequestDto;
import com.amex.lumi.ingestion.dto.IngestionResponseDto;
import com.amex.lumi.ingestion.sparkOrchestrator.service.SparkJobService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import tools.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Service
public class AirflowTriggerService {

    private final RestTemplate restTemplate;
    private final String airflowApiUrl;
    private final String airflowUsername;
    private final String airflowPassword;
    private final String dagId;
    private final String defaultErrorLogPath;
    private final SparkJobService sparkJobService;

    private static final Logger logger = LoggerFactory.getLogger(AirflowTriggerService.class);

    @Value("${ingestion.file.size.threshold}")
    private long fileSizeThreshold;

    @Value("${pyspark.script.path}")
    private String pysparkScriptPath;

    public AirflowTriggerService(
            RestTemplate restTemplate,
            @Value("${airflow.api-url}") String airflowApiUrl,
            @Value("${airflow.username}") String airflowUsername,
            @Value("${airflow.password}") String airflowPassword,
            @Value("${airflow.dag-id:pipeline-trigger}") String dagId,
            @Value("${airflow.default-error-log-path:/opt/airflow/errors}") String defaultErrorLogPath, SparkJobService sparkJobService) {
        this.restTemplate = restTemplate;
        this.airflowApiUrl = airflowApiUrl;
        this.airflowUsername = airflowUsername;
        this.airflowPassword = airflowPassword;
        this.dagId = dagId;
        this.defaultErrorLogPath = defaultErrorLogPath;
        this.sparkJobService = sparkJobService;
    }

    public IngestionResponseDto trigger(IngestionRequestDto request) throws IOException {
        String executionId = UUID.randomUUID().toString();
        System.out.println("Execution ID:" + executionId);
        String dagRunId = "ingestion-" + executionId;
        System.out.println("DAG run ID:" + dagRunId);
        String errorLogPath = request.errorLogPath() == null || request.errorLogPath().isBlank()
                ? defaultErrorLogPath
                : request.errorLogPath();

        File file = new File(request.inputFile());
        if (!file.exists()) {
            throw new FileNotFoundException("Input file not found at: " + request.inputFile());
        }

        String airflowPath = request.inputFile();
        airflowPath = airflowPath.replaceAll("^.*?airflow_505", "/opt/airflow").trim();
        System.out.println(airflowPath);
        String ingestionPath = airflowPath;
        long fileSizeInBytes = file.length();
        System.out.println("File size : " + fileSizeInBytes + " bytes.");
        String fileFormat = detectFileFormat(file);
        Path controlFilePath = resolveControlFile(file, request.controlFile());
        long recordCount = countRecords(file, fileFormat);
        writeControlFile(controlFilePath, recordCount);
        logger.info("Generated control file with {} records: {}", recordCount, controlFilePath);
        System.out.println("File size threshold: " + fileSizeThreshold + " bytes.");
        System.out.println("File Format:" + fileFormat);
        if (file.isFile() && fileSizeInBytes > fileSizeThreshold) {
            logger.info("File exceeds threshold. Triggering PySpark split job.");
            int recordsPerSplit = calculateRecordsPerSplit(file, fileSizeInBytes);
            logger.info("Calculated records per split: {}", recordsPerSplit);

            // Generate an output directory specific to this execution
            String outputDirectory = file.getParent() + "\\split_" + executionId;

            boolean isSplitSuccessful = sparkJobService.triggerSparkSplitJob(
                    pysparkScriptPath,
                    request.inputFile(),
                    outputDirectory,
                    fileFormat,
                    recordsPerSplit
            );
            if (!isSplitSuccessful) {
                throw new IllegalStateException("Unable to split input file: " + request.inputFile());
            }
            outputDirectory = outputDirectory.replace("\\", "/");
            ingestionPath = outputDirectory.replaceAll("^.*?airflow_505", "/opt/airflow").trim();
        }
        else if (request.inputFile().endsWith(".json")){
            try {
                Path path = Path.of(request.inputFile());
                String multilineJson = Files.readString(path);

                ObjectMapper mapper = new ObjectMapper();
                Object jsonObject = mapper.readValue(multilineJson, Object.class);
                String singleLineJson = mapper.writeValueAsString(jsonObject);

                // Constructing new path with _flatten suffix before the extension
                String fileName = path.getFileName().toString();
                String newFileName;
                int dotIndex = fileName.lastIndexOf('.');
                if (dotIndex == -1) {
                    newFileName = fileName + "_flatten";
                } else {
                    newFileName = fileName.substring(0, dotIndex) + "_flatten" + fileName.substring(dotIndex);
                }
                Path newPath = path.resolveSibling(newFileName);
                ingestionPath = newPath.toString().replaceAll("^.*?airflow_505", "/opt/airflow").trim();
                ingestionPath = ingestionPath.replace("\\", "/");
                System.out.println("New Input Path: " + ingestionPath);
                Files.writeString(newPath, singleLineJson);
                System.out.println("File successfully minimized to a single line!");
            } catch (Exception e) {
                System.err.println("An error occurred during processing: " + e.getMessage());
            }
        }

        errorLogPath = errorLogPath.replace("\\", "/");
        errorLogPath = errorLogPath.replaceAll("^.*?airflow_505", "/opt/airflow").trim();

        String controlFile = controlFilePath.toString().replace("\\", "/");
        controlFile = controlFile.replaceAll("^.*?airflow_505", "/opt/airflow").trim();

        Map<String, Object> payload = Map.of(
                "dag_run_id", dagRunId,
                "conf", Map.of(
                        "inputPath", ingestionPath,
                        "fileExtension", fileFormat,
                        "executionId", executionId,
                        "errorLogPath", errorLogPath,
                        "controlFile", controlFile
                )
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setBasicAuth(airflowUsername, airflowPassword);
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<Map> response = restTemplate.postForEntity(
                airflowApiUrl + "/api/v1/dags/" + dagId + "/dagRuns",
                new HttpEntity<>(payload, headers),
                Map.class);

        String acceptedDagRunId = response.getBody() != null && response.getBody().get("dag_run_id") != null
                ? response.getBody().get("dag_run_id").toString()
                : dagRunId;
        return new IngestionResponseDto(executionId, dagId, acceptedDagRunId, "TRIGGERED");
    }

    private Path resolveControlFile(File input, String configuredPath) {
        if (configuredPath != null && !configuredPath.isBlank()) {
            return Path.of(configuredPath);
        }
        File parent = input.isDirectory() ? input : input.getParentFile();
        return parent.toPath().resolve("controlFile.txt");
    }

    private long countRecords(File input, String fileFormat) throws IOException {
        if (input.isDirectory()) {
            try (var files = Files.walk(input.toPath())) {
                return files.filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT)
                                .endsWith("." + fileFormat))
                        .mapToLong(path -> countFileRecords(path, fileFormat))
                        .sum();
            }
        }
        return countFileRecords(input.toPath(), fileFormat);
    }

    private long countFileRecords(Path path, String fileFormat) {
        try {
            if ("json".equals(fileFormat)) {
                Object json = new ObjectMapper().readValue(Files.readString(path), Object.class);
                return json instanceof Collection<?> collection ? collection.size() : 1;
            }
            try (var lines = Files.lines(path)) {
                long lineCount = lines.count();
                return "csv".equals(fileFormat) && lineCount > 0 ? lineCount - 1 : lineCount;
            }
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to count records in input: " + path, exception);
        }
    }

    private void writeControlFile(Path path, long recordCount) throws IOException {
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }
        Files.writeString(path, "record.count=" + recordCount + System.lineSeparator()
                + "recordCount=" + recordCount + System.lineSeparator());
    }

    private int calculateRecordsPerSplit(File file, long fileSizeInBytes) {
        try (var lines = Files.lines(file.toPath())) {
            long totalLines = lines.count();
            if (totalLines == 0 || file.length() == 0) {
                return 1;
            }
            double thresholdRatio = (double) fileSizeThreshold / fileSizeInBytes ;
            long calculated = (long) Math.ceil(totalLines * thresholdRatio);
            return (int) Math.max(1, Math.min(2000, calculated));
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to count lines in input file: " + file, exception);
        }
    }

    private String detectFileFormat(File input) throws FileNotFoundException {
        File source = input;
        if (input.isDirectory()) {
            File[] files = input.listFiles(File::isFile);
            if (files == null || files.length == 0) {
                throw new FileNotFoundException("No files found in input directory: " + input);
            }
            source = files[0];
        }
        String name = source.getName().toLowerCase(Locale.ROOT);
        if (name.endsWith(".csv")) return "csv";
        if (name.endsWith(".json")) return "json";
        if (name.endsWith(".txt") || name.endsWith(".dat")) return "txt";
        throw new IllegalArgumentException("Unsupported input file extension: " + source.getName());
    }
}