package com.amex.lumi.ingestion.sparkOrchestrator.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class SparkJobService {

    private static final Logger logger = LoggerFactory.getLogger(SparkJobService.class);

    /**
     * Invokes the PySpark script to split a large file.
     *
     * @param scriptPath Path to the PySpark Python script.
     * @param inputPath Path to the large source file.
     * @param outputPath Directory to save the split files.
     * @param fileFormat Format of the file (e.g., csv, json, xml, fixedwidth).
     * @param maxRecords Maximum records per split file.
     * @return boolean True if the process finished successfully, false otherwise.
     */
    public boolean triggerSparkSplitJob(String scriptPath, String inputPath, String outputPath, String fileFormat, int maxRecords) {
        List<String> command = new ArrayList<>();

        // Base spark-submit command
        command.add("py");

        // Add external packages if handling XML
        if ("xml".equalsIgnoreCase(fileFormat)) {
            command.add("--packages");
            command.add("com.databricks:spark-xml_2.12:0.17.0"); // Adjust version based on your Spark setup
        }

        // Script and its arguments
        command.add(scriptPath);
        command.add("--input_path");
        command.add(inputPath);
        command.add("--output_path");
        command.add(outputPath);
        command.add("--file_format");
        command.add(fileFormat);
        command.add("--max_records");
        command.add(String.valueOf(maxRecords));

        ProcessBuilder processBuilder = new ProcessBuilder(command);

        // Redirect error and standard output to the Spring Boot console
        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();

        try {
            logger.info("Executing PySpark job with command: {}", String.join(" ", command));
            Process process = processBuilder.start();

            // Wait for the Spark job to finish
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                logger.info("PySpark job completed successfully for file: {}", inputPath);
                return true;
            } else {
                logger.error("PySpark job failed with exit code: {}", exitCode);
                return false;
            }

        } catch (IOException | InterruptedException e) {
            logger.error("Exception occurred while running PySpark job", e);
            Thread.currentThread().interrupt();
            return false;
        }
    }
}