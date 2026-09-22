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

    public boolean triggerSparkSplitJob(String scriptPath, String inputPath, String outputPath, String fileFormat, int maxRecords) {
        List<String> command = new ArrayList<>();

        command.add("py");

        if ("xml".equalsIgnoreCase(fileFormat)) {
            command.add("--packages");
            command.add("com.databricks:spark-xml_2.12:0.17.0"); // Adjust version based on your Spark setup
        }

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

        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();

        try {
            logger.info("Executing PySpark job with command: {}", String.join(" ", command));
            Process process = processBuilder.start();

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