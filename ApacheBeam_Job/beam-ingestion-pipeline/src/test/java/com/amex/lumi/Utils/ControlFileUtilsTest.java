package com.amex.lumi.Utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlFileUtilsTest {

    @Test
    void readExpectedRecordCountReadsPositiveValue() throws IOException {
        Path tempFile = Files.createTempFile("control-file", ".properties");
        Files.writeString(tempFile, "record.count=42\n");

        assertEquals(42L, ControlFileUtils.readExpectedRecordCount(tempFile.toString()));
    }

    @Test
    void readExpectedRecordCountRejectsMissingRecordCount() throws IOException {
        Path tempFile = Files.createTempFile("control-file-missing", ".properties");
        Files.writeString(tempFile, "other=value\n");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ControlFileUtils.readExpectedRecordCount(tempFile.toString()));

        assertTrue(exception.getMessage().contains("record.count"));
    }

    @Test
    void readExpectedRecordCountRejectsNegativeValues() throws IOException {
        Path tempFile = Files.createTempFile("control-file-negative", ".properties");
        Files.writeString(tempFile, "record.count=-5\n");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> ControlFileUtils.readExpectedRecordCount(tempFile.toString()));

        assertTrue(exception.getMessage().contains("non-negative"));
    }
}
