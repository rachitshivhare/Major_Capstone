package com.amex.lumi.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public final class ControlFileUtils {
    private ControlFileUtils() {
    }

    public static Long readExpectedRecordCount(String controlFile) {
        if (controlFile == null || controlFile.isBlank()) {
            return null;
        }
        Properties properties = new Properties();
        try (InputStream input = Files.newInputStream(Path.of(controlFile))) {
            properties.load(input);
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to read control file: " + controlFile, exception);
        }
        String configuredCount = properties.getProperty("record.count");
        if (configuredCount == null || configuredCount.isBlank()) {
            throw new IllegalArgumentException("Control file must define record.count: " + controlFile);
        }
        try {
            long count = Long.parseLong(configuredCount.trim());
            if (count < 0) {
                throw new NumberFormatException("negative count");
            }
            return count;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("record.count must be a non-negative integer: " + configuredCount, exception);
        }
    }
}