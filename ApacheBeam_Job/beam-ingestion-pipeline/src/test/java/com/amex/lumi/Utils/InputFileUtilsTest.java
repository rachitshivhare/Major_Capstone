package com.amex.lumi.Utils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InputFileUtilsTest {

    @Test
    void normaliseFormatAcceptsDotPrefixedAndLowercaseFormats() {
        assertEquals("json", InputFileUtils.normaliseFormat("JSON"));
        assertEquals("csv", InputFileUtils.normaliseFormat(".CSV"));
        assertEquals("txt", InputFileUtils.normaliseFormat("txt"));
        assertEquals("dat", InputFileUtils.normaliseFormat(".dat"));
    }

    @Test
    void normaliseFormatRejectsUnsupportedFormats() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> InputFileUtils.normaliseFormat("xml"));
        assertTrue(exception.getMessage().contains("Unsupported fileFormat"));
    }

    @Test
    void expandInputsReturnsMatchingFilesInDirectory() throws IOException {
        Path tempDir = Files.createTempDirectory("beam-inputs");
        Path jsonOne = tempDir.resolve("one.json");
        Path jsonTwo = tempDir.resolve("two.json");
        Path txtFile = tempDir.resolve("notes.txt");

        Files.writeString(jsonOne, "{}\n");
        Files.writeString(jsonTwo, "{}\n");
        Files.writeString(txtFile, "hello\n");

        List<String> files = InputFileUtils.expandInputs(tempDir.toString(), "json");

        assertEquals(2, files.size());
        assertTrue(files.stream().anyMatch(path -> path.endsWith("one.json")));
        assertTrue(files.stream().anyMatch(path -> path.endsWith("two.json")));
    }

    @Test
    void expandInputsThrowsWhenNoMatchingFilesExist() throws IOException {
        Path tempDir = Files.createTempDirectory("beam-empty-input");
        Files.writeString(tempDir.resolve("notes.txt"), "hello");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> InputFileUtils.expandInputs(tempDir.toString(), "json"));

        assertTrue(exception.getMessage().contains("No .json files found"));
    }
}
