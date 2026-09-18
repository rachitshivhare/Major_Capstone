package com.amex.lumi.Utils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public final class InputFileUtils {
    private InputFileUtils() {
    }

    public static String normaliseFormat(String fileFormat) {
        if (fileFormat == null || fileFormat.isBlank()) {
            throw new IllegalArgumentException("fileFormat must be one of: json, csv, txt, dat");
        }
        String format = fileFormat.trim().toLowerCase(Locale.ROOT);
        if (format.startsWith(".")) {
            format = format.substring(1);
        }
        if (!Set.of("json", "csv", "txt", "dat").contains(format)) {
            throw new IllegalArgumentException("Unsupported fileFormat: " + fileFormat);
        }
        return format;
    }

    public static List<String> expandInputs(String inputPath, String fileExtension) {
        List<String> inputs = new ArrayList<>();
        Path path = Path.of(inputPath.trim());
        try {
            if (Files.isDirectory(path)) {
                try (var paths = Files.walk(path)) {
                    paths.filter(Files::isRegularFile)
                            .filter(file -> hasExtension(file, fileExtension))
                            .sorted()
                            .forEach(file -> inputs.add(file.toString()));
                }
            } else if (Files.isRegularFile(path) && hasExtension(path, fileExtension)) {
                inputs.add(path.toString());
            }
        } catch (IOException exception) {
            throw new IllegalArgumentException("Unable to expand input path: " + inputPath, exception);
        }
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("No ." + fileExtension + " files found at: " + inputPath);
        }
        return inputs;
    }

    public static PCollection<String> read(Pipeline pipeline, List<String> inputFiles) {
        return readInputLines(pipeline, inputFiles);
    }

    private static PCollection<String> readInputLines(Pipeline pipeline, List<String> inputFiles) {
        PCollectionList<String> inputs = PCollectionList.empty(pipeline);
        int index = 0;
        for (String input : inputFiles) {
            inputs = inputs.and(pipeline.apply("ReadInput_" + index++, TextIO.read().from(input)));
        }
        return inputs.apply("FlattenInputs", Flatten.pCollections());
    }

    private static boolean hasExtension(Path path, String fileExtension) {
        return path.getFileName().toString().toLowerCase(Locale.ROOT)
                .endsWith("." + fileExtension);
    }
}