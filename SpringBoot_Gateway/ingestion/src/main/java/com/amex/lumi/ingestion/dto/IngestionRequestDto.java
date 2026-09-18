package com.amex.lumi.ingestion.dto;

import jakarta.validation.constraints.NotBlank;

public record IngestionRequestDto(
        @NotBlank(message = "inputFile is required") String inputFile,
        String errorLogPath,
        String controlFile
) {
}
