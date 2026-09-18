package com.amex.lumi.ingestion.dto;

public record IngestionResponseDto(
        String executionId,
        String dagId,
        String dagRunId,
        String status
) {
}
