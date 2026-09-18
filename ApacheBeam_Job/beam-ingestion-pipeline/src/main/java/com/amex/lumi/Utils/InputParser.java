package com.amex.lumi.Utils;

import com.amex.lumi.Ingestion;
import com.amex.lumi.Praser.CsvParseFn;
import com.amex.lumi.Praser.FixedWidthParseFn;
import com.amex.lumi.Praser.JsonParseFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.UUID;

public final class InputParser {
    private InputParser() {
    }

    public static PCollectionTuple parse(PCollection<String> rawInput, String format, String executionId) {
        UUID runId = UUID.fromString(executionId);
        if ("json".equals(format)) {
            return rawInput.apply("ParseJson", ParDo.of(new JsonParseFn(runId))
                    .withOutputTags(Ingestion.VALID_RECORDS, TupleTagList.of(Ingestion.ERROR_RECORDS)));
        }
        if ("fixed".equals(format)) {
            return rawInput.apply("ParseFixedWidth", ParDo.of(new FixedWidthParseFn(runId))
                    .withOutputTags(Ingestion.VALID_RECORDS, TupleTagList.of(Ingestion.ERROR_RECORDS)));
        }
        return rawInput.apply("ParseCsv", ParDo.of(new CsvParseFn(runId))
                .withOutputTags(Ingestion.VALID_RECORDS, TupleTagList.of(Ingestion.ERROR_RECORDS)));
    }
}