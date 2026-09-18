package com.amex.lumi;

import com.amex.lumi.Model.EmployeeRecord;
import com.amex.lumi.Transforms.CleanseNullFn;
import com.amex.lumi.Transforms.EncodeFn;
import com.amex.lumi.Transforms.RecordCountValidationFn;
import com.amex.lumi.Utils.ControlFileUtils;
import com.amex.lumi.Utils.DatabaseWriter;
import com.amex.lumi.Utils.InputFileUtils;
import com.amex.lumi.Utils.InputParser;
import com.amex.lumi.Validation.RecordValidationFn;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

import java.util.List;

public class Ingestion {


    public static final TupleTag<EmployeeRecord> VALID_RECORDS = new TupleTag<>() { };
    public static final TupleTag<String> ERROR_RECORDS = new TupleTag<>() { };

    public static void main(String[] args) {

        IngestionOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(IngestionOptions.class);
        options.setRunner(DirectRunner.class);

        Pipeline pipeline = Pipeline.create(options);

        String configuredFormat = options.getFileExtension();

        String fileExtension = InputFileUtils.normaliseFormat(configuredFormat);

        List<String> inputFiles = InputFileUtils.expandInputs(options.getInputPath(), fileExtension);

        PCollection<String> rawInput = InputFileUtils.read(pipeline, inputFiles);

        PCollectionTuple parsed = InputParser.parse(rawInput, fileExtension, options.getExecutionId());

        PCollectionTuple validated = parsed.get(VALID_RECORDS)
                .apply("ValidateEmployeeRecords", ParDo.of(new RecordValidationFn())
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));
        PCollection<EmployeeRecord> encodedRecords = validated.get(VALID_RECORDS)
            .apply("CleanseValidatedRecords", ParDo.of(new CleanseNullFn()))
                .apply("EncodeSensitiveFields", ParDo.of(new EncodeFn()));

        PCollection<EmployeeRecord> recordsToWrite = encodedRecords;
        Long expectedRecordCount = ControlFileUtils.readExpectedRecordCount(options.getControlFile());
        if (expectedRecordCount != null) {
            PCollection<Void> countValidation = encodedRecords
                    .apply("CountValidRecords", Count.globally())
                    .apply("ValidateRecordCount", ParDo.of(new RecordCountValidationFn(expectedRecordCount)));
            recordsToWrite = encodedRecords.apply("AwaitRecordCountValidation", Wait.on(countValidation));
        }

        DatabaseWriter.write(recordsToWrite);

        PCollectionList<String> allErrors = PCollectionList.of(parsed.get(ERROR_RECORDS))
                .and(validated.get(ERROR_RECORDS));
        allErrors.apply("FlattenErrors", Flatten.pCollections())
            .apply("WriteErrorLogs", TextIO.write()
                .to(options.getErrorLogPath())
                .withSuffix(".txt")
                .withNumShards(1));

        pipeline.run().waitUntilFinish();
    }

}
