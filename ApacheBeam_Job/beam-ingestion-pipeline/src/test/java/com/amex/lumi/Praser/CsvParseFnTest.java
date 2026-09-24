package com.amex.lumi.Praser;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

import java.util.UUID;

import static com.amex.lumi.Ingestion.ERROR_RECORDS;
import static com.amex.lumi.Ingestion.VALID_RECORDS;

public class CsvParseFnTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void parsesCsvLineIntoEmployeeRecord() {
        String line = "E123456,Alice,Smith,alice@example.com,5551234567,2024-01-15,Engineering,Engineer,85000.00,USD,FULL_TIME,M123456,true,Java,123 Main St,Austin,TX,78701,USA,Jane Smith,Spouse,5551112222,jane@example.com";

        PCollectionTuple tuple = pipeline.apply(Create.of(line))
                .apply(ParDo.of(new CsvParseFn(UUID.randomUUID()))
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));

        PAssert.that(tuple.get(VALID_RECORDS)).satisfies((Iterable<EmployeeRecord> records) -> {
            int count = 0;
            for (EmployeeRecord record : records) {
                count++;
                assert record.employeeId.equals("E123456");
                assert record.city.equals("Austin");
                assert record.executionId != null;
            }
            assert count == 1;
            return null;
        });
        PAssert.that(tuple.get(ERROR_RECORDS)).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void skipsHeaderRowAndRejectsMalformedLine() {
        String header = "employee_id,first_name,last_name,email,phone_number,hire_date,department,job_title,salary,currency,employment_status,manager_id,is_active,skills,street,city,state,postal_code,country,emergency_name,emergency_relationship,emergency_phone,emergency_email";
        String malformed = "E123456,Alice,Smith,alice@example.com,5551234567,2024-01-15";

        PCollectionTuple headerTuple = pipeline.apply("Create header", Create.of(header))
            .apply("Parse header", ParDo.of(new CsvParseFn(UUID.randomUUID()))
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));
        PCollectionTuple malformedTuple = pipeline.apply("Create malformed row", Create.of(malformed))
            .apply("Parse malformed row", ParDo.of(new CsvParseFn(UUID.randomUUID()))
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));

        PAssert.that(headerTuple.get(VALID_RECORDS)).empty();
        PAssert.that(malformedTuple.get(VALID_RECORDS)).empty();
        PAssert.that(malformedTuple.get(ERROR_RECORDS)).satisfies((Iterable<String> errors) -> {
            int count = 0;
            for (String error : errors) {
                count++;
                assert error.contains("CSV Parse Error");
            }
            assert count == 1;
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
