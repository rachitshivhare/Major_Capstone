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

public class JsonParseFnTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void parsesSingleJsonEmployeeRecord() {
        String json = "{\n"
                + "  \"employee_id\": \"E123456\",\n"
                + "  \"first_name\": \"Alice\",\n"
                + "  \"last_name\": \"Smith\",\n"
                + "  \"email\": \"alice@example.com\",\n"
                + "  \"phone_number\": \"5551234567\",\n"
                + "  \"hire_date\": \"2024-01-15\",\n"
                + "  \"department\": \"Engineering\",\n"
                + "  \"job_title\": \"Engineer\",\n"
                + "  \"salary\": 85000.0,\n"
                + "  \"currency\": \"USD\",\n"
                + "  \"employment_status\": \"FULL_TIME\",\n"
                + "  \"manager_id\": \"M123456\",\n"
                + "  \"is_active\": true,\n"
                + "  \"skills\": [\"Java\", \"Beam\"],\n"
                + "  \"address\": {\n"
                + "    \"street\": \"123 Main St\",\n"
                + "    \"city\": \"Austin\",\n"
                + "    \"state\": \"TX\",\n"
                + "    \"postal_code\": \"78701\",\n"
                + "    \"country\": \"USA\"\n"
                + "  },\n"
                + "  \"emergency_contact\": {\n"
                + "    \"name\": \"Jane Smith\",\n"
                + "    \"relationship\": \"Spouse\",\n"
                + "    \"phone\": \"5551112222\",\n"
                + "    \"email\": \"jane@example.com\"\n"
                + "  }\n"
                + "}";

        PCollectionTuple tuple = pipeline.apply(Create.of(json))
                .apply(ParDo.of(new JsonParseFn(UUID.randomUUID()))
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
    public void invalidJsonIsSentToErrorOutput() {
        String invalidJson = "{bad json";

        PCollectionTuple tuple = pipeline.apply(Create.of(invalidJson))
                .apply(ParDo.of(new JsonParseFn(UUID.randomUUID()))
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));

        PAssert.that(tuple.get(VALID_RECORDS)).empty();
        PAssert.that(tuple.get(ERROR_RECORDS)).satisfies((Iterable<String> errors) -> {
            int count = 0;
            for (String error : errors) {
                count++;
                assert error.contains("JSON Parse Error");
            }
            assert count == 1;
            return null;
        });

        pipeline.run().waitUntilFinish();
    }
}
