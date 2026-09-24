package com.amex.lumi.Validation;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static com.amex.lumi.Ingestion.ERROR_RECORDS;
import static com.amex.lumi.Ingestion.VALID_RECORDS;
import static org.junit.Assert.assertEquals;

public class RecordValidationFnTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void validRecordPassesValidation() {
        EmployeeRecord record = new EmployeeRecord();
        record.employeeId = "E123456";
        record.firstName = "Alice";
        record.lastName = "Smith";
        record.email = "alice.smith@example.com";
        record.phoneNumber = "+1 (555) 123-4567";
        record.hireDate = LocalDate.of(2022, 1, 15);
        record.department = "Engineering";
        record.jobTitle = "Engineer";
        record.salary = new BigDecimal("85000.00");
        record.currency = "USD";
        record.employmentStatus = "FULL_TIME";
        record.managerId = "M123456";
        record.isActive = true;
        record.skills = List.of("Java");
        record.street = "123 Main St";
        record.city = "Austin";
        record.state = "TX";
        record.postalCode = "78701";
        record.country = "USA";
        record.emergencyName = "Jane Smith";
        record.emergencyRelationship = "Spouse";
        record.emergencyPhone = "5551231111";
        record.emergencyEmail = "jane@example.com";

        PCollectionTuple tuple = pipeline.apply(Create.of(record))
                .apply(ParDo.of(new RecordValidationFn())
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));

        PAssert.that(tuple.get(VALID_RECORDS)).satisfies((Iterable<EmployeeRecord> records) -> {
            int count = 0;
            for (EmployeeRecord actual : records) {
                count++;
                assertEquals(record.employeeId, actual.employeeId);
                assertEquals(record.firstName, actual.firstName);
                assertEquals(record.email, actual.email);
                assertEquals(record.phoneNumber, actual.phoneNumber);
                assertEquals(record.salary, actual.salary);
                assertEquals(record.city, actual.city);
            }
            assertEquals(1, count);
            return null;
        });
        PAssert.that(tuple.get(ERROR_RECORDS)).empty();

        pipeline.run().waitUntilFinish();
    }

    @Test
    public void invalidRecordIsSentToErrorOutput() {
        EmployeeRecord record = new EmployeeRecord();
        record.employeeId = "BAD";
        record.firstName = "A";
        record.lastName = "B";
        record.email = "bad";
        record.phoneNumber = "123";
        record.hireDate = LocalDate.of(1999, 1, 1);
        record.department = "D";
        record.jobTitle = "J";
        record.salary = new BigDecimal("-1");
        record.currency = "US";
        record.employmentStatus = "X";
        record.managerId = "M";
        record.isActive = null;
        record.skills = List.of();
        record.street = "";
        record.city = "";
        record.state = "";
        record.postalCode = "";
        record.country = "";
        record.emergencyName = "";
        record.emergencyRelationship = "";
        record.emergencyPhone = "";
        record.emergencyEmail = "";

        PCollectionTuple tuple = pipeline.apply(Create.of(record))
                .apply(ParDo.of(new RecordValidationFn())
                        .withOutputTags(VALID_RECORDS, TupleTagList.of(ERROR_RECORDS)));

        PAssert.that(tuple.get(VALID_RECORDS)).empty();
        PAssert.that(tuple.get(ERROR_RECORDS)).satisfies((Iterable<String> errors) -> {
            for (String error : errors) {
                if (error.contains("validation errors")) {
                    return null;
                }
            }
            throw new AssertionError("Expected a validation error message");
        });

        pipeline.run().waitUntilFinish();
    }
}
