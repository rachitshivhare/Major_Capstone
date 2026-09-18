package com.amex.lumi.Validation;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

import static com.amex.lumi.Ingestion.ERROR_RECORDS;
import static com.amex.lumi.Ingestion.VALID_RECORDS;

public class RecordValidationFn extends DoFn<EmployeeRecord, EmployeeRecord> {
    @ProcessElement
    public void processElement(@Element EmployeeRecord record, ProcessContext context) {
        List<String> failures = new ArrayList<>();

        validateLength(failures, "employee_id", String.valueOf(record.employeeId), 7, 36);
        validateLength(failures, "first_name", record.firstName, 3, 15);
        validateLength(failures, "last_name", record.lastName, 0, 15);
        validateLength(failures, "email", record.email, 13, 30);
        validateLength(failures, "phone_number", normalisePhone(record.phoneNumber), 10, 10);
        validateDate(failures, String.valueOf(record.hireDate));
        validateLength(failures, "department", record.department, 0, 20);
        validateLength(failures, "job_title", record.jobTitle, 0, 30);
        validateLength(failures, "currency", record.currency, 3, 3);
        validateLength(failures, "employment_status", record.employmentStatus, 3, 13);
        validateLength(failures, "manager_id", String.valueOf(record.managerId), 7, 36);
        if (record.isActive == null) {
            failures.add("is_active must be true or false");
        }
        validateLength(failures, "skills", record.skills == null ? null : record.skills.toString(), 0, 100);

        if (record.salary == null || record.salary.compareTo(BigDecimal.ZERO) < 0) {
            failures.add("salary must be a non-negative number");
        }

        validateRequired(failures, "address.street", record.street);
        validateRequired(failures, "address.city", record.city);
        validateRequired(failures, "address.state", record.state);
        validateRequired(failures, "address.postal_code", record.postalCode);
        validateRequired(failures, "address.country", record.country);
        validateRequired(failures, "emergency_contact.name", record.emergencyName);
        validateRequired(failures, "emergency_contact.relationship", record.emergencyRelationship);
        validateRequired(failures, "emergency_contact.phone", record.emergencyPhone);
        validateRequired(failures, "emergency_contact.email", record.emergencyEmail);

        if (failures.isEmpty()) {
            context.output(VALID_RECORDS, record);
        } else {
            context.output(ERROR_RECORDS, "employee_id=" + printable(String.valueOf(record.employeeId))
                    + " | validation errors: " + String.join("; ", failures)
                    + " | record=" + recordSummary(record));
        }
    }

    private static void validateLength(List<String> failures, String field, String value, int minimum, int maximum) {
        int length = value == null ? 0 : value.trim().length();
        if (length < minimum || length > maximum) {
            failures.add(field + " length must be between " + minimum + " and " + maximum
                    + " but was " + length);
        }
    }

    private static void validateRequired(List<String> failures, String field, String value) {
        if (value == null || value.trim().isEmpty()) {
            failures.add(field + " is required");
        }
    }

    private static void validateDate(List<String> failures, String value) {
        validateLength(failures, "hire_date", value, 10, 10);
        if (value != null) {
            try {
                LocalDate.parse(value.trim());
            } catch (DateTimeParseException exception) {
                failures.add("hire_date" + " must use yyyy-MM-dd format");
            }
        }
    }

    private static String normalisePhone(String value) {
        if (value == null) {
            return null;
        }
        String digits = value.replaceAll("\\D", "");
        if (digits.startsWith("91") && digits.length() == 12) {
            return digits.substring(2);
        }
        return digits;
    }

    private static String printable(String value) {
        return value == null ? "<null>" : value;
    }

    private static String recordSummary(EmployeeRecord record) {
        return "{first_name=" + printable(record.firstName)
                + ", last_name=" + printable(record.lastName)
                + ", email=" + printable(record.email)
                + ", phone_number=" + printable(record.phoneNumber)
                + ", hire_date=" + printable(String.valueOf(record.hireDate))
                + ", department=" + printable(record.department)
                + ", job_title=" + printable(record.jobTitle)
                + ", salary=" + record.salary
                + ", currency=" + printable(record.currency)
                + ", employment_status=" + printable(record.employmentStatus)
                + ", manager_id=" + printable(String.valueOf(record.managerId))
                + ", is_active=" + record.isActive
                + ", skills=" + printable(record.skills == null ? null : record.skills.toString()) + "}";
    }
}