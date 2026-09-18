package com.amex.lumi.Utils;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.values.PCollection;

public final class DatabaseWriter {
    private DatabaseWriter() {
    }

    public static void write(PCollection<EmployeeRecord> records) {
        records.apply("WriteToDatabase", JdbcIO.<EmployeeRecord>write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                                "com.mysql.cj.jdbc.Driver", databaseSetting(
                                        "INGESTION_DB_URL",
                                        "jdbc:mysql://localhost:3306/beam_warehouse"))
                        .withUsername(requiredSetting("INGESTION_DB_USERNAME"))
                        .withPassword(requiredSetting("INGESTION_DB_PASSWORD")))
                .withStatement("INSERT INTO employee_warehouse (employee_id, first_name, last_name, email, phone_number, hire_date, department, job_title, salary, currency, employment_status, manager_id, is_active, skills, street, city, state, postal_code, country, emergency_name, emergency_relationship, emergency_phone, emergency_email, execution_id, source_creation_time, ingestion_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .withPreparedStatementSetter((record, statement) -> {
                    statement.setString(1, record.employeeId);
                    statement.setString(2, record.firstName);
                    statement.setString(3, record.lastName);
                    statement.setString(4, record.email);
                    statement.setString(5, record.encryptedPhoneNumber);
                    statement.setString(6, String.valueOf(record.hireDate));
                    statement.setString(7, record.department);
                    statement.setString(8, record.jobTitle);
                    statement.setBigDecimal(9, record.salary);
                    statement.setString(10, record.currency);
                    statement.setString(11, record.employmentStatus);
                    statement.setString(12, record.managerId);
                    statement.setBoolean(13, Boolean.TRUE.equals(record.isActive));
                    statement.setString(14, record.skills.toString());
                    statement.setString(15, record.street);
                    statement.setString(16, record.city);
                    statement.setString(17, record.state);
                    statement.setString(18, record.postalCode);
                    statement.setString(19, record.country);
                    statement.setString(20, record.emergencyName);
                    statement.setString(21, record.emergencyRelationship);
                    statement.setString(22, record.emergencyPhone);
                    statement.setString(23, record.emergencyEmail);
                    statement.setString(24, String.valueOf(record.executionId));
                    statement.setString(25, String.valueOf(record.sourceCreationTime));
                    statement.setString(26, String.valueOf(record.ingestionTimestamp));
                }));
    }

    private static String databaseSetting(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String requiredSetting(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " must be provided by the runtime environment");
        }
        return value;
    }
}