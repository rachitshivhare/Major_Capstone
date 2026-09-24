package com.amex.lumi.Model;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class EmployeeRecordTest {

    @Test
    void copyCreatesIndependentCopyOfRecord() {
        EmployeeRecord record = new EmployeeRecord();
        record.employeeId = "E123456";
        record.firstName = "Alice";
        record.skills = List.of("Java", "Beam");
        record.salary = BigDecimal.valueOf(1000.00);

        EmployeeRecord copy = record.copy();
        copy.firstName = "Bob";
        copy.skills = List.of("Python");

        assertEquals("Alice", record.firstName);
        assertEquals(List.of("Java", "Beam"), record.skills);
        assertEquals("Bob", copy.firstName);
    }

    @Test
    void cleanseNullsAppliesDefaultsAndKeepsValues() {
        EmployeeRecord record = new EmployeeRecord();
        record.email = "   ";
        record.salary = null;
        record.isActive = null;
        record.skills = null;

        record.cleanseNulls();

        assertEquals(" ", record.email);
        assertEquals(BigDecimal.ZERO, record.salary);
        assertFalse(record.isActive);
        assertEquals(List.of(), record.skills);
        assertNotNull(record.sourceCreationTime);
        assertNotNull(record.ingestionTimestamp);
    }

    @Test
    void defaultWhitespaceAndInstantDefaultsAreApplied() {
        EmployeeRecord record = new EmployeeRecord();
        record.city = null;
        record.sourceCreationTime = null;
        record.ingestionTimestamp = null;

        record.cleanseNulls();

        assertEquals(" ", record.city);
        assertNotNull(record.sourceCreationTime);
        assertNotNull(record.ingestionTimestamp);
    }
}
