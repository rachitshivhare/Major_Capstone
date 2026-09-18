package com.amex.lumi.Praser;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Objects;
import java.util.UUID;

import static com.amex.lumi.Ingestion.ERROR_RECORDS;
import static com.amex.lumi.Ingestion.VALID_RECORDS;

public class CsvParseFn extends DoFn<String, EmployeeRecord> {
    private final UUID execId;

    public CsvParseFn(UUID execId) {
        this.execId = execId;
    }

    @ProcessElement
    public void processElement(@Element String line, ProcessContext c) {
        try {
            // Regex handles comma delimiter while ignoring commas inside quotes
            String[] tokens = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            for (int i = 0; i < tokens.length; i++) {
                // Strip surrounding quotes and whitespace
                tokens[i] = tokens[i].replaceAll("^\"|\"$", "").trim();
                String HEADER_PREFIX = "employee_id";
                if (Objects.equals(tokens[0], HEADER_PREFIX))  return;
            }

            EmployeeRecord rec = new EmployeeRecord();
            rec.employeeId = tokens[0];
            rec.firstName = tokens[1];
            rec.lastName = tokens[2];
            rec.email = tokens[3];
            rec.phoneNumber = tokens[4];
            rec.hireDate = LocalDate.parse(tokens[5]);
            rec.department = tokens[6];
            rec.jobTitle = tokens[7];
            rec.salary = BigDecimal.valueOf(tokens[8].isEmpty() ? null : Double.parseDouble(tokens[8]));
            rec.currency = tokens[9];
            rec.employmentStatus = tokens[10];
            rec.managerId = tokens[11];
            rec.isActive = Boolean.parseBoolean(tokens[12]);
            rec.skills = Collections.singletonList(tokens[13]);
            rec.street = tokens[14];
            rec.city = tokens[15];
            rec.state = tokens[16];
            rec.postalCode = tokens[17];
            rec.country = tokens[18];
            rec.emergencyName = tokens[19];
            rec.emergencyRelationship = tokens[20];
            rec.emergencyPhone = tokens[21];
            rec.emergencyEmail = tokens[22];
            rec.sourceCreationTime = Instant.parse(Instant.now().toString());
            rec.executionId = execId;
            rec.ingestionTimestamp = Instant.parse(Instant.now().toString());
            c.output(VALID_RECORDS, rec);
        } catch (Exception e) {
            c.output(ERROR_RECORDS, line + " | CSV Parse Error: " + e.getMessage());
        }
    }
}
