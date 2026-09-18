package com.amex.lumi.Praser;

import com.amex.lumi.Model.EmployeeRecord;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.amex.lumi.Ingestion.ERROR_RECORDS;
import static com.amex.lumi.Ingestion.VALID_RECORDS;

public class JsonParseFn extends DoFn<String, EmployeeRecord> {
    private transient ObjectMapper objectMapper;
    private final UUID execId;

    @DoFn.Setup
    public void setup() {
        objectMapper = new ObjectMapper();
    }

    public JsonParseFn(UUID execId) {
        this.execId = execId;
    }

    @ProcessElement
    public void processElement(@Element String line, ProcessContext c) {
        try {
            JsonNode root = objectMapper.readTree(line);
            if (root.isArray()) {
                for (JsonNode node : root) {
                    emitRecord(node, c);
                }
            } else {
                emitRecord(root, c);
            }
        } catch (Exception e) {
            c.output(ERROR_RECORDS, line + " | JSON Parse Error: " + e.getMessage());
        }
    }

    private void emitRecord(JsonNode node, ProcessContext c) {
        try {
            EmployeeRecord rec = new EmployeeRecord();
            rec.employeeId = node.path("employee_id").asText();
            rec.firstName = node.path("first_name").asText();
            rec.lastName = node.path("last_name").asText();
            rec.email = node.path("email").asText();
            rec.phoneNumber = node.path("phone_number").asText();
            rec.hireDate = LocalDate.parse(node.path("hire_date").asText());
            rec.department = node.path("department").asText();
            rec.jobTitle = node.path("job_title").asText();
            rec.salary = BigDecimal.valueOf(node.hasNonNull("salary") ? node.get("salary").asDouble() : null);
            rec.currency = node.path("currency").asText();
            rec.employmentStatus = node.path("employment_status").asText();
            rec.managerId = node.path("manager_id").asText();
            rec.isActive = node.hasNonNull("is_active") ? node.get("is_active").asBoolean() : null;

            if (node.has("skills")) {
                List<String> list = objectMapper.convertValue(node.get("skills"), new TypeReference<List<String>>(){});
                rec.skills = Collections.singletonList(String.join(";", list));
            }

            JsonNode addr = node.path("address");
            rec.street = addr.path("street").asText();
            rec.city = addr.path("city").asText();
            rec.state = addr.path("state").asText();
            rec.postalCode = addr.path("postal_code").asText();
            rec.country = addr.path("country").asText();

            JsonNode emg = node.path("emergency_contact");
            rec.emergencyName = emg.path("name").asText();
            rec.emergencyRelationship = emg.path("relationship").asText();
            rec.emergencyPhone = emg.path("phone").asText();
            rec.emergencyEmail = emg.path("email").asText();
            rec.sourceCreationTime = Instant.parse(Instant.now().toString());
            rec.executionId = execId;
            rec.ingestionTimestamp = Instant.parse(Instant.now().toString());
            c.output(VALID_RECORDS, rec);
        } catch (Exception e) {
            c.output(ERROR_RECORDS, node + " | JSON Parse Error: " + e.getMessage());
        }
    }
}
