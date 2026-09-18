package com.amex.lumi.Model;
import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;


public class EmployeeRecord implements Serializable {
    public String employeeId;
    public String firstName;
    public String lastName;
    public String email;
    public String phoneNumber;
    public LocalDate hireDate;
    public String department;
    public String jobTitle;
    public BigDecimal salary;
    public String currency;
    public String employmentStatus;
    public String managerId;
    public Boolean isActive;

    public List<String> skills;

    public String street;
    public String city;
    public String state;
    public String postalCode;
    public String country;

    public String emergencyName;
    public String emergencyRelationship;
    public String emergencyPhone;
    public String emergencyEmail;

    public String encryptedPhoneNumber;
    public String encryptedSalary;

    public UUID executionId;
    public Instant sourceCreationTime;
    public Instant ingestionTimestamp;

    public EmployeeRecord copy() {
        EmployeeRecord copy = new EmployeeRecord();
        copy.employeeId = this.employeeId;
        copy.firstName = this.firstName;
        copy.lastName = this.lastName;
        copy.email = this.email;
        copy.phoneNumber = this.phoneNumber;
        copy.hireDate = this.hireDate;
        copy.department = this.department;
        copy.jobTitle = this.jobTitle;
        copy.salary = this.salary;
        copy.currency = this.currency;
        copy.employmentStatus = this.employmentStatus;
        copy.managerId = this.managerId;
        copy.isActive = this.isActive;

        copy.skills = this.skills == null ? null : List.copyOf(this.skills);

        copy.street = this.street;
        copy.city = this.city;
        copy.state = this.state;
        copy.postalCode = this.postalCode;
        copy.country = this.country;
        
        copy.emergencyName = this.emergencyName;
        copy.emergencyRelationship = this.emergencyRelationship;
        copy.emergencyPhone = this.emergencyPhone;
        copy.emergencyEmail = this.emergencyEmail;
        
        copy.encryptedPhoneNumber = this.encryptedPhoneNumber;
        copy.encryptedSalary = this.encryptedSalary;
        
        copy.executionId = this.executionId;
        copy.sourceCreationTime = this.sourceCreationTime;
        copy.ingestionTimestamp = this.ingestionTimestamp;
        return copy;
    }

    public void cleanseNulls() {
        this.firstName = defaultWhitespace(this.firstName);
        this.lastName = defaultWhitespace(this.lastName);
        this.email = defaultWhitespace(this.email);
        this.phoneNumber = defaultWhitespace(this.phoneNumber);
        this.department = defaultWhitespace(this.department);
        this.jobTitle = defaultWhitespace(this.jobTitle);
        this.currency = defaultWhitespace(this.currency);
        this.employmentStatus = defaultWhitespace(this.employmentStatus);
        this.skills = defaultValue(this.skills, List.of());
        this.street = defaultWhitespace(this.street);
        this.city = defaultWhitespace(this.city);
        this.state = defaultWhitespace(this.state);
        this.postalCode = defaultWhitespace(this.postalCode);
        this.country = defaultWhitespace(this.country);
        this.emergencyName = defaultWhitespace(this.emergencyName);
        this.emergencyRelationship = defaultWhitespace(this.emergencyRelationship);
        this.emergencyPhone = defaultWhitespace(this.emergencyPhone);
        this.emergencyEmail = defaultWhitespace(this.emergencyEmail);
        this.salary = defaultValue(this.salary, BigDecimal.ZERO);
        this.isActive = defaultValue(this.isActive, false);
        this.sourceCreationTime = defaultValue(this.sourceCreationTime, Instant.now());
        this.ingestionTimestamp = defaultValue(this.ingestionTimestamp, Instant.now());
    }

    private String defaultWhitespace(String val) {
        return (val == null || val.trim().isEmpty()) ? " " : val;
    }

    private <T> T defaultValue(T value, T fallback) {
        return value == null ? fallback : value;
    }
}