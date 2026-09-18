package com.amex.lumi.Transforms;

import com.amex.lumi.Model.EmployeeRecord;
import com.amex.lumi.Security.Base64Encoder;
import org.apache.beam.sdk.transforms.DoFn;

public final class EncodeFn extends DoFn<EmployeeRecord, EmployeeRecord> {
    private transient Base64Encoder encoder;

    @Setup
    public void setup() {
        encoder = new Base64Encoder();
    }

    @ProcessElement
    public void processElement(@Element EmployeeRecord record, OutputReceiver<EmployeeRecord> output) {
        EmployeeRecord copy = record.copy();
        copy.encryptedPhoneNumber = encoder.encode(copy.phoneNumber);
        copy.encryptedSalary = encoder.encode(String.valueOf(copy.salary));
        output.output(copy);
    }
}
