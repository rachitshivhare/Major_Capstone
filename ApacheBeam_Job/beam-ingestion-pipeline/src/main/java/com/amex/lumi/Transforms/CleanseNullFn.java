package com.amex.lumi.Transforms;

import com.amex.lumi.Model.EmployeeRecord;
import org.apache.beam.sdk.transforms.DoFn;

public final class CleanseNullFn extends DoFn<EmployeeRecord, EmployeeRecord> {

    @ProcessElement
    public void processElement(@Element EmployeeRecord record, OutputReceiver<EmployeeRecord> output) {
        EmployeeRecord copy = record.copy();
        copy.cleanseNulls();
        output.output(copy);
    }
}
