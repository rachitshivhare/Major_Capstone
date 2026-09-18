package com.amex.lumi.Transforms;

import org.apache.beam.sdk.transforms.DoFn;

public final class RecordCountValidationFn extends DoFn<Long, Void> {
    private final long expectedCount;

    public RecordCountValidationFn(long expectedCount) {
        this.expectedCount = expectedCount;
    }

    @ProcessElement
    public void processElement(@Element Long actualCount) {
        if (actualCount != expectedCount) {
            throw new IllegalStateException("Record count mismatch: expected "
                    + expectedCount + " but loaded " + actualCount);
        }
    }
}