package com.amex.lumi.Security;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class Base64EncoderTest {

    @Test
    void encodeReturnsBase64ValueForText() {
        Base64Encoder encoder = new Base64Encoder();

        assertEquals("aGVsbG8=", encoder.encode("hello"));
    }

    @Test
    void encodeReturnsNullWhenValueIsNull() {
        Base64Encoder encoder = new Base64Encoder();

        assertNull(encoder.encode(null));
    }
}
