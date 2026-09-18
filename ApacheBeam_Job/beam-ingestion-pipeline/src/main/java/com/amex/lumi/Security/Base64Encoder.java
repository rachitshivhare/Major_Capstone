package com.amex.lumi.Security;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class Base64Encoder {

    public String encode(String value) {
        if (value == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }
}