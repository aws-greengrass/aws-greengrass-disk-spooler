/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import java.util.Arrays;

@SuppressWarnings("MissingJavadocMethod")
public enum PayloadFormatIndicator {
    BYTES(0),
    UTF8(1);

    private final int indicator;

    PayloadFormatIndicator(int value) {
        this.indicator = value;
    }

    public int getValue() {
        return this.indicator;
    }

    public static PayloadFormatIndicator fromIndicator(int indicator) {
        return Arrays.stream(PayloadFormatIndicator.values())
                .filter(i -> i.getValue() == indicator)
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unexpected payload format indicator " + indicator));
    }
}
