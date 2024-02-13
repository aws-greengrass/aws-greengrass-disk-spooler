/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Value
@Builder
@SuppressWarnings({"PMD.AvoidFieldNameMatchingTypeName", "checkstyle:VisibilityModifier", "MissingJavadocMethod"})
public class Message {

    @Getter(lazy = true)
    String message = messageFromPayload();
    byte[] payload;
    String contentType;
    Long topicAlias;
    byte[] correlationData;
    Long messageExpiryIntervalSeconds;
    PayloadFormatIndicator payloadFormat;
    String responseTopic;
    boolean retain;
    List<Long> subscriptionIdentifiers;
    List<UserProperty> userProperties;

    String messageFromPayload() {
        return payload == null ? null : new String(payload, StandardCharsets.UTF_8);
    }
}
