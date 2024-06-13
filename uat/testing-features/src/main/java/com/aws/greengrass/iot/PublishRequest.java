/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.crt.mqtt5.QOS;

@Value
@Builder
@SuppressWarnings("checkstyle:VisibilityModifier")
public class PublishRequest {
    String topic;
    Message message;
    @Builder.Default
    QOS qos = QOS.AT_LEAST_ONCE;
}
