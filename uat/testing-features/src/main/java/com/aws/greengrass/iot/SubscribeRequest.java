/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import lombok.Builder;
import lombok.Value;
import software.amazon.awssdk.crt.mqtt5.QOS;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

@Value
@Builder
@SuppressWarnings("checkstyle:VisibilityModifier")
public class SubscribeRequest {
    String topic;
    @Builder.Default
    Consumer<Message> callback = m -> {
    };
    @Builder.Default
    BiConsumer<SubAckPacket, Throwable> errorCallback = (p, e) -> {
    };
    @Builder.Default
    QOS qos = QOS.AT_LEAST_ONCE;
}
