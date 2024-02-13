/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import lombok.Builder;
import lombok.Value;

import java.util.function.Consumer;

@Value
@Builder
@SuppressWarnings("checkstyle:VisibilityModifier")
public class Subscription {
    String topic;
    Consumer<Message> callback;
    SubscribeRequest request;
}
