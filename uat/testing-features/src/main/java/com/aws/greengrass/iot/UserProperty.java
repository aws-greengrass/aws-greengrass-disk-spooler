/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
@SuppressWarnings("checkstyle:VisibilityModifier")
public class UserProperty {
    String key;
    String value;
}
