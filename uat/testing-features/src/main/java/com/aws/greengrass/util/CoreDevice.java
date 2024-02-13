/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.util;

import com.aws.greengrass.testing.model.RegistrationContext;
import com.aws.greengrass.testing.model.TestContext;
import com.aws.greengrass.testing.resources.AWSResources;
import com.aws.greengrass.testing.resources.iot.IotLifecycle;
import com.aws.greengrass.testing.resources.iot.IotThingSpec;
import com.google.inject.Inject;
import lombok.Getter;

/**
 * Helper class that provides information related to the core device under test.
 */
@SuppressWarnings("MissingJavadocMethod")
public class CoreDevice {

    private final AWSResources awsResources;
    private final TestContext testContext;
    @Getter
    private final String rootCA;
    @Getter
    private final String iotCoreDataEndpoint;

    @Inject
    public CoreDevice(TestContext testContext,
                      RegistrationContext registrationContext,
                      AWSResources awsResources) {
        this.awsResources = awsResources;
        this.testContext = testContext;
        this.rootCA = registrationContext.rootCA();
        this.iotCoreDataEndpoint = awsResources.lifecycle(IotLifecycle.class).dataEndpoint();
    }

    public IotThingSpec getSpec() {
        return awsResources.trackingSpecs(IotThingSpec.class)
                .filter(s -> s.thingName().equals(testContext.coreThingName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("no core thing found"));
    }
}
