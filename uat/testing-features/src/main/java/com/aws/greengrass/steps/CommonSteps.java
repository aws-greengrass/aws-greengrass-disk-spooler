/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.steps;

import com.aws.greengrass.testing.model.ScenarioContext;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.en.When;

import java.util.UUID;

@ScenarioScoped
public class CommonSteps {
    private final ScenarioContext scenarioContext;

    @Inject
    public CommonSteps(ScenarioContext scenarioContext) {
        this.scenarioContext = scenarioContext;
    }

    @When("I create a random name as {word}")
    public void storeRandomNameInContext(String key) {
        scenarioContext.put(key, randomName());
    }

    private static String randomName() {
        return String.format("e2e-%d-%s", System.currentTimeMillis(), UUID.randomUUID());
    }
}
