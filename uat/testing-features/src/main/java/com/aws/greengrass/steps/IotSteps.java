/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.steps;

import com.aws.greengrass.iot.IotCoreClient;
import com.aws.greengrass.iot.Message;
import com.aws.greengrass.iot.SubscribeRequest;
import com.aws.greengrass.testing.features.WaitSteps;
import com.aws.greengrass.testing.model.ScenarioContext;
import com.google.inject.Inject;
import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.crt.mqtt5.QOS;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Log4j2
@ScenarioScoped
@SuppressWarnings("MissingJavadocMethod")
public class IotSteps {
    private final ScenarioContext scenarioContext;
    private final WaitSteps waitSteps;
    private final IotCoreClient iotCoreClient;
    private final Map<String, List<Message>> messagesByTopic = Collections.synchronizedMap(new HashMap<>());

    @Inject
    public IotSteps(ScenarioContext scenarioContext,
                    WaitSteps waitSteps,
                    IotCoreClient iotCoreClient) {
        this.scenarioContext = scenarioContext;
        this.waitSteps = waitSteps;
        this.iotCoreClient = iotCoreClient;
    }

    @After
    public void close() {
        iotCoreClient.close();
    }

    @When("I subscribe to cloud topics")
    public void subscribeToCloudTopics(List<String> topics)
            throws IOException, ExecutionException, InterruptedException, TimeoutException {
        for (String topic : topics) {
            String resolvedTopic = scenarioContext.applyInline(topic);
            iotCoreClient.subscribe(SubscribeRequest.builder()
                    .topic(resolvedTopic)
                    .qos(QOS.AT_LEAST_ONCE)
                    .callback(message -> {
                        messagesByTopic.compute(resolvedTopic, (s, messages) -> {
                            if (messages == null) {
                                messages = new CopyOnWriteArrayList<>();
                            }
                            messages.add(message);
                            return messages;
                        });
                    })
                    .build());
        }
    }

    @Then("the cloud topic {word} receives the following messages within {int} seconds")
    public void checkMessagesOnTopic(String topic, int timeoutSeconds, List<String> messages)
            throws InterruptedException {
        String resolvedTopic = scenarioContext.applyInline(topic);
        assertTrue(waitSteps.untilTrue(
                () -> {
                    List<String> receivedMessages = receivedMessagePayloadsByTopic(resolvedTopic);
                    return Objects.equals(messages, receivedMessages);
                },
                timeoutSeconds, TimeUnit.SECONDS));
    }

    private List<String> receivedMessagePayloadsByTopic(String topic) {
        List<Message> messages = messagesByTopic.get(topic);
        if (messages == null) {
            return Collections.emptyList();
        }
        return messages.stream().map(Message::getMessage).collect(Collectors.toList());
    }
}
