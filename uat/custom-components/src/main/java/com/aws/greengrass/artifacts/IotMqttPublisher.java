/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.artifacts;

import com.aws.greengrass.utils.Client;
import com.aws.greengrass.utils.IPCTestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPC;
import software.amazon.awssdk.aws.greengrass.GreengrassCoreIPCClientV2;
import software.amazon.awssdk.aws.greengrass.model.PublishToIoTCoreRequest;
import software.amazon.awssdk.aws.greengrass.model.QOS;
import software.amazon.awssdk.aws.greengrass.model.ReportedLifecycleState;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public class IotMqttPublisher implements Consumer<String[]> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IotMqttPublisher.class);
    private static final String SPOOL_SIZE_ERROR = "Message is larger than the size of message spool.";
    private static final String SPOOL_FULL_ERROR = "Message spool is full. Message could not be added.";

    @Override
    public void accept(String[] args) {
        try (Client assertionClient = new Client();
             GreengrassCoreIPCClientV2 eventStreamRpcConnection = IPCTestUtils.getGreengrassClient()) {
            GreengrassCoreIPC greengrassCoreIPCClient = eventStreamRpcConnection.getClient();

            String topic = System.getenv("topic");
            String payload = System.getenv("payload");
            QOS qos = IPCTestUtils.getQOSFromValue(Integer.parseInt(System.getenv("qos")));

            PublishToIoTCoreRequest request = new PublishToIoTCoreRequest();
            request.setTopicName(topic);
            request.setPayload(payload.getBytes(StandardCharsets.UTF_8));
            request.setQos(qos);

            try {
                greengrassCoreIPCClient.publishToIoTCore(request, Optional.empty()).getResponse().get();
                assertionClient.sendAssertion(true, "Successfully published to IoT topic " + topic, "");
            } catch (ExecutionException e) {
                LOGGER.error("Error occurred while publishing to IoT topic", e);
                try {
                    String errorMessage = e.getCause().getMessage();
                    if (errorMessage.contains(SPOOL_SIZE_ERROR) || errorMessage.contains(SPOOL_FULL_ERROR)) {
                        assertionClient.sendAssertion(true, "SPOOL_FULL_ERROR", "Spooler is full");
                        return;
                    }
                    if (e.getCause() instanceof UnauthorizedError) {
                        assertionClient.sendAssertion(true, e.getCause().getMessage(), "Unauthorized error while publishing to IoT topic " + topic);
                        return;
                    }
                } catch (IOException e2) {
                    LOGGER.error("Failed to send assertion", e2);
                }
                IPCTestUtils.reportState(greengrassCoreIPCClient, ReportedLifecycleState.ERRORED);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.exit(1);
        } catch (Exception e) {
            LOGGER.error("Service errored", e);
            System.exit(1);
        }
    }
}
