/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.iot;

import com.aws.greengrass.util.CoreDevice;
import com.google.inject.Inject;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CrtRuntimeException;
import software.amazon.awssdk.crt.mqtt5.Mqtt5Client;
import software.amazon.awssdk.crt.mqtt5.Mqtt5ClientOptions;
import software.amazon.awssdk.crt.mqtt5.OnAttemptingConnectReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionFailureReturn;
import software.amazon.awssdk.crt.mqtt5.OnConnectionSuccessReturn;
import software.amazon.awssdk.crt.mqtt5.OnDisconnectionReturn;
import software.amazon.awssdk.crt.mqtt5.OnStoppedReturn;
import software.amazon.awssdk.crt.mqtt5.PublishResult;
import software.amazon.awssdk.crt.mqtt5.PublishReturn;
import software.amazon.awssdk.crt.mqtt5.packets.ConnAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.ConnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.DisconnectPacket;
import software.amazon.awssdk.crt.mqtt5.packets.PublishPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubAckPacket;
import software.amazon.awssdk.crt.mqtt5.packets.SubscribePacket;
import software.amazon.awssdk.iot.AwsIotMqtt5ClientBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@Log4j2
@SuppressWarnings("MissingJavadocMethod")
public class IotCoreClient {
    private final Set<Subscription> subscriptions = Collections.synchronizedSet(new HashSet<>());
    private final PublishHandler publishHandler = new PublishHandler();
    private final LifecycleHandler lifecycleHandler = new LifecycleHandler();
    private final CountDownLatch started = new CountDownLatch(1);
    private Mqtt5Client client;
    private Future<?> resubscribeTask;

    private final CoreDevice coreDevice;
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Inject
    public IotCoreClient(CoreDevice coreDevice) {
        this.coreDevice = coreDevice;
    }

    public void publish(PublishRequest request)
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        start();
        PublishResult result = client.publish(asPublishPacket(request)).get();
        log.trace("publish result: topic={}; reason={}; reasonCode={}; message={};",
                request.getTopic(),
                result.getResultPubAck().getReasonString(),
                result.getResultPubAck().getReasonCode(),
                request.getMessage().getMessage());
    }

    public void resubscribeAsync() {
        if (resubscribeTask != null) {
            resubscribeTask.cancel(true);
        }
        resubscribeTask = executorService.submit(() -> {
            Set<SubscribeRequest> subscribeRequests = getSubscriptions().stream()
                    .map(Subscription::getRequest)
                    .collect(Collectors.toSet());
            for (SubscribeRequest request : subscribeRequests) {
                try {
                    subscribe(request);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException | TimeoutException | IOException e) {
                    log.error("unable to resubscribe to topic {}", request.getTopic(), e);
                }
            }
        });
    }

    public void subscribe(SubscribeRequest request)
            throws ExecutionException, InterruptedException, TimeoutException, IOException {
        start();

        SubscribePacket subscribePacket = new SubscribePacket.SubscribePacketBuilder()
                .withSubscription(request.getTopic(), request.getQos())
                .build();
        Subscription subscription = Subscription.builder()
                .topic(request.getTopic())
                .callback(request.getCallback())
                .request(request)
                .build();
        boolean newSubscription = subscriptions.add(subscription);

        try {
            SubAckPacket subAckPacket = client.subscribe(subscribePacket).get();
            log.debug("subscription result: topic={}; reason={}; reasonCode={};",
                    request.getTopic(),
                    subAckPacket.getReasonString(),
                    subAckPacket.getReasonCodes().get(0));
            if (subAckPacket.getReasonCodes().get(0).getValue() > 2) {
                if (newSubscription) {
                    // only remove if not resubscribing
                    subscriptions.remove(subscription);
                }
                request.getErrorCallback().accept(subAckPacket, null);
                throw new RuntimeException("Unable to subscribe reasonCode="
                        + subAckPacket.getReasonCodes().get(0));
            }
        } catch (ExecutionException | InterruptedException e) {
            if (newSubscription) {
                // only remove if not resubscribing
                subscriptions.remove(subscription);
            }
            request.getErrorCallback().accept(null, e);
            throw e;
        }
    }

    private synchronized void start() throws InterruptedException, TimeoutException {
        if (client == null) {
            client = createMqttClient();
            client.start();
            log.info("Waiting for client to connect...");
            if (!started.await(10L, TimeUnit.SECONDS)) {
                throw new TimeoutException("Timed-out waiting for client connect on start");
            }
        }
    }

    public synchronized void close() {
        if (client != null) {
            try {
                client.stop(new DisconnectPacket.DisconnectPacketBuilder()
                        .withReasonCode(DisconnectPacket.DisconnectReasonCode.NORMAL_DISCONNECTION)
                        .build());
            } catch (CrtRuntimeException e) {
                log.error("Unable to stop mqtt client", e);
                client.close();
            }
        }
        executorService.shutdownNow();
    }

    private Set<Subscription> getSubscriptions() {
        Set<Subscription> subscriptions;
        synchronized (this.subscriptions) {
            subscriptions = new HashSet<>(this.subscriptions);
        }
        return subscriptions;
    }

    private static PublishPacket asPublishPacket(PublishRequest request) {
        return new PublishPacket.PublishPacketBuilder()
                .withQOS(request.getQos())
                .withTopic(request.getTopic())
                .withResponseTopic(request.getMessage().getResponseTopic())
                .withPayload(request.getMessage().getPayload())
                .withPayloadFormat(request.getMessage().getPayloadFormat() == null ? null
                        : PublishPacket.PayloadFormatIndicator.getEnumValueFromInteger(
                        request.getMessage().getPayloadFormat().getValue()))
                .withRetain(request.getMessage().isRetain())
                .withContentType(request.getMessage().getContentType())
                .withCorrelationData(request.getMessage().getCorrelationData())
                .withMessageExpiryIntervalSeconds(request.getMessage().getMessageExpiryIntervalSeconds())
                .withUserProperties(request.getMessage().getUserProperties() == null ? null
                        : request.getMessage().getUserProperties().stream()
                        .map(p -> new software.amazon.awssdk.crt.mqtt5.packets.UserProperty(p.getKey(), p.getValue()))
                        .collect(Collectors.toList()))
                .build();
    }

    private Mqtt5Client createMqttClient() {
        try (AwsIotMqtt5ClientBuilder builder = AwsIotMqtt5ClientBuilder.newDirectMqttBuilderWithMtlsFromMemory(
                coreDevice.getIotCoreDataEndpoint(),
                coreDevice.getSpec().resource().certificate().certificatePem(),
                coreDevice.getSpec().resource().certificate().keyPair().privateKey())) {
            return builder
                    .withCertificateAuthority(coreDevice.getRootCA())
                    .withLifeCycleEvents(lifecycleHandler)
                    .withPublishEvents(publishHandler)
                    .withSessionBehavior(Mqtt5ClientOptions.ClientSessionBehavior.REJOIN_POST_SUCCESS)
                    .withOfflineQueueBehavior(Mqtt5ClientOptions.ClientOfflineQueueBehavior.FAIL_ALL_ON_DISCONNECT)
                    .withConnectProperties(new ConnectPacket.ConnectPacketBuilder()
                            .withRequestProblemInformation(true)
                            .withClientId(getClass().getSimpleName() + UUID.randomUUID())
                            .withSessionExpiryIntervalSeconds(10_080L) // overriding default value to extend session
                            // must be between 30 and 1200 seconds
                            .withKeepAliveIntervalSeconds(30L))
                    .withPingTimeoutMs(Duration.ofSeconds(3).toMillis())
                    .withMinReconnectDelayMs(Duration.ofSeconds(1).toMillis())
                    .withMaxReconnectDelayMs(Duration.ofSeconds(2).toMillis())
                    .build();
        }
    }

    private class LifecycleHandler implements Mqtt5ClientOptions.LifecycleEvents {
        @Override
        public void onAttemptingConnect(Mqtt5Client mqtt5Client,
                                        OnAttemptingConnectReturn onAttemptingConnectReturn) {
            log.debug("Connect in progress");
        }

        @Override
        public void onConnectionSuccess(Mqtt5Client mqtt5Client,
                                        OnConnectionSuccessReturn onConnectionSuccessReturn) {
            boolean sessionPresent = onConnectionSuccessReturn.getConnAckPacket().getSessionPresent();
            log.info("Connected, sessionPresent={}", sessionPresent);
            started.countDown();
            if (!sessionPresent) {
                // don't block crt thread
                resubscribeAsync();
            }
        }

        @Override
        public void onConnectionFailure(Mqtt5Client mqtt5Client,
                                        OnConnectionFailureReturn onConnectionFailureReturn) {
            ConnAckPacket connAck = onConnectionFailureReturn.getConnAckPacket();
            log.warn("Connect failed. errorCode={}; reasonCode={}; reason={};",
                    CRT.awsErrorString(onConnectionFailureReturn.getErrorCode()),
                    connAck == null ? null : connAck.getReasonCode(),
                    connAck == null ? null : connAck.getReasonString());
        }

        @Override
        public void onDisconnection(Mqtt5Client mqtt5Client, OnDisconnectionReturn onDisconnectionReturn) {
            DisconnectPacket discon = onDisconnectionReturn.getDisconnectPacket();
            log.info("Disconnected. errorCode={}; reasonCode={}; reason={};",
                    CRT.awsErrorString(onDisconnectionReturn.getErrorCode()),
                    discon == null ? null : discon.getReasonCode(),
                    discon == null ? null : discon.getReasonString());
        }

        @Override
        public void onStopped(Mqtt5Client mqtt5Client, OnStoppedReturn onStoppedReturn) {
            log.debug("Stopped");
            mqtt5Client.close();
        }
    }

    private class PublishHandler implements Mqtt5ClientOptions.PublishEvents {

        @Override
        public void onMessageReceived(Mqtt5Client mqtt5Client, PublishReturn publishReturn) {
            String topic = publishReturn.getPublishPacket().getTopic();
            log.trace("Message received on topic {} with payload {}",
                    topic,
                    publishReturn.getPublishPacket().getPayload() == null
                            ? null
                            : new String(publishReturn.getPublishPacket().getPayload(), StandardCharsets.UTF_8));
            Message message = asMessage(publishReturn);
            forwardMessageToSubscribers(topic, message);
        }

        private Message asMessage(PublishReturn publishReturn) {
            PublishPacket publishPacket = publishReturn.getPublishPacket();
            return Message.builder()
                    .payload(publishPacket.getPayload())
                    .contentType(publishPacket.getContentType())
                    .correlationData(publishPacket.getCorrelationData())
                    .messageExpiryIntervalSeconds(publishPacket.getMessageExpiryIntervalSeconds())
                    .payloadFormat(publishPacket.getPayloadFormat() == null ? null
                            : PayloadFormatIndicator.fromIndicator(publishPacket.getPayloadFormat().getValue()))
                    .responseTopic(publishPacket.getResponseTopic())
                    .retain(publishPacket.getRetain())
                    .subscriptionIdentifiers(publishPacket.getSubscriptionIdentifiers())
                    .userProperties(publishPacket.getUserProperties() == null ? null
                            : publishPacket.getUserProperties().stream()
                            .map(p -> UserProperty.builder().key(p.key).value(p.value).build())
                            .collect(Collectors.toList()))
                    .build();
        }

        private void forwardMessageToSubscribers(String topic, Message message) {
            getSubscriptions().stream()
                    .filter(s -> Objects.equals(s.getTopic(), topic))
                    .map(Subscription::getCallback)
                    .forEach(c -> c.accept(message));
        }
    }
}
