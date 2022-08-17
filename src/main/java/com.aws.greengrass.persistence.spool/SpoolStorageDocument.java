/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import lombok.AccessLevel;
import lombok.Getter;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.util.concurrent.atomic.AtomicInteger;

@Getter(AccessLevel.PACKAGE)
public class SpoolStorageDocument {

    private final long messageId;
    private final int retried;
    private final String messageTopic;
    private final int messageQOS;
    private final boolean retain;
    private final byte[] messagePayload;

    protected SpoolStorageDocument(SpoolMessage message) {
        messageId = message.getId();
        retried = message.getRetried().get();
        messageTopic = message.getRequest().getTopic();
        messageQOS = message.getRequest().getQos().getValue();
        retain = message.getRequest().isRetain();
        messagePayload = message.getRequest().getPayload();
    }

    protected SpoolStorageDocument(long messageId, int retried, String messageTopic,
                                int messageQOS, boolean retain, byte[] messagePayload) {
        this.messageId = messageId;
        this.retried = retried;
        this.messageTopic = messageTopic;
        this.messageQOS = messageQOS;
        this.retain = retain;
        this.messagePayload = messagePayload;

    }

    /**
     * This function converts the current SpoolStorageDocument object into a SpoolMessage
     * @return SpoolMessage instance
     */
    protected SpoolMessage getSpoolMessage() {
        PublishRequest request = PublishRequest.builder()
                .qos(QualityOfService.getEnumValueFromInteger(messageQOS))
                .retain(retain)
                .topic(messageTopic)
                .payload(messagePayload).build();
        return SpoolMessage.builder()
                .id(messageId)
                .retried(new AtomicInteger(retried))
                .request(request).build();
    }
}
