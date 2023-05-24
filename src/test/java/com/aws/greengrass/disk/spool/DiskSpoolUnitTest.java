/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.spool.Spool;
import com.aws.greengrass.mqttclient.spool.SpoolerStorageType;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;

@ExtendWith({GGExtension.class, MockitoExtension.class})
public class DiskSpoolUnitTest extends BaseITCase {
    @TempDir
    Path rootDir;
    @Mock
    DeviceConfiguration deviceConfiguration;
    private static final long TEST_TIME_OUT_SEC = 30L;
    private Spool spool;
    private Kernel kernel;

    Configuration config = new Configuration(new Context());
    private static final String GG_SPOOL_MAX_SIZE_IN_BYTES_KEY = "maxSizeInBytes";
    private static final String GG_SPOOL_STORAGE_TYPE_KEY = "storageType";

    @BeforeEach
    void before() throws InterruptedException {
        runFirst();
    }

    @AfterEach
    void after() throws IOException, InterruptedException {
        //clear DB
        clearDB();
        runLast();
    }

    private void runFirst() throws InterruptedException {
        startKernelWithConfig();
        config.lookup("spooler", GG_SPOOL_MAX_SIZE_IN_BYTES_KEY).withValue(25L);
        config.lookup("spooler", GG_SPOOL_STORAGE_TYPE_KEY).withValue(String.valueOf(SpoolerStorageType.Disk));
        lenient().when(deviceConfiguration.getSpoolerNamespace()).thenReturn(config.lookupTopics("spooler"));
        spool = spy(new Spool(deviceConfiguration, kernel));
    }

    private void runLast() throws IOException {
        kernel.shutdown();
        config.context.close();
    }

    private void startKernelWithConfig() throws InterruptedException {
        kernel = new Kernel();
        CountDownLatch diskSpoolerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource("config.yaml").toString());
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(DiskSpool.PERSISTENCE_SERVICE_NAME) && service.getState()
                    .equals(State.RUNNING)) {
                diskSpoolerRunning.countDown();
            }
        });
        kernel.launch();
        assertTrue(diskSpoolerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
    }

    private void clearDB() throws InterruptedException {
        while (spool.getCurrentMessageCount() > 0) {
            popAndRemoveNextId();
        }
        assertEquals(0, spool.getCurrentMessageCount());
    }

    private long popAndRemoveNextId() throws InterruptedException {
        long id = spool.popId();
        spool.removeMessageById(id);
        return id;
    }

    @Test
    void GIVEN_request_with_text_WHEN_add_to_spool_and_extract_THEN_text_should_stay_the_same()
            throws InterruptedException, SpoolerStoreException {
        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(request).getId();
        String result_string =
                new String (spool.getMessageById(id1).getRequest().getPayload(), StandardCharsets.UTF_8);
        assertEquals(message, result_string);

    }

    @Test
    void GIVEN_id_WHEN_remove_message_by_id_THEN_spooler_message_is_null()
            throws SpoolerStoreException, InterruptedException {
        String message = "Hello";
        Publish request = Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE).build();
        long id  = spool.addMessage(request).getId();
        assertEquals(1, spool.getCurrentMessageCount());
        spool.popId();
        spool.removeMessageById(id);
        assertNull(spool.getMessageById(id));
    }

    @Test
    void GIVEN_multiple_requests_different_qos_WHEN_pop_all_qos_zero_THEN_continue_if_qos_removed()
            throws SpoolerStoreException, InterruptedException {
        String message = "Hello";

        Publish requestAtLeastOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(requestAtLeastOnce).getId();
        long id2 = spool.addMessage(requestAtLeastOnce).getId();
        Publish requestAtMostOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_MOST_ONCE).build();
        long id3 = spool.addMessage(requestAtMostOnce).getId();
        Publish requestExactlyOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.EXACTLY_ONCE).build();
        long id4 = spool.addMessage(requestExactlyOnce).getId();
        long id5 = spool.addMessage(requestExactlyOnce).getId();
        spool.popOutMessagesWithQosZero();
        assertEquals(id1, popAndRemoveNextId());
        assertEquals(id2, popAndRemoveNextId());
        assertEquals(id4, popAndRemoveNextId());
        assertEquals(id5, popAndRemoveNextId());
        assertNull(spool.getMessageById(id3));
        assertEquals(0, spool.getCurrentMessageCount());
    }

    @Test
    void GIVEN_too_many_requests_WHEN_one_qos_zero_request_responsible_for_overflow_THEN_qos_zero_should_be_removed()
            throws SpoolerStoreException, InterruptedException {
        String message = "Hello";
        Publish requestAtLeastOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).build();
        Publish requestExactlyOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.EXACTLY_ONCE).build();
        Publish requestAtMostOnce =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_MOST_ONCE).build();
        long id1 = spool.addMessage(requestAtLeastOnce).getId();
        long id2 = spool.addMessage(requestExactlyOnce).getId();
        long id3 = spool.addMessage(requestExactlyOnce).getId();
        long id4 = spool.addMessage(requestAtMostOnce).getId();
        long id5 = spool.addMessage(requestAtLeastOnce).getId();
        long id6 = spool.addMessage(requestAtLeastOnce).getId();
        assertEquals(id1, popAndRemoveNextId());
        assertEquals(id2, popAndRemoveNextId());
        assertEquals(id3, popAndRemoveNextId());
        assertEquals(id5, popAndRemoveNextId());
        assertEquals(id6, popAndRemoveNextId());
        assertNull(spool.getMessageById(id4));
        assertEquals(0, spool.getCurrentMessageCount());
    }

    @Test
    void GIVEN_too_many_requests_WHEN_all_qos_not_zero_THEN_exception_thrown()
            throws SpoolerStoreException, InterruptedException {
        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).build();
        spool.addMessage(request).getId();
        spool.addMessage(request).getId();
        spool.addMessage(request).getId();
        spool.addMessage(request).getId();
        spool.addMessage(request).getId();
        assertThrows(SpoolerStoreException.class, () -> spool.addMessage(request));
    }

    @Test
    void GIVEN_requests_added_to_spooler_WHEN_spooler_restarts_THEN_queue_should_persist()
            throws SpoolerStoreException, InterruptedException, IOException {
        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(request).getId();
        long id2 = spool.addMessage(request).getId();
        long id3 = spool.addMessage(request).getId();
        assertEquals(spool.getCurrentMessageCount(), 3);

        runLast();
        runFirst();

        //check length of RAM queue (we expect this to be synced with the db)
        assertEquals(3, spool.getCurrentMessageCount());
        //check the specific ids stored in spooler
        assertEquals(id1, popAndRemoveNextId());
        assertEquals(id2, popAndRemoveNextId());
        assertEquals(id3, popAndRemoveNextId());
    }
    @Test
    void GIVEN_request_with_MQTT5_fields_WHEN_add_to_spool_and_extract_THEN_fields_should_stay_the_same()
            throws InterruptedException, SpoolerStoreException {
        String message = "Hello";
        UserProperty prop1 = new UserProperty("aaa", "bbb");
        UserProperty prop2 = new UserProperty("ccc", "ddd");
        List<UserProperty> props = Arrays.asList(prop1, prop2);
        Publish request =
                Publish.builder()
                        .topic("spool")
                        .payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE)
                        .retain(false)
                        .messageExpiryIntervalSeconds(1L)
                        .payloadFormat(Publish.PayloadFormatIndicator.fromInt(1))
                        .responseTopic("spool")
                        .contentType("aaa")
                        .correlationData("a".getBytes())
                        .userProperties(props)
                        .build();
        long id1 = spool.addMessage(request).getId();
        Publish response = spool.getMessageById(id1).getRequest();
        String result_string = new String(response.getPayload(), StandardCharsets.UTF_8);
        List<UserProperty> result_list = response.getUserProperties();
        assertEquals(message, result_string);
        assertEquals(props, result_list);
        assertEquals(request.getTopic(), response.getTopic());
        assertEquals(request.getQos(), response.getQos());
        assertEquals(request.getMessageExpiryIntervalSeconds(), response.getMessageExpiryIntervalSeconds());
        assertEquals(request.getResponseTopic(), response.getResponseTopic());
        assertEquals(request.getContentType(), response.getContentType());
        assertArrayEquals(request.getCorrelationData(), response.getCorrelationData());
        assertEquals(request.getPayloadFormat(), response.getPayloadFormat());
        assertEquals(request.getUserProperties(), response.getUserProperties());
        assertEquals(request.isRetain(), response.isRetain());
    }
    @Test
    void GIVEN_request_with_MQTT3_fields_WHEN_add_to_spool_and_extract_THEN_MQTT5_fields_should_be_null()
            throws InterruptedException, SpoolerStoreException {
        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8)).qos(QOS.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(request).getId();
        Publish response = spool.getMessageById(id1).getRequest();
        assertNull(response.getMessageExpiryIntervalSeconds());
        assertNull(response.getResponseTopic());
        assertNull(response.getContentType());
        assertNull(response.getCorrelationData());
        assertNull(response.getPayloadFormat());
        assertNull(response.getUserProperties());
        String result_string = new String(response.getPayload(), StandardCharsets.UTF_8);
        assertEquals("Hello", result_string);
        assertEquals(request.getTopic(), response.getTopic());
        assertEquals(request.getQos(), response.getQos());
    }
}
