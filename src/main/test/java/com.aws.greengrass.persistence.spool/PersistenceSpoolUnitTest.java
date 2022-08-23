/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.config.Configuration;
import com.aws.greengrass.dependency.Context;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.mqttclient.spool.Spool;
import com.aws.greengrass.mqttclient.spool.SpoolerStorageType;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;

@ExtendWith({GGExtension.class, MockitoExtension.class})
public class PersistenceSpoolUnitTest extends BaseITCase {
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
        config.lookup("spooler", GG_SPOOL_STORAGE_TYPE_KEY).withValue(String.valueOf(SpoolerStorageType.Plugin));
        lenient().when(deviceConfiguration.getSpoolerNamespace()).thenReturn(config.lookupTopics("spooler"));
        spool = spy(new Spool(deviceConfiguration, kernel));
    }

    private void runLast() throws IOException {
        kernel.shutdown();
        config.context.close();
    }

    private void startKernelWithConfig() throws InterruptedException {
        kernel = new Kernel();
        CountDownLatch lambdaManagerRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i",
                getClass().getResource("config.yaml").toString());
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(PersistenceSpool.PERSISTENCE_SERVICE_NAME) && service.getState()
                    .equals(State.RUNNING)) {
                lambdaManagerRunning.countDown();
            }
        });
        kernel.launch();
        assertTrue(lambdaManagerRunning.await(TEST_TIME_OUT_SEC, TimeUnit.SECONDS));
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
        PublishRequest request =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(request).getId();
        String result_string =
                new String (spool.getMessageById(id1).getRequest().getPayload(), StandardCharsets.UTF_8);
        assertEquals(message, result_string);

    }

    @Test
    void GIVEN_id_WHEN_remove_message_by_id_THEN_spooler_message_is_null()
            throws SpoolerStoreException, InterruptedException {
        String message = "Hello";
        PublishRequest request = PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                .qos(QualityOfService.AT_LEAST_ONCE).build();
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

        PublishRequest requestAtLeastOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(requestAtLeastOnce).getId();
        long id2 = spool.addMessage(requestAtLeastOnce).getId();
        PublishRequest requestAtMostOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_MOST_ONCE).build();
        long id3 = spool.addMessage(requestAtMostOnce).getId();
        PublishRequest requestExactlyOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.EXACTLY_ONCE).build();
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
        PublishRequest requestAtLeastOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_LEAST_ONCE).build();
        PublishRequest requestExactlyOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.EXACTLY_ONCE).build();
        PublishRequest requestAtMostOnce =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_MOST_ONCE).build();
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
        PublishRequest request =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_LEAST_ONCE).build();
        long id1 = spool.addMessage(request).getId();
        long id2 = spool.addMessage(request).getId();
        long id3 = spool.addMessage(request).getId();
        long id4 = spool.addMessage(request).getId();
        long id5 = spool.addMessage(request).getId();
        assertThrows(SpoolerStoreException.class, () -> spool.addMessage(request));
    }

    @Test
    void GIVEN_requests_added_to_spooler_WHEN_spooler_restarts_THEN_queue_should_persist()
            throws SpoolerStoreException, InterruptedException, IOException {
        String message = "Hello";
        PublishRequest request =
                PublishRequest.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QualityOfService.AT_LEAST_ONCE).build();
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
}
