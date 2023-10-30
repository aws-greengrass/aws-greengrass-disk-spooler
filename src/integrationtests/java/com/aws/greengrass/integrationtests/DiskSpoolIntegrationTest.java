/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.disk.spool.DiskSpool;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.spool.Spool;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
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
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.deleteIfExists;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class DiskSpoolIntegrationTest {

    private static final String DATABASE_FILE_NAME = "spooler.db";
    private static final String DATABASE_FORMAT = "jdbc:sqlite:%s";
    private static final long TEST_TIME_OUT_SEC = 30L;

    @TempDir
    Path rootDir;

    Kernel kernel;
    DiskSpool diskSpool;
    Spool spooler;
    Path spoolerDatabaseFile;

    @BeforeEach
    void beforeEach() throws InterruptedException, IOException {
        startNucleus();
    }

    @AfterEach
    void afterEach() throws IOException {
        stopNucleus(true);
    }

    @Test
    void GIVEN_publish_request_WHEN_message_added_and_retrieved_THEN_payload_should_stay_the_same()
            throws InterruptedException, SpoolerStoreException {
        String payload = "Hello";
        SpoolMessage message = spooler.addMessage(publishRequestFromPayload(payload));
        assertPayloadByMessageIdEquals(message, payload);
    }

    @Test
    void GIVEN_spooled_message_WHEN_message_removed_by_id_THEN_message_is_removed()
            throws SpoolerStoreException, InterruptedException {
        String payload = "Hello";

        // spool a message
        long id  = spooler.addMessage(publishRequestFromPayload(payload)).getId();
        assertEquals(1, spooler.getCurrentMessageCount());

        // remove the message
        assertEquals(id, removeMessageByNextId());

        // verify message removed
        assertNull(spooler.getMessageById(id));
    }

    @Test
    void GIVEN_too_many_requests_WHEN_one_qos_zero_request_responsible_for_overflow_THEN_qos_zero_should_be_removed()
            throws SpoolerStoreException, InterruptedException {
        Publish.PublishBuilder builder = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8));

        Publish qos0Request = builder.qos(QOS.AT_MOST_ONCE).build();
        Publish qos1Request = builder.qos(QOS.AT_LEAST_ONCE).build();
        Publish qos2Request = builder.qos(QOS.EXACTLY_ONCE).build();

        long id1 = spooler.addMessage(qos1Request).getId();
        long id2 = spooler.addMessage(qos2Request).getId();
        long id3 = spooler.addMessage(qos2Request).getId();
        long id4 = spooler.addMessage(qos0Request).getId();
        long id5 = spooler.addMessage(qos1Request).getId();
        long id6 = spooler.addMessage(qos1Request).getId();

        // verify non-qos0 messages exist
        assertEquals(id1, removeMessageByNextId());
        assertEquals(id2, removeMessageByNextId());
        assertEquals(id3, removeMessageByNextId());
        assertEquals(id5, removeMessageByNextId());
        assertEquals(id6, removeMessageByNextId());

        assertNull(spooler.getMessageById(id4));

        assertEquals(0, spooler.getCurrentMessageCount());
    }

    @Test
    void GIVEN_too_many_requests_WHEN_all_qos_not_zero_THEN_exception_thrown() throws SpoolerStoreException, InterruptedException {
        Publish qos1Request = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE).build();
        spooler.addMessage(qos1Request).getId();
        spooler.addMessage(qos1Request).getId();
        spooler.addMessage(qos1Request).getId();
        spooler.addMessage(qos1Request).getId();
        spooler.addMessage(qos1Request).getId();
        assertThrows(SpoolerStoreException.class, () -> spooler.addMessage(qos1Request));
    }

    @Test
    void GIVEN_request_with_MQTT5_fields_WHEN_add_to_spool_and_extract_THEN_fields_should_stay_the_same() throws InterruptedException, SpoolerStoreException {
        Publish request = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE)
                .retain(false)
                .messageExpiryIntervalSeconds(1L)
                .payloadFormat(Publish.PayloadFormatIndicator.fromInt(1))
                .responseTopic("spool")
                .contentType("aaa")
                .correlationData("a".getBytes())
                .userProperties(Arrays.asList(
                        new UserProperty("aaa", "bbb"),
                        new UserProperty("ccc", "ddd")))
                .build();
        long id1 = spooler.addMessage(request).getId();
        Publish response = spooler.getMessageById(id1).getRequest();
        assertEquals(request, response);
    }

    @Test
    void GIVEN_request_with_MQTT3_fields_WHEN_add_to_spool_and_extract_THEN_MQTT5_fields_should_be_null() throws InterruptedException, SpoolerStoreException {
        Publish request = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE)
                .build();
        long id1 = spooler.addMessage(request).getId();
        Publish response = spooler.getMessageById(id1).getRequest();
        assertEquals(request, response);
    }

    @Test
    void GIVEN_persistence_spool_plugin_with_messages_WHEN_nucleus_restarts_THEN_messages_are_persisted() throws SpoolerStoreException, InterruptedException, IOException {
        String payload1 = "Message1";
        String payload2 = "Message2";
        String payload3 = "Message3";

        // Add messages
        spooler.addMessage(publishRequestFromPayload(payload1));
        spooler.addMessage(publishRequestFromPayload(payload2));
        spooler.addMessage(publishRequestFromPayload(payload3));

        // restart nucleus
        stopNucleus(false);
        startNucleus();

        // Read messages
        assertPayloadByMessageIdEquals(0L, payload1);
        assertPayloadByMessageIdEquals(1L, payload2);
        assertPayloadByMessageIdEquals(2L, payload3);
    }

    @Test
    void GIVEN_disk_spool_plugin_WHEN_kernel_starts_THEN_database_file_instantiated() {
        assertTrue(Files.exists(spoolerDatabaseFile));
    }

    @Test
    void GIVEN_disk_spool_plugin_WHEN_kernel_starts_THEN_database_table_created_correctly() throws SQLException {
        DriverManager.registerDriver(new org.sqlite.JDBC());
        try (Connection conn = DriverManager.getConnection(String.format(DATABASE_FORMAT, spoolerDatabaseFile))) {
            DatabaseMetaData databaseMetaData = conn.getMetaData();

            //check if table is created correctly
            try (ResultSet tables = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
                assertTrue(tables.next());
                assertEquals("spooler", tables.getString("TABLE_NAME"));
            }
            try (ResultSet columns = databaseMetaData.getColumns(null, null, "spooler", null)) {
                assertTrue(columns.next());
                assertEquals("message_id", columns.getString("COLUMN_NAME"));

                assertTrue(columns.next());
                assertEquals("retried", columns.getString("COLUMN_NAME"));

                assertTrue(columns.next());
                assertEquals("topic", columns.getString("COLUMN_NAME"));

                assertTrue(columns.next());
                assertEquals("qos", columns.getString("COLUMN_NAME"));

                assertTrue(columns.next());
                assertEquals("retain", columns.getString("COLUMN_NAME"));

                assertTrue(columns.next());
                assertEquals("payload", columns.getString("COLUMN_NAME"));
            }
        }
    }

    @Test
    void GIVEN_disk_spool_plugin_WHEN_operation_fails_with_database_corruption_THEN_database_recreated_AND_next_operation_successful() throws IOException {
        String payload1 = "Message1";
        String payload2 = "Message2";
        String payload3 = "Message3";

        // Add first message
        SpoolMessage spoolMessage1 = SpoolMessage.builder().id(1L).request(publishRequestFromPayload(payload1)).build();
        diskSpool.add(1L, spoolMessage1);
        assertPayloadByMessageIdEquals(1L, payload1);

        // Corrupt Database
        try (RandomAccessFile f = new RandomAccessFile(spoolerDatabaseFile.toFile(), "rw")) {
            f.seek(100);
            f.writeBytes("Garbage");
        }

        // Fail to add second message
        SpoolMessage spoolMessage2 = SpoolMessage.builder().id(2L).request(publishRequestFromPayload(payload2)).build();
        assertThrows(IOException.class, () -> diskSpool.add(2L, spoolMessage2));

        // Successfully add third Message
        SpoolMessage spoolMessage3 = SpoolMessage.builder().id(3L).request(publishRequestFromPayload(payload3)).build();
        diskSpool.add(3L, spoolMessage3);
        assertPayloadByMessageIdEquals(3L, payload3);
    }

    @SuppressWarnings("PMD.DetachedTestCase")
    void startNucleus() throws InterruptedException, IOException {
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");
        startKernelWithConfig();
        spoolerDatabaseFile = kernel.getNucleusPaths()
                .workPath(DiskSpool.PERSISTENCE_SERVICE_NAME).resolve(DATABASE_FILE_NAME);
        diskSpool = kernel.getContext().get(DiskSpool.class);
        spooler = new Spool(kernel.getContext().get(DeviceConfiguration.class), kernel);
    }

    void stopNucleus(boolean clearSpoolDb) throws IOException {
        try {
            if (clearSpoolDb) {
                deleteIfExists(spoolerDatabaseFile);
            }
        } finally {
            kernel.shutdown();
        }
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

    private void assertPayloadByMessageIdEquals(SpoolMessage message, String payload) {
        assertPayloadByMessageIdEquals(message.getId(), payload);
    }

    private void assertPayloadByMessageIdEquals(long id, String payload) {
        SpoolMessage persistedMessage = spooler.getMessageById(id);
        assertNotNull(persistedMessage);
        assertEquals(payload, payload(persistedMessage));
    }

    private long removeMessageByNextId() throws InterruptedException {
        long id = spooler.popId();
        spooler.removeMessageById(id);
        return id;
    }

    private static String payload(SpoolMessage message) {
        return new String(message.getRequest().getPayload(), StandardCharsets.UTF_8);
    }

    private static Publish publishRequestFromPayload(String payload) {
        return Publish.builder()
                .topic("spool")
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE)
                .messageExpiryIntervalSeconds(2L)
                .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                .contentType("Test")
                .build();
    }
}
