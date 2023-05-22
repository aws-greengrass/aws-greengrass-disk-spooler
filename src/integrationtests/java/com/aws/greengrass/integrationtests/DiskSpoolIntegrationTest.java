/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.deployment.DeviceConfiguration;
import com.aws.greengrass.disk.spool.DiskSpool;
import com.aws.greengrass.disk.spool.DiskSpoolDAO;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.mqttclient.spool.Spool;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.spool.SpoolerStoreException;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.nio.file.Files.deleteIfExists;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({GGExtension.class, MockitoExtension.class})
// TODO: Enable after Plugin loading changed in Nucleus
@Disabled
public class DiskSpoolIntegrationTest extends BaseITCase {
    @TempDir
    Path rootDir;
    private static final long TEST_TIME_OUT_SEC = 30L;
    private Kernel kernel;
    private DiskSpool diskSpool;
    private DiskSpoolDAO dao;
    private DeviceConfiguration deviceConfiguration;
    private Spool spooler;
    private Path spoolerDatabaseFile;
    private static final String DATABASE_FILE_NAME = "spooler.db";
    private static final String DATABASE_FORMAT = "jdbc:sqlite:%s";


    @BeforeEach
    void beforeEach() throws InterruptedException, IOException {
        startKernelWithConfig();
        spoolerDatabaseFile = kernel.getNucleusPaths()
                .workPath(DiskSpool.PERSISTENCE_SERVICE_NAME)
                .resolve(DATABASE_FILE_NAME);
    }

    @AfterEach
    void afterEach() throws IOException {
        clearDB();
        kernel.shutdown();
    }

    private void clearDB() throws IOException {
        deleteIfExists(spoolerDatabaseFile);
    }

    @Test
    void GIVEN_disk_spool_plugin_WHEN_kernel_starts_THEN_database_file_instantiated() {
        assertTrue(Files.exists(spoolerDatabaseFile));
    }

    @Test
    void GIVEN_disk_spool_plugin_WHEN_kernel_starts_THEN_database_table_created_correctly()
            throws SQLException {
        DriverManager.registerDriver(new org.sqlite.JDBC());
        try(Connection conn = DriverManager.getConnection(String.format(DATABASE_FORMAT, spoolerDatabaseFile))) {
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
    void GIVEN_disk_spool_plugin_WHEN_operation_fails_with_database_corruption_THEN_database_recreated_AND_next_operation_successful()
            throws IOException {

        deviceConfiguration = new DeviceConfiguration(kernel);
        dao = new DiskSpoolDAO(kernel.getNucleusPaths());
        diskSpool = new DiskSpool(kernel.getConfig().getRoot(), dao);

        String message1 = "Message1";
        Publish request1 = createTestPublishRequestWithMessage(message1);
        String message2 = "Message2";
        Publish request2 = createTestPublishRequestWithMessage(message2);
        String message3 = "Message3";
        Publish request3 = createTestPublishRequestWithMessage(message3);

        // Add first message
        SpoolMessage spoolMessage1 = SpoolMessage.builder().id(1L).request(request1).build();
        diskSpool.add(1L, spoolMessage1);
        String readMessage1 = new String (diskSpool.getMessageById(1L).getRequest().getPayload(), StandardCharsets.UTF_8);
        assertEquals(message1, readMessage1);

        // Corrupt Database
        try(RandomAccessFile f = new RandomAccessFile(spoolerDatabaseFile.toFile(), "rw")) {
            f.seek(100);
            f.writeBytes("Garbage");
        }

        // Fail to add second message
        SpoolMessage spoolMessage2 = SpoolMessage.builder().id(2L).request(request2).build();
        assertThrows(IOException.class, () -> diskSpool.add(2L, spoolMessage2));

        // Successfully add third Message
        SpoolMessage spoolMessage3 = SpoolMessage.builder().id(3L).request(request3).build();
        diskSpool.add(3L, spoolMessage3);
        String readMessage3 = new String (diskSpool.getMessageById(3L).getRequest().getPayload(), StandardCharsets.UTF_8);
        assertEquals(message3, readMessage3);
    }

    @Test
    void GIVEN_persistence_spool_plugin_WHEN_nucleus_restarts_THEN_database_persists() throws SpoolerStoreException, InterruptedException {
        deviceConfiguration = new DeviceConfiguration(kernel);
        spooler = new Spool(deviceConfiguration, kernel);

        String message1 = "Message1";
        Publish request1 = createTestPublishRequestWithMessage(message1);
        String message2 = "Message2";
        Publish request2 = createTestPublishRequestWithMessage(message2);
        String message3 = "Message3";
        Publish request3 = createTestPublishRequestWithMessage(message3);

        // Add messages
        spooler.addMessage(request1);
        spooler.addMessage(request2);
        spooler.addMessage(request3);

        kernel.shutdown();
        startKernelWithConfig();
        spooler = new Spool(deviceConfiguration, kernel);

        // Read messages
        String readMessage1 = new String (spooler.getMessageById(0L).getRequest().getPayload(), StandardCharsets.UTF_8);
        String readMessage2 = new String (spooler.getMessageById(1L).getRequest().getPayload(), StandardCharsets.UTF_8);
        String readMessage3 = new String (spooler.getMessageById(2L).getRequest().getPayload(), StandardCharsets.UTF_8);

        assertEquals(message1, readMessage1);
        assertEquals(message2, readMessage2);
        assertEquals(message3, readMessage3);
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

    private Publish createTestPublishRequestWithMessage(String message) {
        return Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE).messageExpiryIntervalSeconds(2L)
                .payloadFormat(Publish.PayloadFormatIndicator.BYTES).contentType("Test").build();
    }
}
