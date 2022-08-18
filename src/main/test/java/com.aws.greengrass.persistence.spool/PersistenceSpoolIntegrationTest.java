/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.persistence.spool;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.testcommons.testutilities.GGExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static com.aws.greengrass.persistence.spool.SpoolStorageDocumentDAO.DATABASE_FILE_NAME;
import static com.aws.greengrass.persistence.spool.SpoolStorageDocumentDAO.DATABASE_FORMAT;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("PMD.CloseResource")
@ExtendWith({GGExtension.class, MockitoExtension.class})
public class PersistenceSpoolIntegrationTest extends BaseITCase {
    @TempDir
    Path rootDir;
    private static final long TEST_TIME_OUT_SEC = 30L;
    @Mock
    private Kernel kernel;

    private Path spoolerDatabaseFile;

    @BeforeEach
    void beforeEach() throws InterruptedException, IOException {
        startKernelWithConfig();
        spoolerDatabaseFile = kernel.getNucleusPaths()
                .workPath(PersistenceSpool.PERSISTENCE_SERVICE_NAME)
                .resolve(DATABASE_FILE_NAME);
    }

    @AfterEach
    void afterEach() {
        kernel.shutdown();
    }

    @Test
    void GIVEN_persistence_spool_plugin_WHEN_kernel_starts_THEN_database_file_instantiated() {
        assertTrue(Files.exists(spoolerDatabaseFile));
    }

    @Test
    void GIVEN_persistence_spool_plugin_WHEN_kernel_starts_THEN_database_table_created_correctly()
            throws ClassNotFoundException, SQLException, IOException {
        Path spoolerDatabaseFile = kernel.getNucleusPaths()
                .workPath(PersistenceSpool.PERSISTENCE_SERVICE_NAME)
                .resolve(DATABASE_FILE_NAME);
        Class.forName("org.sqlite.JDBC");
        Connection conn = DriverManager.getConnection(String.format(DATABASE_FORMAT, spoolerDatabaseFile));
        DatabaseMetaData databaseMetaData = conn.getMetaData();

        //check if table is created correctly
        ResultSet tables = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"});
        assertTrue(tables.next());
        assertEquals("spooler", tables.getString("TABLE_NAME"));
        tables.close();
        ResultSet columns = databaseMetaData.getColumns(null,null, "spooler", null);

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

        columns.close();
        conn.close();
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
}
