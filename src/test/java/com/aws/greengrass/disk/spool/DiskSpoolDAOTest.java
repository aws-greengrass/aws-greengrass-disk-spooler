/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientException;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class DiskSpoolDAOTest {
    @TempDir
    Path currDir;
    DiskSpoolDAOFake dao;

    @BeforeEach
    void setUp() throws SQLException {
        dao = spy(new DiskSpoolDAOFake(currDir));
        dao.setUpDatabase();
    }

    @AfterEach
    @SuppressWarnings("PMD.CloseResource")
    void tearDown() throws SQLException {
        Connection conn = dao.getConnection();
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void GIVEN_request_with_text_WHEN_operation_to_spool_fail_and_DB_corrupt_THEN_should_recover_DB() throws SQLException {
        SQLException sqlException = new SQLException("DB is corrupt", "some state", 11);
        dao.getConnection().addExceptionOnUpdate(sqlException);

        Publish request = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE)
                .messageExpiryIntervalSeconds(2L)
                .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                .contentType("Test")
                .build();

        SpoolMessage spoolMessage = SpoolMessage.builder().id(1L).request(request).build();
        assertThrows(SQLException.class, () -> dao.insertSpoolMessage(spoolMessage));
        verify(dao).checkAndHandleCorruption(sqlException);
        dao.insertSpoolMessage(spoolMessage);
    }

    @Test
    void GIVEN_request_with_text_WHEN_operation_to_spool_fail_and_transient_error_THEN_should_retry(ExtensionContext context) throws SQLException {
        ignoreExceptionOfType(context, SQLTransientException.class);
        SQLException sqlException = new SQLTransientException("Some Transient Error");
        dao.getConnection().addExceptionOnUpdate(sqlException);
        dao.getConnection().addExceptionOnUpdate(sqlException);

        Publish request = Publish.builder()
                .topic("spool")
                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                .qos(QOS.AT_LEAST_ONCE)
                .messageExpiryIntervalSeconds(2L)
                .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                .contentType("Test")
                .build();

        SpoolMessage spoolMessage = SpoolMessage.builder().id(1L).request(request).build();
        dao.insertSpoolMessage(spoolMessage);
    }
}
