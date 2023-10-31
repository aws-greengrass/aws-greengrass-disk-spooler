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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void GIVEN_empty_spooler_WHEN_messages_added_and_removed_from_spooler_THEN_success() throws SQLException {
        List<Long> messageIds = LongStream.range(0, 100L).boxed().collect(Collectors.toList());

        // fill db with messages
        for (long id : messageIds) {
            dao.insertSpoolMessage(
                    SpoolMessage.builder()
                            .id(id)
                            .request(
                                    Publish.builder()
                                            .topic("spool")
                                            .payload("Hello".getBytes(StandardCharsets.UTF_8))
                                            .qos(QOS.AT_LEAST_ONCE)
                                            .messageExpiryIntervalSeconds(2L)
                                            .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                                            .contentType("Test")
                                            .build())
                            .build());
            // verify message exists
            assertNotNull(dao.getSpoolMessageById(id));
        }

        // verify getting all ids
        int numMessagesChecked = 0;
        Iterator<Long> persistedIds = dao.getAllSpoolMessageIds().iterator();
        for (int i = 0; i < messageIds.size(); i++, numMessagesChecked++) {
            assertEquals(messageIds.get(i), persistedIds.next());
        }
        assertEquals(messageIds.size(), numMessagesChecked);

        // remove everything
        for (long id : messageIds) {
            dao.removeSpoolMessageById(id);
            assertNull(dao.getSpoolMessageById(id));
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
