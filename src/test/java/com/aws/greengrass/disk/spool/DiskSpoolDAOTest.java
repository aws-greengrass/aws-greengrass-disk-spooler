/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.CrashableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({GGExtension.class, MockitoExtension.class})
class DiskSpoolDAOTest {

    private static final CrashableFunction<DiskSpoolDAO, Void, Exception> OPERATION_INSERT_SPOOL_MESSAGE = dao -> {
        dao.insertSpoolMessage(SpoolMessage.builder()
                .id(1L)
                .request(Publish.builder()
                        .topic("spool")
                        .payload("Hello".getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE)
                        .messageExpiryIntervalSeconds(2L)
                        .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                        .contentType("Test")
                        .build())
                .build());
        return null;
    };

    private static final CrashableFunction<DiskSpoolDAO, Void, Exception> OPERATION_GET_ALL_SPOOL_MESSAGE_IDS = dao -> {
        dao.getAllSpoolMessageIds();
        return null;
    };

    private static final CrashableFunction<DiskSpoolDAO, Void, Exception> OPERATION_GET_SPOOL_MESSAGE_BY_ID = dao -> {
        dao.getSpoolMessageById(0L);
        return null;
    };

    private static final CrashableFunction<DiskSpoolDAO, Void, Exception> OPERATION_REMOVE_SPOOL_MESSAGE_BY_ID = dao -> {
        dao.removeSpoolMessageById(0L);
        return null;
    };

    @TempDir
    Path currDir;
    ExecutorService executorService = Executors.newCachedThreadPool();
    DiskSpoolDAOFake dao;

    @BeforeEach
    void setUp() throws SQLException {
        dao = new DiskSpoolDAOFake(currDir.resolve("spooler.db"));
        dao.initialize();
    }

    @AfterEach
    void tearDown() {
        if (dao != null) {
            dao.close();
        }
        executorService.shutdownNow();
    }

    @Test
    void GIVEN_spooler_WHEN_concurrent_get_operations_THEN_success() throws SQLException, InterruptedException {
        SpoolMessage message = SpoolMessage.builder()
                .id(1)
                .request(
                        Publish.builder()
                                .topic("spool")
                                .payload("Hello".getBytes(StandardCharsets.UTF_8))
                                .qos(QOS.AT_LEAST_ONCE)
                                .messageExpiryIntervalSeconds(2L)
                                .payloadFormat(Publish.PayloadFormatIndicator.BYTES)
                                .contentType("Test")
                                .build())
                .build();
        dao.insertSpoolMessage(message);
        AtomicReference<SQLException> ex = new AtomicReference<>();
        for (int i = 0; i < 30; i++) {
            executorService.submit(() -> {
                try {
                    dao.getAllSpoolMessageIds();
                    dao.getSpoolMessageById(1);
                } catch (SQLException e) {
                    ex.set(e);
                    throw new RuntimeException(e);
                }
            });
        }
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(5L, TimeUnit.SECONDS));
        assertNull(ex.get());
    }

    @Test
    void GIVEN_empty_spooler_WHEN_messages_added_and_removed_from_spooler_THEN_success() throws SQLException {
        List<Long> messageIds = LongStream.range(0, 100L).boxed().collect(Collectors.toList());

        // fill db with messages
        for (long id : messageIds) {
            SpoolMessage message = SpoolMessage.builder()
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
                    .build();
            dao.insertSpoolMessage(message);
            // verify message exists
            assertNotNull(dao.getSpoolMessageById(id));
        }

        // verify getting all ids
        int numMessagesChecked = 0;
        Iterator<Long> persistedIds = dao.getAllSpoolMessageIds().iterator();
        for (int i = 0; persistedIds.hasNext(); i++, numMessagesChecked++) {
            assertEquals(messageIds.get(i), persistedIds.next());
        }
        assertEquals(messageIds.size(), numMessagesChecked);

        // remove everything
        for (long id : messageIds) {
            dao.removeSpoolMessageById(id);
            assertNull(dao.getSpoolMessageById(id));
        }
    }

    @ParameterizedTest
    @MethodSource("allSpoolerOperations")
    void GIVEN_spooler_WHEN_corruption_detected_during_operation_THEN_spooler_recovers(CrashableFunction<DiskSpoolDAO, Void, SQLException> operation) throws SQLException {
        SQLException corruptionException = new SQLException("DB is corrupt", "some state", 11);
        dao.getConnection().addExceptionOnUpdate(corruptionException);
        assertThrows(SQLException.class, () -> operation.apply(dao));
        operation.apply(dao);
    }

    @ParameterizedTest
    @MethodSource("allSpoolerOperations")
    void GIVEN_spooler_WHEN_error_during_operation_THEN_exception_thrown(CrashableFunction<DiskSpoolDAO, Void, SQLException> operation, ExtensionContext context) {
        ignoreExceptionOfType(context, SQLTransientException.class);
        SQLException transientException = new SQLTransientException("Some Transient Error");
        dao.getConnection().addExceptionOnUpdate(transientException);
        dao.getConnection().addExceptionOnUpdate(transientException);
        assertThrows(SQLException.class, () -> operation.apply(dao));
    }

    public static Stream<Arguments> allSpoolerOperations() {
        return Stream.of(
                Arguments.of(OPERATION_INSERT_SPOOL_MESSAGE),
                Arguments.of(OPERATION_GET_ALL_SPOOL_MESSAGE_IDS),
                Arguments.of(OPERATION_GET_SPOOL_MESSAGE_BY_ID),
                Arguments.of(OPERATION_REMOVE_SPOOL_MESSAGE_BY_ID)
        );
    }
}
