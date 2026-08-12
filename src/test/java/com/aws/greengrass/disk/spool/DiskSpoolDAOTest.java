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

    @Test
    void GIVEN_empty_spooler_WHEN_get_max_message_id_THEN_returns_negative_one() throws SQLException {
        assertEquals(-1, dao.getMaxMessageId());
    }

    @Test
    void GIVEN_multiple_messages_WHEN_get_max_message_id_THEN_returns_highest() throws SQLException {
        // Insert in non-sequential order
        for (long id : new long[]{5L, 100L, 3L}) {
            dao.insertSpoolMessage(SpoolMessage.builder()
                    .id(id)
                    .request(Publish.builder()
                            .topic("test")
                            .payload("msg".getBytes(StandardCharsets.UTF_8))
                            .qos(QOS.AT_LEAST_ONCE)
                            .build())
                    .build());
        }
        assertEquals(100L, dao.getMaxMessageId());
    }

    @Test
    void GIVEN_messages_WHEN_highest_removed_THEN_max_id_reflects_new_highest() throws SQLException {
        for (long id : new long[]{10L, 20L, 30L}) {
            dao.insertSpoolMessage(SpoolMessage.builder()
                    .id(id)
                    .request(Publish.builder()
                            .topic("test")
                            .payload("msg".getBytes(StandardCharsets.UTF_8))
                            .qos(QOS.AT_LEAST_ONCE)
                            .build())
                    .build());
        }
        assertEquals(30L, dao.getMaxMessageId());

        dao.removeSpoolMessageById(30L);
        assertEquals(20L, dao.getMaxMessageId());
    }

    @Test
    void GIVEN_empty_spooler_WHEN_get_all_ids_with_sizes_THEN_returns_empty_list() throws SQLException {
        List<long[]> result = dao.getAllMessageIdsWithPayloadSize();
        assertTrue(result.isEmpty());
    }

    @Test
    void GIVEN_messages_inserted_in_random_order_WHEN_get_all_ids_with_sizes_THEN_returns_ordered_with_correct_sizes() throws SQLException {
        byte[] payload5 = "hello".getBytes(StandardCharsets.UTF_8);   // 5 bytes
        byte[] payload11 = "hello world".getBytes(StandardCharsets.UTF_8); // 11 bytes
        byte[] payload3 = "abc".getBytes(StandardCharsets.UTF_8);     // 3 bytes

        // Insert in non-sequential order
        dao.insertSpoolMessage(SpoolMessage.builder().id(50L)
                .request(Publish.builder().topic("t").payload(payload11).qos(QOS.AT_LEAST_ONCE).build()).build());
        dao.insertSpoolMessage(SpoolMessage.builder().id(10L)
                .request(Publish.builder().topic("t").payload(payload5).qos(QOS.AT_LEAST_ONCE).build()).build());
        dao.insertSpoolMessage(SpoolMessage.builder().id(30L)
                .request(Publish.builder().topic("t").payload(payload3).qos(QOS.AT_LEAST_ONCE).build()).build());

        List<long[]> result = dao.getAllMessageIdsWithPayloadSize();

        assertEquals(3, result.size());
        // Should be ordered by message_id ASC
        assertEquals(10L, result.get(0)[0]);
        assertEquals(5L, result.get(0)[1]);
        assertEquals(30L, result.get(1)[0]);
        assertEquals(3L, result.get(1)[1]);
        assertEquals(50L, result.get(2)[0]);
        assertEquals(11L, result.get(2)[1]);
    }

    @Test
    void GIVEN_message_with_null_payload_WHEN_get_all_ids_with_sizes_THEN_returns_zero_size() throws SQLException {
        dao.insertSpoolMessage(SpoolMessage.builder().id(1L)
                .request(Publish.builder().topic("t").payload(null).qos(QOS.AT_LEAST_ONCE).build()).build());

        List<long[]> result = dao.getAllMessageIdsWithPayloadSize();

        assertEquals(1, result.size());
        assertEquals(1L, result.get(0)[0]);
        assertEquals(0L, result.get(0)[1]);
    }

    @Test
    void GIVEN_messages_WHEN_one_removed_THEN_ids_with_sizes_reflects_current_state() throws SQLException {
        byte[] payload = "test".getBytes(StandardCharsets.UTF_8); // 4 bytes
        for (long id : new long[]{1L, 2L, 3L}) {
            dao.insertSpoolMessage(SpoolMessage.builder().id(id)
                    .request(Publish.builder().topic("t").payload(payload).qos(QOS.AT_LEAST_ONCE).build()).build());
        }

        dao.removeSpoolMessageById(2L);

        List<long[]> result = dao.getAllMessageIdsWithPayloadSize();
        assertEquals(2, result.size());
        assertEquals(1L, result.get(0)[0]);
        assertEquals(3L, result.get(1)[0]);
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
