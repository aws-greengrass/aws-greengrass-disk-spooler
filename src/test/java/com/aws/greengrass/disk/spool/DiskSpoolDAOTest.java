/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.NucleusPaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;

import org.mockito.junit.jupiter.MockitoExtension;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.*;


import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;


@ExtendWith({GGExtension.class, MockitoExtension.class})
public class DiskSpoolDAOTest {
    @Mock
    private PreparedStatement preparedStatement;
    @Mock
    private Statement statement;
    @Mock
    volatile Connection dbConnection;
   @Mock
    private NucleusPaths mockPaths;
    Path tempDir;
    @BeforeEach
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("myTempDir");
    }

    @Test
    void GIVEN_request_with_text_WHEN_operation_to_spool_fail_and_DB_corrupt_THEN_should_recover_DB()
            throws SQLException, IOException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        SQLException sqlException = new SQLException("DB is corrupt", "some state", 11);
        lenient().when(mockPaths.workPath(anyString())).thenReturn(tempDir);
        lenient().when(dbConnection.prepareStatement(anyString())).thenReturn(preparedStatement);
        lenient().when(dbConnection.createStatement()).thenReturn(statement);
        lenient().when(preparedStatement.executeUpdate())
                .thenThrow(sqlException)
                .thenReturn(0);
        lenient().when(statement.executeUpdate(anyString())).thenReturn(0);

        DiskSpoolDAO diskSpoolDAO = spy(new DiskSpoolDAO(mockPaths));
        Field field = DiskSpoolDAO.class.getDeclaredField("dbConnection");
        field.setAccessible(true);
        field.set(diskSpoolDAO, dbConnection);
        doNothing().when(diskSpoolDAO).close();
        doNothing().when(diskSpoolDAO).init();


        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).messageExpiryIntervalSeconds(2L)
                        .payloadFormat(Publish.PayloadFormatIndicator.BYTES).contentType("Test").build();

        SpoolMessage spoolMessage = SpoolMessage.builder().id(1L).request(request).build();
        assertThrows(SQLException.class, () -> diskSpoolDAO.insertSpoolMessage(spoolMessage));
        verify(diskSpoolDAO, times(1)).checkAndHandleCorruption(sqlException);
        assertDoesNotThrow(() ->diskSpoolDAO.insertSpoolMessage(spoolMessage));
    }

    @Test
    void GIVEN_request_with_text_WHEN_operation_to_spool_fail_and_transient_error_THEN_should_retry(ExtensionContext context)
            throws SQLException, IOException, NoSuchFieldException, IllegalAccessException {
        ignoreExceptionOfType(context, SQLTransientException.class);
        SQLException sqlException = new SQLTransientException("Some Transient Error");
        lenient().when(mockPaths.workPath(anyString())).thenReturn(tempDir);
        lenient().when(dbConnection.prepareStatement(anyString())).thenReturn(preparedStatement);
        lenient().when(dbConnection.createStatement()).thenReturn(statement);
        // Fail the first two times to check retry behavior
        lenient().when(preparedStatement.executeUpdate())
                .thenThrow(sqlException)
                .thenThrow(sqlException)
                .thenReturn(0);
        lenient().when(statement.executeUpdate(anyString())).thenReturn(0);

        DiskSpoolDAO diskSpoolDAO = spy(new DiskSpoolDAO(mockPaths));
        Field field = DiskSpoolDAO.class.getDeclaredField("dbConnection");
        field.setAccessible(true);
        field.set(diskSpoolDAO, dbConnection);

        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).messageExpiryIntervalSeconds(2L)
                        .payloadFormat(Publish.PayloadFormatIndicator.BYTES).contentType("Test").build();

        SpoolMessage spoolMessage = SpoolMessage.builder().id(1L).request(request).build();
        assertDoesNotThrow(() -> diskSpoolDAO.insertSpoolMessage(spoolMessage));
    }
}
