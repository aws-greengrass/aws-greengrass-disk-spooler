package com.aws.greengrass.disk.spool;

import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.util.NucleusPaths;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Statement;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({GGExtension.class, MockitoExtension.class})
public class DiskSpoolDAOTest {
    @Mock
    private PreparedStatement preparedStatement;
    @Mock
    private Statement statement;
    @Mock
    private Connection connection;
    @TempDir
    Path currDir;
    @Mock
    private NucleusPaths paths;

    @Test
    void GIVEN_request_with_text_WHEN_operation_to_spool_fail_and_DB_corrupt_THEN_should_recover_DB()
            throws SQLException, IOException {
        SQLException sqlException = new SQLException("DB is corrupt", "some state", 11);
        lenient().when(paths.workPath(anyString())).thenReturn(currDir);
        lenient().when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        lenient().when(connection.createStatement()).thenReturn(statement);
        lenient().when(preparedStatement.executeUpdate())
                .thenThrow(sqlException)
                .thenReturn(0);
        lenient().when(statement.executeUpdate(anyString())).thenReturn(0);

        DiskSpoolDAO diskSpoolDAO = spy(new DiskSpoolDAO(paths));
        doReturn(connection).when(diskSpoolDAO).getDbInstance();

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
            throws SQLException, IOException {
        ignoreExceptionOfType(context, SQLTransientException.class);
        SQLException sqlException = new SQLTransientException("Some Transient Error");
        lenient().when(paths.workPath(anyString())).thenReturn(currDir);
        lenient().when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        lenient().when(connection.createStatement()).thenReturn(statement);
        // Fail the first two times to check retry behavior
        lenient().when(preparedStatement.executeUpdate())
                .thenThrow(sqlException)
                .thenThrow(sqlException)
                .thenReturn(0);
        lenient().when(statement.executeUpdate(anyString())).thenReturn(0);

        DiskSpoolDAO diskSpoolDAO = spy(new DiskSpoolDAO(paths));
        doReturn(connection).when(diskSpoolDAO).getDbInstance();

        String message = "Hello";
        Publish request =
                Publish.builder().topic("spool").payload(message.getBytes(StandardCharsets.UTF_8))
                        .qos(QOS.AT_LEAST_ONCE).messageExpiryIntervalSeconds(2L)
                        .payloadFormat(Publish.PayloadFormatIndicator.BYTES).contentType("Test").build();

        SpoolMessage spoolMessage = SpoolMessage.builder().id(1L).request(request).build();
        assertDoesNotThrow(() -> diskSpoolDAO.insertSpoolMessage(spoolMessage));
    }
}
