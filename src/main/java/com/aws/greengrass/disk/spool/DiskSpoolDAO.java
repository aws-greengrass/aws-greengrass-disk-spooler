/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.disk.spool;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.spool.SpoolMessage;
import com.aws.greengrass.mqttclient.v5.Publish;
import com.aws.greengrass.mqttclient.v5.QOS;
import com.aws.greengrass.mqttclient.v5.UserProperty;
import com.aws.greengrass.util.LockScope;
import com.aws.greengrass.util.NucleusPaths;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sqlite.SQLiteErrorCode;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.inject.Inject;

import static com.aws.greengrass.disk.spool.DiskSpool.PERSISTENCE_SERVICE_NAME;
import static java.nio.file.Files.deleteIfExists;

public class DiskSpoolDAO {
    private final String url;
    protected static final String DATABASE_CONNECTION_URL = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";
    private final Path databasePath;
    private static final Logger logger = LogManager.getLogger(DiskSpoolDAO.class);
    private static final ObjectMapper mapper = SerializerFactory.getFailSafeJsonObjectMapper();
    private final ReentrantLock recoverDBLock = new ReentrantLock();
    private final RetryUtils.RetryConfig sqlStatementRetryConfig =
            RetryUtils.RetryConfig.builder().initialRetryInterval(Duration.ofSeconds(1L))
                    .maxRetryInterval(Duration.ofSeconds(3L)).maxAttempt(3)
                    .retryableExceptions(Collections.singletonList(SQLTransientException.class)).build();
    private volatile Connection dbConnection;

    private final ReentrantReadWriteLock dbConnectionLock = new ReentrantReadWriteLock();
    /**
     * This method will construct the database path.
     * @param paths The path to the working directory
     * @throws IOException when fails to set up the database
     * @throws SQLException when fails to make database connection
     */

    @Inject
    public DiskSpoolDAO(NucleusPaths paths) throws IOException, SQLException {
        databasePath = paths.workPath(PERSISTENCE_SERVICE_NAME).resolve(DATABASE_FILE_NAME);
        url = String.format(DATABASE_CONNECTION_URL, databasePath);
        init();
    }

    /** Initialize the singleton database connection.
     *
     * @throws SQLException when fails to make database connection
     */
    synchronized void init() throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.writeLock())) {
            if (dbConnection == null || dbConnection.isClosed() || !isConnectionHealthy(dbConnection)) {
                if (dbConnection != null && !dbConnection.isClosed()) {
                    dbConnection.close(); // close the existing connection if it's unhealthy
                }
                dbConnection = getDbInstance(); // get a new connection instance
            }
        }
    }

    /**
     * This method will query the existing database for the existing queue of MQTT request Ids
     * and return them in order.
     * @return ordered list of the existing ids in the persistent queue
     * @throws SQLException when fails to get SpoolMessage IDs
     */
    @SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.AvoidCatchingGenericException"})
    public Iterable<Long> getAllSpoolMessageIds() throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.readLock())) {
            ensureConnection();
            List<Long> currentIds;
            String query = "SELECT message_id FROM spooler;";
            try (PreparedStatement pstmt = dbConnection.prepareStatement(query);
                 ResultSet rs = RetryUtils.runWithRetry(sqlStatementRetryConfig, pstmt::executeQuery,
                         "get-all-spool-message-ids", logger)) {
                currentIds = getIdsFromRs(rs);
            } catch (SQLException e) {
                checkAndHandleCorruption(e);
                throw e;
            } catch (Exception e) {
                throw new SQLException("Failed to get Spool Message IDs", e);
            }
            return currentIds;
        }
    }

    private List<Long> getIdsFromRs(ResultSet rs) throws SQLException {
        List<Long> currentIds = new ArrayList<>();
        while (rs.next()) {
            currentIds.add(rs.getLong("message_id"));
        }
        return currentIds;
    }

    /**
     * This method will query a SpoolMessage and return it given an id.
     * @param messageId the id of the SpoolMessage
     * @return SpoolMessage
     * @throws SQLException when fails to get a SpoolMessage by id
     */
    @SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.AvoidCatchingGenericException"})
    public synchronized SpoolMessage getSpoolMessageById(long messageId) throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.readLock())) {
            ensureConnection();
            String query = "SELECT retried, topic, qos, retain, payload, userProperties, messageExpiryIntervalSeconds, "
                    + "correlationData, responseTopic, payloadFormat, contentType FROM spooler WHERE message_id = ?;";
            try (PreparedStatement pstmt = dbConnection.prepareStatement(query)) {
                pstmt.setLong(1, messageId);
                try (ResultSet rs = RetryUtils.runWithRetry(sqlStatementRetryConfig, pstmt::executeQuery,
                        "get-spool-message-by-id", logger)) {
                    return getSpoolMessageFromRs(messageId, rs);
                }
            } catch (SQLException e) {
                checkAndHandleCorruption(e);
                throw e;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    /**
     * This method will insert a SpoolMessage into the database.
     * @param message instance of SpoolMessage
     * @throws SQLException when fails to insert SpoolMessage in the database
     */
    @SuppressWarnings({"PMD.ExceptionAsFlowControl", "PMD.AvoidCatchingGenericException"})
    public synchronized void insertSpoolMessage(SpoolMessage message) throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.writeLock())) {
            ensureConnection();
            String sqlString =
                    "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload, userProperties, "
                            + "messageExpiryIntervalSeconds, correlationData,"
                            + " responseTopic, payloadFormat, contentType) "
                            + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
            Publish request = message.getRequest();
            try (PreparedStatement pstmt = dbConnection.prepareStatement(sqlString)) {
                pstmt.setLong(1, message.getId());
                pstmt.setInt(2, message.getRetried().get());

                // MQTT 3 & 5 fields
                pstmt.setString(3, request.getTopic());
                pstmt.setInt(4, request.getQos().getValue());
                pstmt.setBoolean(5, request.isRetain());
                pstmt.setBytes(6, request.getPayload());

                if (request.getUserProperties() == null) {
                    pstmt.setNull(7, Types.NULL);
                } else {
                    try {
                        pstmt.setString(7, mapper.writeValueAsString(request.getUserProperties()));
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }
                }
                if (request.getMessageExpiryIntervalSeconds() == null) {
                    pstmt.setNull(8, Types.NULL);
                } else {
                    pstmt.setLong(8, request.getMessageExpiryIntervalSeconds());
                }
                if (request.getCorrelationData() == null) {
                    pstmt.setNull(9, Types.NULL);
                } else {
                    pstmt.setBytes(9, request.getCorrelationData());
                }
                if (request.getResponseTopic() == null) {
                    pstmt.setNull(10, Types.NULL);
                } else {
                    pstmt.setString(10, request.getResponseTopic());
                }
                if (request.getPayloadFormat() == null) {
                    pstmt.setNull(11, Types.NULL);
                } else {
                    pstmt.setInt(11, request.getPayloadFormat().getValue());
                }
                if (request.getContentType() == null) {
                    pstmt.setNull(12, Types.NULL);
                } else {
                    pstmt.setString(12, request.getContentType());
                }
                RetryUtils.runWithRetry(sqlStatementRetryConfig, pstmt::executeUpdate,
                        "insert-spool-message", logger);
            } catch (SQLException e) {
                checkAndHandleCorruption(e);
                throw e;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    /**
     * This method will remove a SpoolMessage from the database given its id.
     * @param messageId the id of the SpoolMessage
     * @throws SQLException when fails to remove a SpoolMessage by id
     */
    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    public synchronized void removeSpoolMessageById(Long messageId) throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.readLock())) {
            ensureConnection();
            String deleteSQL = "DELETE FROM spooler WHERE message_id = ?;";
            try (PreparedStatement pstmt = dbConnection.prepareStatement(deleteSQL)) {
                pstmt.setLong(1, messageId);
                RetryUtils.runWithRetry(sqlStatementRetryConfig, pstmt::executeUpdate,
                        "remove-spool-message-by-id", logger);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    /**
     * This method creates a connection instance of the SQLite database.
     * @return Connection for SQLite database instance
     * @throws SQLException When fails to get Database Connection
     */
    public Connection getDbInstance() throws SQLException {
        return DriverManager.getConnection(url);
    }

    /**
     * This method closes database connection.
     * @throws SQLException when fails to close database connection.
     */
    public void close() throws SQLException {
        try (LockScope ignored = LockScope.lock(dbConnectionLock.writeLock())) {
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
            }
        }
    }

    protected void setUpDatabase() throws SQLException {
        String tableCreationString = "CREATE TABLE IF NOT EXISTS spooler ("
                + "message_id INTEGER PRIMARY KEY, "
                + "retried INTEGER NOT NULL, "
                + "topic STRING NOT NULL,"
                + "qos INTEGER NOT NULL,"
                + "retain BOOLEAN,"
                + "payload BLOB,"
                + "userProperties TEXT,"
                + "messageExpiryIntervalSeconds INTEGER,"
                + "correlationData BLOB,"
                + "responseTopic STRING,"
                + "payloadFormat INTEGER,"
                + "contentType STRING"
                + ");";
        DriverManager.registerDriver(new org.sqlite.JDBC());
        try (LockScope ignored = LockScope.lock(dbConnectionLock.writeLock())) {
            try (Statement st = dbConnection.createStatement()) {
                // Create new table if it doesn't exist
                st.executeUpdate(tableCreationString);
            }
        }
    }

    private SpoolMessage getSpoolMessageFromRs(long messageId, ResultSet rs) throws SQLException, IOException {
        if (rs.next()) {
            Publish request = Publish.builder()
                    .qos(QOS.fromInt(rs.getInt("qos")))
                    .retain(rs.getBoolean("retain"))
                    .topic(rs.getString("topic"))
                    .payload(rs.getBytes("payload"))
                    .payloadFormat(rs.getObject("messageExpiryIntervalSeconds") == null
                            ? null : Publish.PayloadFormatIndicator.fromInt(rs.getInt("payloadFormat")))
                    .messageExpiryIntervalSeconds(rs.getObject("messageExpiryIntervalSeconds") == null
                            ? null : rs.getLong("messageExpiryIntervalSeconds"))
                    .responseTopic(rs.getString("responseTopic"))
                    .correlationData(rs.getBytes("correlationData"))
                    .contentType(rs.getString("contentType"))
                    .userProperties(rs.getString("userProperties") == null
                            ? null : mapper.readValue(rs.getString("userProperties"),
                                    new TypeReference<List<UserProperty>>(){})).build();

            return SpoolMessage.builder()
                    .id(messageId)
                    .retried(new AtomicInteger(rs.getInt("retried")))
                    .request(request).build();
        } else {
            return null;
        }
    }

    /**
     * This method checks for connection exists or not if not creates connection.
     * @throws SQLException When fails to get Database Connection
     */
    synchronized void ensureConnection() throws SQLException {
        if (dbConnection == null || dbConnection.isClosed()) {
            try (LockScope ignored = LockScope.lock(dbConnectionLock.writeLock())) {
                init();
            }
        }
    }

    boolean isConnectionHealthy(Connection dbConnection) {
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.executeQuery("SELECT 1");
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    void checkAndHandleCorruption(SQLException e) throws SQLException {
        if (e.getErrorCode() == SQLiteErrorCode.SQLITE_CORRUPT.code && recoverDBLock.tryLock()) {
            try {
                logger.atWarn().log(String.format("Database %s is corrupted, creating new database", databasePath));
                close();
                deleteIfExists(databasePath);
                init();
                setUpDatabase();
            } catch (IOException e2) {
                throw new SQLException(e2);
            } finally {
                recoverDBLock.unlock();
            }
        }
    }
}
