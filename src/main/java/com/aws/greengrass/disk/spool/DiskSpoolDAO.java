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
import com.aws.greengrass.util.CrashableFunction;
import com.aws.greengrass.util.LockScope;
import com.aws.greengrass.util.NucleusPaths;
import com.aws.greengrass.util.RetryUtils;
import com.aws.greengrass.util.SerializerFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sqlite.SQLiteErrorCode;

import java.io.IOException;
import java.nio.file.Files;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.inject.Inject;

import static com.aws.greengrass.disk.spool.DiskSpool.PERSISTENCE_SERVICE_NAME;

public class DiskSpoolDAO {

    private static final Logger logger = LogManager.getLogger(DiskSpoolDAO.class);
    private static final ObjectMapper MAPPER = SerializerFactory.getFailSafeJsonObjectMapper();
    private static final RetryUtils.RetryConfig sqlStatementRetryConfig =
            RetryUtils.RetryConfig.builder()
                    .initialRetryInterval(Duration.ofMillis(1L))
                    .maxRetryInterval(Duration.ofMillis(500L))
                    .maxAttempt(3)
                    .retryableExceptions(Collections.singletonList(SQLTransientException.class))
                    .build();
    protected static final String DATABASE_CONNECTION_URL = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";
    private static final Set<Integer> CORRUPTION_ERROR_CODES = new HashSet<>();

    static {
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_CORRUPT.code);
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_NOTADB.code);
    }

    private final Path databasePath;
    private final String url;
    private final ReentrantLock recoverDBLock = new ReentrantLock();
    private final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock();
    private Connection connection;

    /**
     * This method will construct the database path.
     *
     * @param paths The path to the working directory
     * @throws IOException when fails to set up the database
     */
    @Inject
    public DiskSpoolDAO(NucleusPaths paths) throws IOException {
        databasePath = paths.workPath(PERSISTENCE_SERVICE_NAME).resolve(DATABASE_FILE_NAME);
        url = String.format(DATABASE_CONNECTION_URL, databasePath);
    }

    DiskSpoolDAO(Path path) {
        databasePath = path;
        url = String.format(DATABASE_CONNECTION_URL, path);
    }

    /**
     * Initialize the database connection.
     *
     * @throws SQLException if db is unable to be created
     */
    public void initialize() throws SQLException {
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            close();
            logger.atDebug().kv("url", url).log("Creating DB connection");
            connection = getDbInstance();
        }
    }

    /**
     * Close DAO resources.
     */
    public void close() {
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.atWarn().kv("url", url).log("Unable to close pre-existing connection");
                }
            }
        }
    }

    /**
     * This method will query the existing database for the existing queue of MQTT request Ids
     * and return them in order.
     *
     * @return ordered list of the existing ids in the persistent queue
     * @throws SQLException         when fails to get SpoolMessage IDs
     * @throws InterruptedException if interrupted during execution
     */
    public Iterable<Long> getAllSpoolMessageIds() throws SQLException, InterruptedException {
        // TODO don't recreate prepared statements every time
        return performSqlOperation(conn -> {
            try (PreparedStatement stmt = getAllSpoolMessageIdsStatement(conn);
                 ResultSet rs = stmt.executeQuery()) {
                return getIdsFromRs(rs);
            }
        }, "get-all-spool-message-ids");
    }

    private PreparedStatement getAllSpoolMessageIdsStatement(Connection conn) throws SQLException {
        String query = "SELECT message_id FROM spooler;";
        return conn.prepareStatement(query);
    }

    /**
     * This method will query a SpoolMessage and return it given an id.
     *
     * @param messageId the id of the SpoolMessage
     * @return SpoolMessage
     * @throws SQLException         when fails to get a SpoolMessage by id
     * @throws InterruptedException if interrupted during execution
     */
    public SpoolMessage getSpoolMessageById(long messageId) throws SQLException, InterruptedException {
        return performSqlOperation(conn -> {
            try (PreparedStatement pstmt = getSpoolMessageByIdStatement(conn, messageId);
                 ResultSet rs = pstmt.executeQuery()) {
                try {
                    return getSpoolMessageFromRs(messageId, rs);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        }, "get-spool-message-by-id");
    }

    private PreparedStatement getSpoolMessageByIdStatement(Connection conn, long messageId) throws SQLException {
        String query = "SELECT retried, topic, qos, retain, payload, userProperties, messageExpiryIntervalSeconds, "
                + "correlationData, responseTopic, payloadFormat, contentType FROM spooler WHERE message_id = ?;";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setLong(1, messageId);
        return pstmt;
    }

    /**
     * This method will insert a SpoolMessage into the database.
     *
     * @param message instance of SpoolMessage
     * @throws SQLException         when fails to insert SpoolMessage in the database
     * @throws InterruptedException if interrupted during execution
     */
    public void insertSpoolMessage(SpoolMessage message) throws SQLException, InterruptedException {
        performSqlOperation(conn -> {
            try (PreparedStatement pstmt = insertSpoolMessageStatement(conn, message)) {
                return pstmt.executeUpdate();
            }
        }, "insert-spool-message");
    }

    private PreparedStatement insertSpoolMessageStatement(Connection conn, SpoolMessage message) throws SQLException {
        String query =
                "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload, userProperties, "
                        + "messageExpiryIntervalSeconds, correlationData, responseTopic, payloadFormat, contentType) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";
        PreparedStatement pstmt = conn.prepareStatement(query);

        Publish request = message.getRequest();
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
                pstmt.setString(7, MAPPER.writeValueAsString(request.getUserProperties()));
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
        return pstmt;
    }

    /**
     * This method will remove a SpoolMessage from the database given its id.
     *
     * @param messageId the id of the SpoolMessage
     * @throws SQLException         when fails to remove a SpoolMessage by id
     * @throws InterruptedException if interrupted during execution
     */
    public void removeSpoolMessageById(Long messageId) throws SQLException, InterruptedException {
        performSqlOperation(conn -> {
            try (PreparedStatement pstmt = removeSpoolMessageByIdStatement(conn, messageId)) {
                return pstmt.executeUpdate();
            }
        }, "remove-spool-message-by-id");
    }

    private PreparedStatement removeSpoolMessageByIdStatement(Connection conn, long messageId) throws SQLException {
        String query = "DELETE FROM spooler WHERE message_id = ?;";
        PreparedStatement pstmt = conn.prepareStatement(query);
        pstmt.setLong(1, messageId);
        return pstmt;
    }

    /**
     * This method creates a connection instance of the SQLite database.
     *
     * @return Connection for SQLite database instance
     * @throws SQLException When fails to get Database Connection
     */
    public Connection getDbInstance() throws SQLException {
        return DriverManager.getConnection(url);
    }

    protected void setUpDatabase() throws SQLException, InterruptedException {
        String query = "CREATE TABLE IF NOT EXISTS spooler ("
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
        performSqlOperation(conn -> {
            try (Statement st = conn.createStatement()) {
                // create new table if table doesn't exist
                st.executeUpdate(query);
                return null;
            }
        }, "create-spooler-table");
    }

    @SuppressWarnings("PMD.AvoidCatchingGenericException")
    private <T> T performSqlOperation(CrashableFunction<Connection, T, SQLException> operation,
                                      String operationName) throws InterruptedException, SQLException {
        try {
            return RetryUtils.runWithRetry(
                    sqlStatementRetryConfig,
                    () -> {
                        try (LockScope ls = LockScope.lock(connectionLock.readLock())) {
                            return operation.apply(connection);
                        }
                    },
                    operationName,
                    logger
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (SQLException e) {
            checkAndHandleCorruption(e);
            throw e;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    void checkAndHandleCorruption(SQLException e) throws SQLException, InterruptedException {
        if (!CORRUPTION_ERROR_CODES.contains(e.getErrorCode())) {
            return;
        }
        if (!recoverDBLock.tryLock()) {
            // corruption recovery in progress
            return;
        }

        // hold connection lock throughout recovery to prevent incoming operations from executing
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            logger.atWarn().log(String.format("Database %s is corrupted, creating new database", databasePath));
            close();
            try {
                Files.deleteIfExists(databasePath);
            } catch (IOException e2) {
                throw new SQLException(e2);
            }
            initialize();
            setUpDatabase();
        } finally {
            recoverDBLock.unlock();
        }
    }

    private static List<Long> getIdsFromRs(ResultSet rs) throws SQLException {
        List<Long> currentIds = new ArrayList<>();
        while (rs.next()) {
            currentIds.add(rs.getLong("message_id"));
        }
        return currentIds;
    }

    private static SpoolMessage getSpoolMessageFromRs(long messageId, ResultSet rs) throws SQLException, IOException {
        if (!rs.next()) {
            return null;
        }
        Publish request = Publish.builder()
                .qos(QOS.fromInt(rs.getInt("qos")))
                .retain(rs.getBoolean("retain"))
                .topic(rs.getString("topic"))
                .payload(rs.getBytes("payload"))
                .payloadFormat(rs.getObject("payloadFormat") == null
                        ? null : Publish.PayloadFormatIndicator.fromInt(rs.getInt("payloadFormat")))
                .messageExpiryIntervalSeconds(rs.getObject("messageExpiryIntervalSeconds") == null
                        ? null : rs.getLong("messageExpiryIntervalSeconds"))
                .responseTopic(rs.getString("responseTopic"))
                .correlationData(rs.getBytes("correlationData"))
                .contentType(rs.getString("contentType"))
                .userProperties(rs.getString("userProperties") == null
                        ? null : MAPPER.readValue(rs.getString("userProperties"),
                        new TypeReference<List<UserProperty>>(){})).build();

        return SpoolMessage.builder()
                .id(messageId)
                .retried(new AtomicInteger(rs.getInt("retried")))
                .request(request).build();
    }
}
