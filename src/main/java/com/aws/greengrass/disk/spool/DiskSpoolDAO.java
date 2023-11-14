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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
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
    protected static final String DATABASE_CONNECTION_URL = "jdbc:sqlite:%s";
    protected static final String DATABASE_FILE_NAME = "spooler.db";
    private static final Set<Integer> CORRUPTION_ERROR_CODES = new HashSet<>();

    static {
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_CORRUPT.code);
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_NOTADB.code);
    }

    private final Path databasePath;
    private final String url;
    private final List<CrashableFunction<Connection, Void, SQLException>> onNewConnection = new ArrayList<>();
    @SuppressWarnings("PMD.UnusedPrivateField") // runs whenever a new db connection is made
    private final CreateSpoolerTable createSpoolerTable = new CreateSpoolerTable();
    private final GetAllSpoolMessageIds getAllSpoolMessageIdsStatement = new GetAllSpoolMessageIds();
    private final GetSpoolMessageById getSpoolMessageByIdStatement = new GetSpoolMessageById();
    private final InsertSpoolMessage insertSpoolMessageStatement = new InsertSpoolMessage();
    private final RemoveSpoolMessageById removeSpoolMessageByIdStatement = new RemoveSpoolMessageById();
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
            connection = createConnection();
            for (CrashableFunction<Connection, Void, SQLException> handler : onNewConnection) {
                handler.apply(connection);
            }
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
     * @throws SQLException when fails to get SpoolMessage IDs
     */
    public Iterable<Long> getAllSpoolMessageIds() throws SQLException {
        try (ResultSet rs = getAllSpoolMessageIdsStatement.execute()) {
            return getAllSpoolMessageIdsStatement.mapResultToIds(rs);
        }
    }

    /**
     * This method will query a SpoolMessage and return it given an id.
     *
     * @param messageId the id of the SpoolMessage
     * @return SpoolMessage
     * @throws SQLException when fails to get a SpoolMessage by id
     */
    public SpoolMessage getSpoolMessageById(long messageId) throws SQLException {
        try (ResultSet rs = getSpoolMessageByIdStatement.executeWithParameters(messageId)) {
            try {
                return getSpoolMessageByIdStatement.mapResultToMessage(messageId, rs);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

    /**
     * This method will insert a SpoolMessage into the database.
     *
     * @param message instance of SpoolMessage
     * @throws SQLException when fails to insert SpoolMessage in the database
     */
    public void insertSpoolMessage(SpoolMessage message) throws SQLException {
        insertSpoolMessageStatement.executeWithParameters(message);
    }

    /**
     * This method will remove a SpoolMessage from the database given its id.
     *
     * @param messageId the id of the SpoolMessage
     * @throws SQLException when fails to remove a SpoolMessage by id
     */
    public void removeSpoolMessageById(Long messageId) throws SQLException {
        removeSpoolMessageByIdStatement.executeWithParameters(messageId);
    }

    /**
     * This method creates a connection instance of the SQLite database.
     *
     * @return Connection for SQLite database instance
     * @throws SQLException When fails to get Database Connection
     */
    protected Connection createConnection() throws SQLException {
        return DriverManager.getConnection(url);
    }

    void recoverFromCorruption() throws SQLException {
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
        } finally {
            recoverDBLock.unlock();
        }
    }

    class GetAllSpoolMessageIds extends CachedStatement<PreparedStatement, ResultSet> {
        private static final String QUERY = "SELECT message_id FROM spooler;";

        @Override
        protected PreparedStatement createStatement(Connection conn) throws SQLException {
            return conn.prepareStatement(QUERY);
        }

        @Override
        protected ResultSet doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeQuery();
        }

        List<Long> mapResultToIds(ResultSet rs) throws SQLException {
            List<Long> currentIds = new ArrayList<>();
            while (rs.next()) {
                currentIds.add(rs.getLong("message_id"));
            }
            return currentIds;
        }
    }

    class GetSpoolMessageById extends CachedStatement<PreparedStatement, ResultSet> {
        private static final String QUERY =
                "SELECT retried, topic, qos, retain, payload, userProperties, messageExpiryIntervalSeconds, "
                        + "correlationData, responseTopic, payloadFormat, contentType "
                        + "FROM spooler WHERE message_id = ?;";

        @Override
        protected PreparedStatement createStatement(Connection conn) throws SQLException {
            return conn.prepareStatement(QUERY);
        }

        @Override
        protected ResultSet doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeQuery();
        }

        ResultSet executeWithParameters(Long messageId) throws SQLException {
            return executeWithParameters(s -> {
                s.setLong(1, messageId);
                return null;
            });
        }

        SpoolMessage mapResultToMessage(long messageId, ResultSet rs) throws SQLException, IOException {
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

    class InsertSpoolMessage extends CachedStatement<PreparedStatement, Integer> {
        private static final String QUERY =
                "INSERT INTO spooler (message_id, retried, topic, qos, retain, payload, userProperties, "
                        + "messageExpiryIntervalSeconds, correlationData, responseTopic, payloadFormat, contentType) "
                        + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";

        @Override
        protected PreparedStatement createStatement(Connection conn) throws SQLException {
            return conn.prepareStatement(QUERY);
        }

        @Override
        protected Integer doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeUpdate();
        }

        Integer executeWithParameters(SpoolMessage message) throws SQLException {
            return executeWithParameters(s -> {
                Publish request = message.getRequest();
                s.setLong(1, message.getId());
                s.setInt(2, message.getRetried().get());

                // MQTT 3 & 5 fields
                s.setString(3, request.getTopic());
                s.setInt(4, request.getQos().getValue());
                s.setBoolean(5, request.isRetain());
                s.setBytes(6, request.getPayload());

                if (request.getUserProperties() == null) {
                    s.setNull(7, Types.NULL);
                } else {
                    try {
                        s.setString(7, MAPPER.writeValueAsString(request.getUserProperties()));
                    } catch (IOException e) {
                        throw new SQLException(e);
                    }
                }
                if (request.getMessageExpiryIntervalSeconds() == null) {
                    s.setNull(8, Types.NULL);
                } else {
                    s.setLong(8, request.getMessageExpiryIntervalSeconds());
                }
                if (request.getCorrelationData() == null) {
                    s.setNull(9, Types.NULL);
                } else {
                    s.setBytes(9, request.getCorrelationData());
                }
                if (request.getResponseTopic() == null) {
                    s.setNull(10, Types.NULL);
                } else {
                    s.setString(10, request.getResponseTopic());
                }
                if (request.getPayloadFormat() == null) {
                    s.setNull(11, Types.NULL);
                } else {
                    s.setInt(11, request.getPayloadFormat().getValue());
                }
                if (request.getContentType() == null) {
                    s.setNull(12, Types.NULL);
                } else {
                    s.setString(12, request.getContentType());
                }
                return null;
            });
        }
    }

    class RemoveSpoolMessageById extends CachedStatement<PreparedStatement, Integer> {
        private static final String QUERY = "DELETE FROM spooler WHERE message_id = ?;";

        @Override
        protected PreparedStatement createStatement(Connection conn) throws SQLException {
            return conn.prepareStatement(QUERY);
        }

        @Override
        protected Integer doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeUpdate();
        }

        protected Integer executeWithParameters(Long messageId) throws SQLException {
            return executeWithParameters(s -> {
                s.setLong(1, messageId);
                return null;
            });
        }
    }

    class CreateSpoolerTable extends CachedStatement<PreparedStatement, Integer> {
        private static final String QUERY = "CREATE TABLE IF NOT EXISTS spooler ("
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

        @Override
        protected PreparedStatement createStatement(Connection conn) throws SQLException {
            return conn.prepareStatement(QUERY);
        }

        @Override
        protected Integer doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeUpdate();
        }

        @Override
        public Void onNewConnection(Connection newConnection) throws SQLException {
            super.onNewConnection(newConnection);
            execute(); // recreate spooler table right away
            return null;
        }
    }

    abstract class CachedStatement<T extends Statement, R> {
        private T statement;

        protected CachedStatement() {
            onNewConnection.add(this::onNewConnection);
        }

        public Void onNewConnection(Connection newConnection) throws SQLException {
            try (LockScope ls = LockScope.lock(connectionLock.readLock())) {
                close(); // clean up old resources
                statement = createStatement(newConnection);
            }
            return null;
        }

        protected abstract T createStatement(Connection conn) throws SQLException;

        public void close() throws SQLException {
            if (statement != null) {
                statement.close();
            }
        }

        R execute() throws SQLException {
            return executeInternal();
        }

        R executeWithParameters(CrashableFunction<T, Void, SQLException> decorator) throws SQLException {
            decorator.apply(statement);
            return executeInternal();
        }

        @SuppressWarnings("PMD.AvoidCatchingGenericException")
        private R executeInternal() throws SQLException {
            try {
                return doExecute(statement);
            } catch (SQLException e) {
                if (CORRUPTION_ERROR_CODES.contains(e.getErrorCode())) {
                    recoverFromCorruption();
                }
                throw e;
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }

        protected abstract R doExecute(T statement) throws SQLException;
    }
}
