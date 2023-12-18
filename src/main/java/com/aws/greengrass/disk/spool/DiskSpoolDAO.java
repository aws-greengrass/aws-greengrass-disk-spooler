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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.inject.Inject;

import static com.aws.greengrass.disk.spool.DiskSpool.PERSISTENCE_SERVICE_NAME;

public class DiskSpoolDAO {

    private static final Logger LOGGER = LogManager.getLogger(DiskSpoolDAO.class);
    private static final ObjectMapper MAPPER = SerializerFactory.getFailSafeJsonObjectMapper();
    private static final String DATABASE_CONNECTION_URL = "jdbc:sqlite:%s";
    private static final String DATABASE_FILE_NAME = "spooler.db";
    private static final String KV_URL = "url";
    private static final Set<Integer> CORRUPTION_ERROR_CODES = new HashSet<>();

    static {
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_CORRUPT.code);
        CORRUPTION_ERROR_CODES.add(SQLiteErrorCode.SQLITE_NOTADB.code);
    }

    private final Path databasePath;
    private final String url;
    private final CreateSpoolerTable createSpoolerTable = new CreateSpoolerTable();
    private final GetAllSpoolMessageIds getAllSpoolMessageIds = new GetAllSpoolMessageIds();
    private final GetSpoolMessageById getSpoolMessageById = new GetSpoolMessageById();
    private final InsertSpoolMessage insertSpoolMessage = new InsertSpoolMessage();
    private final RemoveSpoolMessageById removeSpoolMessageById = new RemoveSpoolMessageById();
    private final List<CachedStatement<?,?>> allStatements = Arrays.asList(
            createSpoolerTable,
            getAllSpoolMessageIds,
            getSpoolMessageById,
            insertSpoolMessage,
            removeSpoolMessageById
    );

    private final ReentrantLock recoverDBLock = new ReentrantLock();
    private final ReentrantReadWriteLock connectionLock = new ReentrantReadWriteLock();
    private Connection connection;

    /**
     * Create a new DiskSpoolDAO.
     *
     * @param paths nucleus paths
     * @throws IOException if unable to resolve database path
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
     * @throws SQLException if database is unable to be created
     */
    public void initialize() throws SQLException {
        registerDriver();
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            close();
            connection = createConnection();

            // recreate the database table first
            createSpoolerTable.replaceStatement(connection);
            createSpoolerTable.execute();

            // eagerly create remaining statements
            for (CachedStatement<?, ?> statement : allStatements) {
                if (Objects.equals(statement, createSpoolerTable)) {
                    continue;
                }
                statement.replaceStatement(connection);
            }
        }
    }

    private void registerDriver() throws SQLException {
        try {
            // driver is registered statically within JDBC,
            // so it will only happen once
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException e) {
            throw new SQLException("unable to load driver", e);
        }
    }

    /**
     * Close any open DAO resources, including database connections.
     */
    public void close() {
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            for (CachedStatement<?, ?> statement : allStatements) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.atWarn()
                            .kv("statement", statement.getClass().getSimpleName())
                            .log("Unable to close statement");
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.atWarn().kv(KV_URL, url).log("Unable to close pre-existing connection");
                }
            }
        }
    }

    /**
     * Get ids of all messages from the database.
     *
     * @return ordered iterable of message ids
     * @throws SQLException if statement failed to execute, or when unable to read results
     */
    public synchronized Iterable<Long> getAllSpoolMessageIds() throws SQLException {
        try (ResultSet rs = getAllSpoolMessageIds.execute()) {
            return getAllSpoolMessageIds.mapResultToIds(rs);
        }
    }

    /**
     * Get a single message by id from the database.
     *
     * @param id message id
     * @return  message
     * @throws SQLException if statement failed to execute, or when unable to read results
     */
    public synchronized SpoolMessage getSpoolMessageById(long id) throws SQLException {
        try (ResultSet rs = getSpoolMessageById.executeWithParameters(id)) {
            return getSpoolMessageById.mapResultToMessage(id, rs);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Insert a message into the database.
     *
     * @param message message
     * @throws SQLException if statement failed to execute
     */
    public void insertSpoolMessage(SpoolMessage message) throws SQLException {
        insertSpoolMessage.executeWithParameters(message);
    }

    /**
     * Remove a message by id from the database.
     *
     * @param id message id
     * @throws SQLException if statement failed to execute
     */
    public void removeSpoolMessageById(Long id) throws SQLException {
        removeSpoolMessageById.executeWithParameters(id);
    }

    /**
     * Create a new database connection.
     *
     * @return connection
     * @throws SQLException if database access error occurs
     */
    protected Connection createConnection() throws SQLException {
        LOGGER.atDebug().kv(KV_URL, url).log("Creating database connection");
        return DriverManager.getConnection(url);
    }

    void recoverFromCorruption() throws SQLException {
        if (!recoverDBLock.tryLock()) {
            // corruption recovery in progress
            return;
        }

        // hold connection lock throughout recovery to prevent incoming operations from executing
        try (LockScope ls = LockScope.lock(connectionLock.writeLock())) {
            LOGGER.atWarn().kv(KV_URL, url).log(DATABASE_FILE_NAME + " is corrupted, creating new database");
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
        protected PreparedStatement createStatement(Connection connection) throws SQLException {
            return connection.prepareStatement(QUERY);
        }

        @Override
        protected ResultSet doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeQuery();
        }

        List<Long> mapResultToIds(ResultSet rs) throws SQLException {
            List<Long> ids = new ArrayList<>();
            while (rs.next()) {
                ids.add(rs.getLong("message_id"));
            }
            return ids;
        }
    }

    class GetSpoolMessageById extends CachedStatement<PreparedStatement, ResultSet> {
        private static final String QUERY =
                "SELECT retried, topic, qos, retain, payload, userProperties, messageExpiryIntervalSeconds, "
                        + "correlationData, responseTopic, payloadFormat, contentType "
                        + "FROM spooler WHERE message_id = ?;";

        @Override
        protected PreparedStatement createStatement(Connection connection) throws SQLException {
            return connection.prepareStatement(QUERY);
        }

        @Override
        protected ResultSet doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeQuery();
        }

        ResultSet executeWithParameters(Long id) throws SQLException {
            return executeWithParameters(s -> {
                s.setLong(1, id);
                return null;
            });
        }

        SpoolMessage mapResultToMessage(long id, ResultSet rs) throws SQLException, IOException {
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
                    .id(id)
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
        protected PreparedStatement createStatement(Connection connection) throws SQLException {
            return connection.prepareStatement(QUERY);
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
        protected PreparedStatement createStatement(Connection connection) throws SQLException {
            return connection.prepareStatement(QUERY);
        }

        @Override
        protected Integer doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeUpdate();
        }

        protected Integer executeWithParameters(Long id) throws SQLException {
            return executeWithParameters(s -> {
                s.setLong(1, id);
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
        protected PreparedStatement createStatement(Connection connection) throws SQLException {
            return connection.prepareStatement(QUERY);
        }

        @Override
        protected Integer doExecute(PreparedStatement statement) throws SQLException {
            return statement.executeUpdate();
        }
    }

    /**
     * A {@link Statement} wrapper that reuses the statement across executions.
     *
     * @param <T> statement type
     * @param <R> execution result type
     */
    abstract class CachedStatement<T extends Statement, R> {
        private T statement;

        /**
         * Create a new statement and replace the existing one, if present.
         *
         * @param connection connection
         * @throws SQLException if unable to create statement
         */
        public void replaceStatement(Connection connection) throws SQLException {
            close(); // clean up old resources
            statement = createStatement(connection);
        }

        /**
         * Create a new statement.
         *
         * @param connection connection
         * @return statement
         * @throws SQLException if unable to create statement
         */
        protected abstract T createStatement(Connection connection) throws SQLException;

        public void close() throws SQLException {
            if (statement != null) {
                statement.close();
            }
        }

        /**
         * Execute the statement.  This could be any type of execution, e.g. executeQuery, executeUpdate.
         *
         * @return execution results
         * @throws SQLException if error occurs during execution
         */
        R execute() throws SQLException {
            return executeInternal();
        }

        /**
         * Set parameters on the statement and execute it.
         *
         * @param decorator function that sets statement parameters
         * @return execution results
         * @throws SQLException if error occurs during execution
         */
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
